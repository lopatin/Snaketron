//! Cache-aside replay retrieval with fail-closed integrity checks.
//!
//! Valkey is an optimization only. A miss, malformed value, timeout, or cache
//! backend error falls through to the durable `ReplayStore`. Only bytes bound
//! to the caller's durable metadata reference are ever returned.

use anyhow::{Context, Result, bail};
use opentelemetry::metrics::Counter;
use opentelemetry::{KeyValue, global};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

use crate::replay_cache::{ReplayCache, ReplayCacheLookup, ReplayCachePutOutcome};
use crate::replay_store::{
    REPLAY_MANIFEST_OBJECT_FORMAT_VERSION, ReplayChunkManifestV1, ReplayObjectMetadata,
    ReplayRecording, ReplayStore,
};

const MAX_RECONSTRUCTED_REPLAY_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_REPLAY_RANGE_BYTES: u64 = 8 * 1024 * 1024;

const CACHE_ENVELOPE_MAGIC: &[u8; 8] = b"SNKRCHE1";
const CACHE_ENVELOPE_VERSION: u16 = 1;
const REFERENCE_DIGEST_BYTES: usize = 32;
const CACHE_ENVELOPE_HEADER_BYTES: usize =
    CACHE_ENVELOPE_MAGIC.len() + 2 + REFERENCE_DIGEST_BYTES + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayLoadSource {
    Cache,
    ObjectStore,
}

impl ReplayLoadSource {
    fn metric_label(self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::ObjectStore => "object_store",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayLoad {
    pub recording: ReplayRecording,
    pub source: ReplayLoadSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRangeLoad {
    pub bytes: Vec<u8>,
    pub total_bytes: u64,
    /// Half-open byte offsets in the canonical recording JSON.
    pub start: u64,
    pub end: u64,
    pub source: ReplayLoadSource,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayRepositoryMetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_get_failures: u64,
    pub cache_corruptions: u64,
    pub cache_fills: u64,
    pub cache_fill_failures: u64,
    pub cache_fill_oversize_skips: u64,
    pub object_store_hits: u64,
    pub object_store_misses: u64,
    pub object_store_failures: u64,
    pub reference_validation_failures: u64,
}

#[derive(Default)]
struct ReplayRepositoryMetricCounters {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_get_failures: AtomicU64,
    cache_corruptions: AtomicU64,
    cache_fills: AtomicU64,
    cache_fill_failures: AtomicU64,
    cache_fill_oversize_skips: AtomicU64,
    object_store_hits: AtomicU64,
    object_store_misses: AtomicU64,
    object_store_failures: AtomicU64,
    reference_validation_failures: AtomicU64,
}

/// Per-repository counters for deterministic tests and local diagnostics. Each
/// update is also emitted through bounded-cardinality OpenTelemetry counters.
#[derive(Clone, Default)]
pub struct ReplayRepositoryMetrics {
    counters: Arc<ReplayRepositoryMetricCounters>,
}

impl ReplayRepositoryMetrics {
    pub fn snapshot(&self) -> ReplayRepositoryMetricsSnapshot {
        let counters = &self.counters;
        ReplayRepositoryMetricsSnapshot {
            cache_hits: counters.cache_hits.load(Ordering::Relaxed),
            cache_misses: counters.cache_misses.load(Ordering::Relaxed),
            cache_get_failures: counters.cache_get_failures.load(Ordering::Relaxed),
            cache_corruptions: counters.cache_corruptions.load(Ordering::Relaxed),
            cache_fills: counters.cache_fills.load(Ordering::Relaxed),
            cache_fill_failures: counters.cache_fill_failures.load(Ordering::Relaxed),
            cache_fill_oversize_skips: counters.cache_fill_oversize_skips.load(Ordering::Relaxed),
            object_store_hits: counters.object_store_hits.load(Ordering::Relaxed),
            object_store_misses: counters.object_store_misses.load(Ordering::Relaxed),
            object_store_failures: counters.object_store_failures.load(Ordering::Relaxed),
            reference_validation_failures: counters
                .reference_validation_failures
                .load(Ordering::Relaxed),
        }
    }

    fn record_cache_lookup(&self, outcome: CacheLookupMetric) {
        let counter = match outcome {
            CacheLookupMetric::Hit => &self.counters.cache_hits,
            CacheLookupMetric::Miss => &self.counters.cache_misses,
            CacheLookupMetric::BackendError => &self.counters.cache_get_failures,
            CacheLookupMetric::Corrupt | CacheLookupMetric::Oversized => {
                &self.counters.cache_corruptions
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
        replay_otel_metrics()
            .cache_lookups
            .add(1, &[KeyValue::new("replay.cache.outcome", outcome.label())]);
    }

    fn record_cache_fill(&self, outcome: CacheFillMetric) {
        let counter = match outcome {
            CacheFillMetric::Stored => &self.counters.cache_fills,
            CacheFillMetric::BackendError => &self.counters.cache_fill_failures,
            CacheFillMetric::Oversized => &self.counters.cache_fill_oversize_skips,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        replay_otel_metrics()
            .cache_fills
            .add(1, &[KeyValue::new("replay.cache.outcome", outcome.label())]);
    }

    fn record_object_store_lookup(&self, outcome: ObjectStoreMetric) {
        let counter = match outcome {
            ObjectStoreMetric::Hit => &self.counters.object_store_hits,
            ObjectStoreMetric::Miss => &self.counters.object_store_misses,
            ObjectStoreMetric::Error => &self.counters.object_store_failures,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        replay_otel_metrics().object_store_lookups.add(
            1,
            &[KeyValue::new(
                "replay.object_store.outcome",
                outcome.label(),
            )],
        );
    }

    fn record_load(&self, source: ReplayLoadSource) {
        replay_otel_metrics()
            .loads
            .add(1, &[KeyValue::new("replay.source", source.metric_label())]);
    }

    fn record_reference_validation_failure(&self) {
        self.counters
            .reference_validation_failures
            .fetch_add(1, Ordering::Relaxed);
        replay_otel_metrics()
            .reference_validation_failures
            .add(1, &[]);
    }
}

#[derive(Clone, Copy)]
enum CacheLookupMetric {
    Hit,
    Miss,
    BackendError,
    Corrupt,
    Oversized,
}

impl CacheLookupMetric {
    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::BackendError => "backend_error",
            Self::Corrupt => "corrupt",
            Self::Oversized => "oversized",
        }
    }
}

#[derive(Clone, Copy)]
enum CacheFillMetric {
    Stored,
    BackendError,
    Oversized,
}

impl CacheFillMetric {
    fn label(self) -> &'static str {
        match self {
            Self::Stored => "stored",
            Self::BackendError => "backend_error",
            Self::Oversized => "oversized",
        }
    }
}

#[derive(Clone, Copy)]
enum ObjectStoreMetric {
    Hit,
    Miss,
    Error,
}

impl ObjectStoreMetric {
    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }
}

struct ReplayOtelMetrics {
    loads: Counter<u64>,
    cache_lookups: Counter<u64>,
    cache_fills: Counter<u64>,
    object_store_lookups: Counter<u64>,
    reference_validation_failures: Counter<u64>,
}

impl ReplayOtelMetrics {
    fn new() -> Self {
        let meter = global::meter("snaketron-server");
        let counter = |name, description| {
            meter
                .u64_counter(name)
                .with_description(description)
                .with_unit("1")
                .build()
        };
        Self {
            loads: counter(
                "snaketron.replay.loads",
                "Successfully loaded replay recordings by source",
            ),
            cache_lookups: counter(
                "snaketron.replay.cache_lookups",
                "Replay cache lookups by bounded outcome",
            ),
            cache_fills: counter(
                "snaketron.replay.cache_fills",
                "Replay cache fill attempts by bounded outcome",
            ),
            object_store_lookups: counter(
                "snaketron.replay.object_store_lookups",
                "Durable replay object lookups by bounded outcome",
            ),
            reference_validation_failures: counter(
                "snaketron.replay.reference_validation_failures",
                "Replay requests rejected before cache or object-store I/O",
            ),
        }
    }
}

static REPLAY_OTEL_METRICS: OnceLock<ReplayOtelMetrics> = OnceLock::new();

fn replay_otel_metrics() -> &'static ReplayOtelMetrics {
    REPLAY_OTEL_METRICS.get_or_init(ReplayOtelMetrics::new)
}

#[derive(Clone)]
pub struct ReplayRepository {
    store: Arc<dyn ReplayStore>,
    cache: Arc<dyn ReplayCache>,
    metrics: ReplayRepositoryMetrics,
}

impl ReplayRepository {
    pub fn new(store: Arc<dyn ReplayStore>, cache: Arc<dyn ReplayCache>) -> Self {
        Self::with_metrics(store, cache, ReplayRepositoryMetrics::default())
    }

    pub fn with_metrics(
        store: Arc<dyn ReplayStore>,
        cache: Arc<dyn ReplayCache>,
        metrics: ReplayRepositoryMetrics,
    ) -> Self {
        Self {
            store,
            cache,
            metrics,
        }
    }

    pub fn metrics(&self) -> ReplayRepositoryMetrics {
        self.metrics.clone()
    }

    pub async fn recording_length(&self, expected: &ReplayObjectMetadata) -> Result<Option<u64>> {
        if expected.format_version != REPLAY_MANIFEST_OBJECT_FORMAT_VERSION {
            self.store.validate_reference(expected)?;
            return Ok(Some(expected.uncompressed_bytes));
        }
        let Some(manifest_load) = self.get_object(expected).await? else {
            return Ok(None);
        };
        let manifest: ReplayChunkManifestV1 =
            serde_json::from_slice(&manifest_load.recording.bytes)
                .context("malformed replay chunk manifest")?;
        manifest.validate()?;
        if manifest.game_id != expected.game_id {
            bail!("replay manifest targets a different game");
        }
        Ok(Some(manifest.recording_uncompressed_bytes))
    }

    pub async fn get_recording(
        &self,
        expected: &ReplayObjectMetadata,
    ) -> Result<Option<ReplayLoad>> {
        if expected.format_version != REPLAY_MANIFEST_OBJECT_FORMAT_VERSION {
            return self.get_object(expected).await;
        }

        let Some(manifest_load) = self.get_object(expected).await? else {
            return Ok(None);
        };
        let manifest: ReplayChunkManifestV1 =
            serde_json::from_slice(&manifest_load.recording.bytes)
                .context("malformed replay chunk manifest")?;
        manifest.validate()?;
        if manifest.game_id != expected.game_id {
            bail!("replay manifest targets a different game");
        }
        if manifest.recording_uncompressed_bytes > MAX_RECONSTRUCTED_REPLAY_BYTES {
            bail!("replay manifest exceeds the reconstruction safety limit");
        }

        let mut source = manifest_load.source;
        let mut bytes = Vec::with_capacity(
            usize::try_from(manifest.recording_uncompressed_bytes)
                .context("replay length does not fit this platform")?,
        );
        for chunk in &manifest.chunks {
            let Some(loaded) = self.get_object(chunk).await? else {
                return Ok(None);
            };
            if loaded.source == ReplayLoadSource::ObjectStore {
                source = ReplayLoadSource::ObjectStore;
            }
            bytes.extend_from_slice(&loaded.recording.bytes);
        }
        verify_reconstructed_recording(&manifest, &bytes)?;
        Ok(Some(ReplayLoad {
            recording: ReplayRecording {
                metadata: expected.clone(),
                bytes,
            },
            source,
        }))
    }

    /// Load a half-open byte range of canonical replay JSON. Manifest-backed
    /// recordings fetch and cache only overlapping chunks; legacy one-object
    /// recordings preserve compatibility and are sliced after verification.
    pub async fn get_recording_range(
        &self,
        expected: &ReplayObjectMetadata,
        start: u64,
        end: u64,
    ) -> Result<Option<ReplayRangeLoad>> {
        if expected.format_version != REPLAY_MANIFEST_OBJECT_FORMAT_VERSION {
            let Some(loaded) = self.get_object(expected).await? else {
                return Ok(None);
            };
            let total = loaded.recording.bytes.len() as u64;
            validate_replay_range(start, end, total)?;
            return Ok(Some(ReplayRangeLoad {
                bytes: loaded.recording.bytes[start as usize..end as usize].to_vec(),
                total_bytes: total,
                start,
                end,
                source: loaded.source,
            }));
        }

        let Some(manifest_load) = self.get_object(expected).await? else {
            return Ok(None);
        };
        let manifest: ReplayChunkManifestV1 =
            serde_json::from_slice(&manifest_load.recording.bytes)
                .context("malformed replay chunk manifest")?;
        manifest.validate()?;
        if manifest.game_id != expected.game_id {
            bail!("replay manifest targets a different game");
        }
        let total = manifest.recording_uncompressed_bytes;
        validate_replay_range(start, end, total)?;

        let capacity = usize::try_from(end - start)
            .context("requested replay range does not fit this platform")?;
        let mut bytes = Vec::with_capacity(capacity);
        let mut source = manifest_load.source;
        let mut chunk_start = 0u64;
        for chunk in &manifest.chunks {
            let chunk_end = chunk_start
                .checked_add(chunk.uncompressed_bytes)
                .context("replay chunk offset overflow")?;
            if start < chunk_end && end > chunk_start {
                let Some(loaded) = self.get_object(chunk).await? else {
                    return Ok(None);
                };
                if loaded.source == ReplayLoadSource::ObjectStore {
                    source = ReplayLoadSource::ObjectStore;
                }
                let local_start = usize::try_from(start.saturating_sub(chunk_start))?;
                let local_end = usize::try_from(end.min(chunk_end) - chunk_start)?;
                bytes.extend_from_slice(&loaded.recording.bytes[local_start..local_end]);
            }
            chunk_start = chunk_end;
        }
        if bytes.len() != capacity {
            bail!("replay range reconstruction length mismatch");
        }
        Ok(Some(ReplayRangeLoad {
            bytes,
            total_bytes: total,
            start,
            end,
            source,
        }))
    }

    async fn get_object(&self, expected: &ReplayObjectMetadata) -> Result<Option<ReplayLoad>> {
        if let Err(error) = self.store.validate_reference(expected) {
            self.metrics.record_reference_validation_failure();
            return Err(error).context("invalid durable replay reference");
        }

        match self.cache.get(expected).await {
            Ok(ReplayCacheLookup::Hit(value)) => match decode_cache_value(expected, &value) {
                Ok(bytes) => {
                    self.metrics.record_cache_lookup(CacheLookupMetric::Hit);
                    self.metrics.record_load(ReplayLoadSource::Cache);
                    return Ok(Some(ReplayLoad {
                        recording: ReplayRecording {
                            metadata: expected.clone(),
                            bytes,
                        },
                        source: ReplayLoadSource::Cache,
                    }));
                }
                Err(error) => {
                    self.metrics.record_cache_lookup(CacheLookupMetric::Corrupt);
                    warn!(
                        game_id = expected.game_id,
                        %error,
                        "ignoring corrupt replay cache value and falling back to object storage"
                    );
                }
            },
            Ok(ReplayCacheLookup::Miss) => {
                self.metrics.record_cache_lookup(CacheLookupMetric::Miss);
            }
            Ok(ReplayCacheLookup::RejectedOversized) => {
                self.metrics
                    .record_cache_lookup(CacheLookupMetric::Oversized);
                warn!(
                    game_id = expected.game_id,
                    "ignoring oversized replay cache value and falling back to object storage"
                );
            }
            Err(error) => {
                self.metrics
                    .record_cache_lookup(CacheLookupMetric::BackendError);
                warn!(
                    game_id = expected.game_id,
                    %error,
                    "replay cache lookup unavailable; falling back to object storage"
                );
            }
        }

        let stored = match self.store.get_recording(expected).await {
            Ok(Some(recording)) => {
                self.metrics
                    .record_object_store_lookup(ObjectStoreMetric::Hit);
                recording
            }
            Ok(None) => {
                self.metrics
                    .record_object_store_lookup(ObjectStoreMetric::Miss);
                return Ok(None);
            }
            Err(error) => {
                self.metrics
                    .record_object_store_lookup(ObjectStoreMetric::Error);
                return Err(error).context("durable replay lookup failed");
            }
        };

        if stored.metadata != *expected {
            self.metrics.record_reference_validation_failure();
            bail!("replay store returned metadata other than the requested durable reference");
        }
        let cache_value = match encode_cache_value(expected, &stored.bytes) {
            Ok(value) => value,
            Err(error) => {
                self.metrics.record_reference_validation_failure();
                return Err(error).context("replay store returned unverifiable bytes");
            }
        };

        match self.cache.put(expected, &cache_value).await {
            Ok(ReplayCachePutOutcome::Stored) => {
                self.metrics.record_cache_fill(CacheFillMetric::Stored);
            }
            Ok(ReplayCachePutOutcome::SkippedOversized) => {
                self.metrics.record_cache_fill(CacheFillMetric::Oversized);
            }
            Err(error) => {
                self.metrics
                    .record_cache_fill(CacheFillMetric::BackendError);
                warn!(
                    game_id = expected.game_id,
                    %error,
                    "replay cache fill unavailable; returning verified object-store recording"
                );
            }
        }

        self.metrics.record_load(ReplayLoadSource::ObjectStore);
        Ok(Some(ReplayLoad {
            recording: stored,
            source: ReplayLoadSource::ObjectStore,
        }))
    }
}

fn validate_replay_range(start: u64, end: u64, total: u64) -> Result<()> {
    if start >= end {
        bail!("replay byte range must be non-empty");
    }
    if end > total {
        bail!("replay byte range exceeds recording length");
    }
    if end - start > MAX_REPLAY_RANGE_BYTES {
        bail!("replay byte range exceeds the repository response limit");
    }
    Ok(())
}

fn verify_reconstructed_recording(manifest: &ReplayChunkManifestV1, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != manifest.recording_uncompressed_bytes {
        bail!("reconstructed replay length does not match its manifest");
    }
    if hex::encode(Sha256::digest(bytes)) != manifest.recording_uncompressed_sha256 {
        bail!("reconstructed replay checksum does not match its manifest");
    }
    Ok(())
}

fn encode_cache_value(expected: &ReplayObjectMetadata, bytes: &[u8]) -> Result<Vec<u8>> {
    expected.validate()?;
    verify_uncompressed_bytes(expected, bytes)?;
    let mut value = Vec::with_capacity(CACHE_ENVELOPE_HEADER_BYTES + bytes.len());
    value.extend_from_slice(CACHE_ENVELOPE_MAGIC);
    value.extend_from_slice(&CACHE_ENVELOPE_VERSION.to_be_bytes());
    value.extend_from_slice(&reference_digest(expected));
    value.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    value.extend_from_slice(bytes);
    Ok(value)
}

fn decode_cache_value(expected: &ReplayObjectMetadata, value: &[u8]) -> Result<Vec<u8>> {
    expected.validate()?;
    if value.len() < CACHE_ENVELOPE_HEADER_BYTES {
        bail!("replay cache value is shorter than its integrity envelope");
    }
    if &value[..CACHE_ENVELOPE_MAGIC.len()] != CACHE_ENVELOPE_MAGIC {
        bail!("replay cache value has invalid magic");
    }

    let version_start = CACHE_ENVELOPE_MAGIC.len();
    let digest_start = version_start + 2;
    let length_start = digest_start + REFERENCE_DIGEST_BYTES;
    let payload_start = length_start + 8;
    let version = u16::from_be_bytes(
        value[version_start..digest_start]
            .try_into()
            .context("replay cache value has invalid version bytes")?,
    );
    if version != CACHE_ENVELOPE_VERSION {
        bail!("unsupported replay cache envelope version {version}");
    }
    if value[digest_start..length_start] != reference_digest(expected) {
        bail!("replay cache value does not match its durable reference");
    }

    let advertised_length = u64::from_be_bytes(
        value[length_start..payload_start]
            .try_into()
            .context("replay cache value has invalid length bytes")?,
    );
    let advertised_length = usize::try_from(advertised_length)
        .context("replay cache payload length does not fit this platform")?;
    let expected_total = payload_start
        .checked_add(advertised_length)
        .context("replay cache payload length overflow")?;
    if value.len() != expected_total {
        bail!("replay cache payload length mismatch");
    }

    let bytes = value[payload_start..].to_vec();
    verify_uncompressed_bytes(expected, &bytes)?;
    Ok(bytes)
}

fn verify_uncompressed_bytes(expected: &ReplayObjectMetadata, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != expected.uncompressed_bytes {
        bail!("replay cache uncompressed length mismatch");
    }
    if hex::encode(Sha256::digest(bytes)) != expected.uncompressed_sha256 {
        bail!("replay cache uncompressed checksum mismatch");
    }
    Ok(())
}

fn reference_digest(expected: &ReplayObjectMetadata) -> [u8; REFERENCE_DIGEST_BYTES] {
    let mut digest = Sha256::new();
    digest.update(b"snaketron-replay-cache-reference-v1\0");
    digest.update(expected.format_version.to_be_bytes());
    digest.update(expected.game_id.to_be_bytes());
    update_length_prefixed(&mut digest, expected.object_key.as_bytes());
    update_length_prefixed(&mut digest, expected.uncompressed_sha256.as_bytes());
    update_length_prefixed(&mut digest, expected.compressed_sha256.as_bytes());
    digest.update(expected.uncompressed_bytes.to_be_bytes());
    digest.update(expected.compressed_bytes.to_be_bytes());
    digest.finalize().into()
}

fn update_length_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay_cache::{FailingReplayCache, InMemoryReplayCache, ReplayCacheFailureMode};
    use crate::replay_store::{InMemoryReplayStore, REPLAY_CHUNK_UNCOMPRESSED_BYTES};
    use async_trait::async_trait;

    #[derive(Clone)]
    struct CountingReplayStore {
        inner: InMemoryReplayStore,
        get_calls: Arc<AtomicU64>,
    }

    impl CountingReplayStore {
        fn new(inner: InMemoryReplayStore) -> Self {
            Self {
                inner,
                get_calls: Arc::new(AtomicU64::new(0)),
            }
        }

        fn get_call_count(&self) -> u64 {
            self.get_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ReplayStore for CountingReplayStore {
        fn validate_reference(&self, expected: &ReplayObjectMetadata) -> Result<()> {
            self.inner.validate_reference(expected)
        }

        async fn put_recording(
            &self,
            game_id: u32,
            recording_bytes: &[u8],
        ) -> Result<ReplayObjectMetadata> {
            self.inner.put_recording(game_id, recording_bytes).await
        }

        async fn get_recording(
            &self,
            expected: &ReplayObjectMetadata,
        ) -> Result<Option<ReplayRecording>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_recording(expected).await
        }
    }

    async fn fixture(
        game_id: u32,
    ) -> (
        Arc<CountingReplayStore>,
        Arc<InMemoryReplayCache>,
        ReplayObjectMetadata,
        Vec<u8>,
    ) {
        let durable = InMemoryReplayStore::new();
        let bytes = format!(r#"{{"game_id":{game_id},"events":[{{"tick":1}}]}}"#).into_bytes();
        let expected = durable.put_recording(game_id, &bytes).await.unwrap();
        (
            Arc::new(CountingReplayStore::new(durable)),
            Arc::new(InMemoryReplayCache::new()),
            expected,
            bytes,
        )
    }

    #[tokio::test]
    async fn cache_hit_returns_verified_bytes_without_object_store_read() {
        let (store, cache, expected, bytes) = fixture(101).await;
        cache
            .seed_raw(&expected, encode_cache_value(&expected, &bytes).unwrap())
            .await
            .unwrap();
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let loaded = repository.get_recording(&expected).await.unwrap().unwrap();

        assert_eq!(loaded.source, ReplayLoadSource::Cache);
        assert_eq!(loaded.recording.bytes, bytes);
        assert_eq!(store.get_call_count(), 0);
        assert_eq!(repository.metrics().snapshot().cache_hits, 1);
    }

    #[tokio::test]
    async fn miss_reads_object_store_once_fills_and_then_hits_cache() {
        let (store, cache, expected, bytes) = fixture(102).await;
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let first = repository.get_recording(&expected).await.unwrap().unwrap();
        let second = repository.get_recording(&expected).await.unwrap().unwrap();

        assert_eq!(first.source, ReplayLoadSource::ObjectStore);
        assert_eq!(second.source, ReplayLoadSource::Cache);
        assert_eq!(first.recording.bytes, bytes);
        assert_eq!(second.recording.bytes, bytes);
        assert_eq!(store.get_call_count(), 1);
        assert_eq!(cache.put_call_count(), 1);
        assert_eq!(cache.object_count().await, 1);
        assert_eq!(
            repository.metrics().snapshot(),
            ReplayRepositoryMetricsSnapshot {
                cache_hits: 1,
                cache_misses: 1,
                cache_fills: 1,
                object_store_hits: 1,
                ..ReplayRepositoryMetricsSnapshot::default()
            }
        );
    }

    #[tokio::test]
    async fn cache_outage_falls_back_and_does_not_hide_verified_recording() {
        let (store, _cache, expected, bytes) = fixture(103).await;
        let cache = Arc::new(FailingReplayCache::new(ReplayCacheFailureMode::All));
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let loaded = repository.get_recording(&expected).await.unwrap().unwrap();

        assert_eq!(loaded.source, ReplayLoadSource::ObjectStore);
        assert_eq!(loaded.recording.bytes, bytes);
        assert_eq!(store.get_call_count(), 1);
        assert_eq!(cache.get_call_count(), 1);
        assert_eq!(cache.put_call_count(), 1);
        let metrics = repository.metrics().snapshot();
        assert_eq!(metrics.cache_get_failures, 1);
        assert_eq!(metrics.cache_fill_failures, 1);
        assert_eq!(metrics.object_store_hits, 1);
    }

    #[tokio::test]
    async fn corrupt_cache_falls_back_to_object_store_and_repairs_entry() {
        let (store, cache, expected, bytes) = fixture(104).await;
        let mut corrupt_value = encode_cache_value(&expected, &bytes).unwrap();
        let last = corrupt_value.len() - 1;
        corrupt_value[last] ^= 0xff;
        cache.seed_raw(&expected, corrupt_value).await.unwrap();
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let repaired = repository.get_recording(&expected).await.unwrap().unwrap();
        let cached = repository.get_recording(&expected).await.unwrap().unwrap();

        assert_eq!(repaired.source, ReplayLoadSource::ObjectStore);
        assert_eq!(cached.source, ReplayLoadSource::Cache);
        assert_eq!(cached.recording.bytes, bytes);
        assert_eq!(store.get_call_count(), 1);
        assert_eq!(repository.metrics().snapshot().cache_corruptions, 1);
    }

    #[tokio::test]
    async fn old_cache_envelope_version_is_not_trusted_and_is_repaired() {
        let (store, cache, expected, bytes) = fixture(105).await;
        let mut old_value = encode_cache_value(&expected, &bytes).unwrap();
        let version_start = CACHE_ENVELOPE_MAGIC.len();
        old_value[version_start..version_start + 2].copy_from_slice(&2_u16.to_be_bytes());
        cache.seed_raw(&expected, old_value).await.unwrap();
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let repaired = repository.get_recording(&expected).await.unwrap().unwrap();
        let cached = repository.get_recording(&expected).await.unwrap().unwrap();

        assert_eq!(repaired.source, ReplayLoadSource::ObjectStore);
        assert_eq!(cached.source, ReplayLoadSource::Cache);
        assert_eq!(store.get_call_count(), 1);
        assert_eq!(repository.metrics().snapshot().cache_corruptions, 1);
    }

    #[tokio::test]
    async fn large_manifest_reconstructs_full_and_cross_chunk_ranges_through_cache() {
        let durable = InMemoryReplayStore::new();
        let mut bytes = vec![0u8; 9 * 1024 * 1024 + 137];
        for (index, chunk) in bytes
            .chunks_mut(REPLAY_CHUNK_UNCOMPRESSED_BYTES)
            .enumerate()
        {
            chunk.fill(index as u8);
        }
        let expected = durable.put_recording(107, &bytes).await.unwrap();
        let store = Arc::new(CountingReplayStore::new(durable));
        let cache = Arc::new(InMemoryReplayCache::new());
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        assert_eq!(
            repository.recording_length(&expected).await.unwrap(),
            Some(bytes.len() as u64)
        );
        let start = REPLAY_CHUNK_UNCOMPRESSED_BYTES as u64 - 7;
        let end = start + 64;
        let first = repository
            .get_recording_range(&expected, start, end)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.bytes, bytes[start as usize..end as usize]);
        assert_eq!(first.source, ReplayLoadSource::ObjectStore);
        let calls_after_first = store.get_call_count();

        let cached = repository
            .get_recording_range(&expected, start, end)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.bytes, first.bytes);
        assert_eq!(cached.source, ReplayLoadSource::Cache);
        assert_eq!(store.get_call_count(), calls_after_first);

        let full = repository.get_recording(&expected).await.unwrap().unwrap();
        assert_eq!(full.recording.bytes, bytes);
        assert_eq!(full.source, ReplayLoadSource::ObjectStore);
        let cached_full = repository.get_recording(&expected).await.unwrap().unwrap();
        assert_eq!(cached_full.recording.bytes, bytes);
        assert_eq!(cached_full.source, ReplayLoadSource::Cache);
    }

    #[tokio::test]
    async fn stale_same_game_manifest_cannot_replace_committed_content() {
        let durable = InMemoryReplayStore::new();
        let committed_bytes = vec![b'n'; REPLAY_CHUNK_UNCOMPRESSED_BYTES + 1];
        let stale_bytes = vec![b's'; REPLAY_CHUNK_UNCOMPRESSED_BYTES + 1];
        let committed = durable.put_recording(108, &committed_bytes).await.unwrap();
        let stale = durable.put_recording(108, &stale_bytes).await.unwrap();
        assert_ne!(committed.object_key, stale.object_key);

        let repository =
            ReplayRepository::new(Arc::new(durable), Arc::new(InMemoryReplayCache::new()));
        assert_eq!(
            repository
                .get_recording(&committed)
                .await
                .unwrap()
                .unwrap()
                .recording
                .bytes,
            committed_bytes
        );
        assert_eq!(
            repository
                .get_recording(&stale)
                .await
                .unwrap()
                .unwrap()
                .recording
                .bytes,
            stale_bytes
        );
    }

    #[tokio::test]
    async fn unsupported_recording_version_fails_closed_before_any_backend_io() {
        let (store, cache, mut expected, _bytes) = fixture(106).await;
        expected.format_version = 999;
        let repository = ReplayRepository::new(store.clone(), cache.clone());

        let error = repository.get_recording(&expected).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("invalid durable replay reference")
        );
        assert_eq!(store.get_call_count(), 0);
        assert_eq!(cache.get_call_count(), 0);
        assert_eq!(
            repository
                .metrics()
                .snapshot()
                .reference_validation_failures,
            1
        );
    }
}
