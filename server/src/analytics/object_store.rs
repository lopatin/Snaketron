//! Additive, idempotent object writes.
//!
//! Two properties come from one primitive. Every upload is a conditional write
//! that fails if the key exists, so:
//!
//! * **Additive** — nothing is ever overwritten, enforced at the API rather
//!   than by convention.
//! * **Idempotent** — a replay of the same batch computes the same key and is
//!   refused, which the writer treats as SUCCESS. That refusal is the
//!   idempotency signal, not an error.
//!
//! Keys are derived from the data (a cursor range plus a content hash), never
//! from wall-clock time. A timestamp would produce a different key on replay
//! and therefore a duplicate object, silently breaking the guarantee.

use std::io::Write;

use anyhow::{Context, Result};
use async_trait::async_trait;
use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};

/// Outcome of a conditional put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// A new object was created.
    Written,
    /// The key already existed. Not a failure: the bytes are already there.
    AlreadyPresent,
}

/// The narrow surface the exporter needs, so it can be tested without S3.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Writes only if `key` does not exist.
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome>;
}

/// Compresses NDJSON for storage.
///
/// Level 6 (the library default) rather than 9: measured, level 9 costs
/// roughly 2.2-2.4x the CPU for only 3-9% smaller output, which is a bad trade
/// when that CPU is spent in the same container as the game loop and the
/// storage saved is cents.
pub fn compress(body: &str) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body.as_bytes())
        .context("compressing analytics batch")?;
    encoder.finish().context("finishing analytics gzip stream")
}

/// First 16 hex chars of the SHA-256 of the body.
///
/// Including it in the key makes a refused write unambiguous: if the key
/// exists, the bytes behind it are identical, so nothing was lost.
pub fn content_hash(body: &str) -> String {
    let digest = Sha256::digest(body.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Sanitizes a component so it cannot escape its prefix or break Hive-style
/// partition parsing.
fn sanitize(component: &str) -> String {
    component
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Builds the object key.
///
/// Partitioned by event date and host only — no hour, no minute, and nothing
/// from the game's internal partitioning. The minute lives nowhere: a
/// minute-level partition would be over a million partitions a year for no
/// gain, while date plus host keeps it to a few thousand.
pub fn object_key(
    dataset: &str,
    date: &str,
    host: &str,
    first_cursor: &str,
    last_cursor: &str,
    content_hash: &str,
) -> String {
    format!(
        "raw/{}/dt={}/host={}/{}-{}-{}.json.gz",
        sanitize(dataset),
        sanitize(date),
        sanitize(host),
        sanitize(first_cursor),
        sanitize(last_cursor),
        sanitize(content_hash),
    )
}

/// S3-backed store using conditional writes.
pub struct S3ObjectStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3ObjectStore {
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    /// Builds a client pinned to an explicit region.
    ///
    /// The Rust SDK does not follow S3 region redirects, and every region
    /// writes to the one US bucket, so the region must be stated rather than
    /// inherited from the task's own environment.
    pub async fn client_for_region(region: &str) -> aws_sdk_s3::Client {
        let config = aws_config::from_env()
            .region(aws_config::Region::new(region.to_owned()))
            .load()
            .await;
        aws_sdk_s3::Client::new(&config)
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .content_type("application/x-ndjson")
            .content_encoding("gzip")
            // The whole additive/idempotent guarantee rests on this header.
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(_) => Ok(PutOutcome::Written),
            Err(error) => {
                // PutObjectError has no modeled PreconditionFailed variant, so
                // the raw HTTP status is the reliable signal. 412 means the key
                // is already there, which for a content-addressed key means the
                // identical bytes are already there.
                let status = error
                    .raw_response()
                    .map(|response| response.status().as_u16());
                match status {
                    Some(412) => Ok(PutOutcome::AlreadyPresent),
                    _ => Err(anyhow::Error::new(error)
                        .context(format!("conditional put failed for {key}"))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn compression_round_trips() {
        let body = "{\"a\":1}\n{\"a\":2}\n";
        let compressed = compress(body).unwrap();
        assert!(compressed.len() > 2);
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut restored = String::new();
        decoder.read_to_string(&mut restored).unwrap();
        assert_eq!(restored, body);
    }

    /// The key must be a pure function of the data. A wall-clock component
    /// would produce a different key on replay and therefore a duplicate.
    #[test]
    fn keys_are_deterministic_for_identical_input() {
        let body = "{\"a\":1}\n";
        let build = || {
            object_key(
                "game-events",
                "2026-08-19",
                "use1-42",
                "1755600000000-0",
                "1755600009999-3",
                &content_hash(body),
            )
        };
        assert_eq!(
            build(),
            build(),
            "the key must be a pure function of the data"
        );
        // Pinned so a change to the key layout is a deliberate, visible edit.
        assert_eq!(
            build(),
            concat!(
                "raw/game-events/dt=2026-08-19/host=use1-42/",
                "1755600000000-0-1755600009999-3-e346432021b04179.json.gz"
            )
        );
    }

    #[test]
    fn different_content_yields_a_different_key() {
        let one = object_key("d", "2026-08-19", "h", "a", "b", &content_hash("x"));
        let two = object_key("d", "2026-08-19", "h", "a", "b", &content_hash("y"));
        assert_ne!(one, two);
    }

    #[test]
    fn the_key_is_partitioned_by_date_and_host_only() {
        let key = object_key("game-events", "2026-08-19", "use1-7", "a", "b", "c");
        assert!(key.starts_with("raw/game-events/dt=2026-08-19/host=use1-7/"));
        assert!(!key.contains("hh="), "no hour partition");
        assert!(!key.contains("mm="), "no minute partition");
        assert!(key.ends_with(".json.gz"), "lowercase extension for Athena");
    }

    /// A traversal attempt in a component must not escape the prefix.
    #[test]
    fn key_components_cannot_escape_the_prefix() {
        let key = object_key("d", "../../etc", "h/../x", "a", "b", "c");
        assert!(!key.contains(".."));
        assert!(key.starts_with("raw/d/dt="));
    }

    #[test]
    fn content_hash_is_stable_and_short() {
        assert_eq!(content_hash("abc").len(), 16);
        assert_eq!(content_hash("abc"), content_hash("abc"));
        assert_ne!(content_hash("abc"), content_hash("abd"));
    }
}
