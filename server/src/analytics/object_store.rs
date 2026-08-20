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

/// The prefix every object of one dataset shares.
///
/// The reading half of [`object_key`], and here rather than at the reader so
/// the two cannot disagree about sanitization — a listing built from an
/// unsanitized dataset name would find nothing and be indistinguishable from
/// a dataset with no data.
pub fn dataset_prefix(dataset: &str) -> String {
    format!("raw/{}/", sanitize(dataset))
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
        "{}dt={}/host={}/{}-{}-{}.json.gz",
        dataset_prefix(dataset),
        sanitize(date),
        sanitize(host),
        sanitize(first_cursor),
        sanitize(last_cursor),
        sanitize(content_hash),
    )
}

/// The partition prefix of a key: the span within which keys sort in the order
/// they were written.
///
/// The reading half of [`object_key`], and here rather than at the reader for
/// the same reason [`dataset_prefix`] is: a parse that disagreed with the
/// writer would group keys that do not belong together, and nothing would say
/// so.
///
/// Only the trailing file name carries the cursor, so two keys are comparable
/// as "written earlier / later" ONLY when they share `dt={date}/host={host}`.
/// Across hosts the comparison is meaningless in both directions —
/// `host=euw1-5` sorts below `host=use1-3` because `'e' < 'u'`, and
/// `host=use1-12` sorts below `host=use1-3` because `'1' < '3'` — which is why
/// one global mark cannot express what has been folded.
///
/// A key that does not carry the layout yields `""`. Those share one bucket:
/// they have no partition to order within, so a single mark is the best
/// available answer for them, and it is exactly the old behaviour.
pub fn partition_prefix(key: &str) -> &str {
    // The first `/dt=` is the real one: `sanitize` turns every `/` in a
    // dataset name into `-`, so a dataset cannot contribute another.
    let Some(start) = key.find("/dt=") else {
        return "";
    };
    let after_dataset = &key[start + 1..];
    let Some(date_end) = after_dataset.find('/') else {
        return "";
    };
    let after_date = &after_dataset[date_end + 1..];
    if !after_date.starts_with("host=") {
        return "";
    }
    let Some(host_end) = after_date.find('/') else {
        return "";
    };
    &after_dataset[..date_end + 1 + host_end]
}

/// The `dt=` day of a partition prefix, or `None` when the prefix carries no
/// day — which is only the `""` bucket [`partition_prefix`] returns for a key
/// outside the layout.
pub fn prefix_day(prefix: &str) -> Option<&str> {
    prefix.strip_prefix("dt=")?.split('/').next()
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
        Self::client_for(
            region,
            std::env::var("SNAKETRON_S3_ENDPOINT").ok().as_deref(),
        )
        .await
    }

    /// Builds a client, optionally against a non-AWS endpoint.
    ///
    /// The endpoint override exists so the pipeline can be exercised end to end
    /// against LocalStack. It also forces path-style addressing, because a
    /// virtual-host bucket name does not resolve against a local endpoint.
    pub async fn client_for(region: &str, endpoint: Option<&str>) -> aws_sdk_s3::Client {
        let loader = aws_config::from_env().region(aws_config::Region::new(region.to_owned()));
        let config = loader.load().await;
        match endpoint {
            None => aws_sdk_s3::Client::new(&config),
            Some(endpoint) => {
                let s3_config = aws_sdk_s3::config::Builder::from(&config)
                    .endpoint_url(endpoint)
                    .force_path_style(true)
                    .build();
                aws_sdk_s3::Client::from_conf(s3_config)
            }
        }
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

    /// The committer lists by prefix, so the prefix has to be a literal
    /// prefix of what the writer produces — including for a dataset name that
    /// sanitizes, where a listing built from the raw name would find nothing
    /// and be indistinguishable from a dataset with no data.
    #[test]
    fn every_written_key_lies_under_its_dataset_prefix() {
        for dataset in ["game-events", "websocket/events", "a b"] {
            let key = object_key(dataset, "2026-08-19", "use1-7", "a", "b", "c");
            assert!(
                key.starts_with(&dataset_prefix(dataset)),
                "{key} is not under {}",
                dataset_prefix(dataset)
            );
        }
    }

    /// A traversal attempt in a component must not escape the prefix.
    #[test]
    fn key_components_cannot_escape_the_prefix() {
        let key = object_key("d", "../../etc", "h/../x", "a", "b", "c");
        assert!(!key.contains(".."));
        assert!(key.starts_with("raw/d/dt="));
    }

    /// The prefix must be exactly the span in which keys sort chronologically:
    /// one date and one host. Anything wider is not ordered, which is the
    /// whole reason the resume state is keyed by it.
    #[test]
    fn the_partition_prefix_is_the_date_and_host_of_a_key() {
        let key = object_key("game-events", "2026-08-19", "use1-3", "a", "b", "c");
        assert_eq!(partition_prefix(&key), "dt=2026-08-19/host=use1-3");
        assert_eq!(prefix_day("dt=2026-08-19/host=use1-3"), Some("2026-08-19"));
    }

    /// Two keys share a prefix exactly when they came from the same host on
    /// the same day — never across regions, and never across server ids.
    #[test]
    fn keys_from_different_hosts_or_days_never_share_a_prefix() {
        let build = |date: &str, host: &str| {
            partition_prefix(&object_key("game-events", date, host, "a", "b", "c")).to_owned()
        };
        assert_eq!(build("2026-08-19", "use1-3"), build("2026-08-19", "use1-3"));
        assert_ne!(build("2026-08-19", "use1-3"), build("2026-08-19", "euw1-5"));
        assert_ne!(
            build("2026-08-19", "use1-3"),
            build("2026-08-19", "use1-12")
        );
        assert_ne!(build("2026-08-19", "use1-3"), build("2026-08-20", "use1-3"));
    }

    /// A key that predates the layout, or that no `object_key` produced, has
    /// no partition to be ordered within. It gets the one shared bucket, which
    /// is the old single-mark behaviour and is correct for exactly these keys.
    #[test]
    fn a_key_outside_the_layout_falls_into_one_shared_bucket() {
        for key in [
            "a.json.gz",
            "raw/proof/k1",
            "raw/game-events/dt=2026-08-19/loose.json.gz",
            "raw/game-events/dt=2026-08-19/host=use1-3",
        ] {
            assert_eq!(partition_prefix(key), "", "{key} must not parse a prefix");
        }
        assert_eq!(prefix_day(""), None);
    }

    #[test]
    fn content_hash_is_stable_and_short() {
        assert_eq!(content_hash("abc").len(), 16);
        assert_eq!(content_hash("abc"), content_hash("abc"));
        assert_ne!(content_hash("abc"), content_hash("abd"));
    }
}
