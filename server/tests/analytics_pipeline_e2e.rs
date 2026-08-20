//! End-to-end proof of the analytics pipeline against real infrastructure.
//!
//! Exercises the whole durable path with nothing stubbed: emit a proto event,
//! serialize it, push it through a real Valkey stream, drain it through a real
//! consumer group, compress it, and write it to a real S3 endpoint — then read
//! the object back and assert the bytes are what was emitted.
//!
//! Requires Valkey on 6379 and an S3 endpoint. Against LocalStack, run
//! `./test-deps.sh` and nothing else — `.cargo/config.toml` already points the
//! SDK at `localhost:4566`.
//!
//! Against real AWS (proven this way on 2026-08-19), the same config file is
//! what you have to defeat: it pins `AWS_ACCESS_KEY_ID=test` and an endpoint,
//! and env credentials outrank `~/.aws/credentials`. So supply both:
//!
//! ```text
//! export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
//! export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
//! SNAKETRON_E2E=1 SNAKETRON_E2E_S3_ENDPOINT=aws \
//!   SNAKETRON_E2E_BUCKET=<a bucket you own> \
//!   cargo test --test analytics_pipeline_e2e -- --test-threads=1
//! ```
//!
//! Set `SNAKETRON_E2E=1` to demand that infrastructure. Without it these skip,
//! so a laptop without Docker is not blocked; WITH it, missing infrastructure
//! is a hard failure. That distinction matters — a test that reports "ok"
//! because it quietly did nothing is worse than no test, since it makes an
//! unproven pipeline look verified.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use redis::AsyncCommands;
use server::analytics::batch::{BufferedEvent, EventBatcher};
use server::analytics::emitter::EmitterMetrics;
use server::analytics::event::{EventIdentity, EventOrigin, envelope, to_json_line};
use server::analytics::exporter::{ExportTarget, default_limits, write_batch};
use server::analytics::object_store::{ObjectStore, PutOutcome, S3ObjectStore};
use server::analytics::proto;

const REGION: &str = "us-east-1";

fn redis_url() -> String {
    std::env::var("SNAKETRON_E2E_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned())
}

/// The default LocalStack endpoint, used when nothing else says otherwise.
const LOCALSTACK: &str = "http://localhost:4566";

/// Where S3 lives for this run, resolved deliberately rather than inherited.
///
/// **These tests never touch real AWS unless explicitly told to.** The
/// fallback is LocalStack, not AWS: a test that writes to a real bucket by
/// default is one stray environment away from writing to a real bucket
/// someone cares about. Real AWS requires `SNAKETRON_E2E_S3_ENDPOINT=aws`.
///
/// The resolved value is then pinned into `AWS_ENDPOINT_URL_S3`, which
/// outranks `AWS_ENDPOINT_URL` in the SDK, so ambient configuration cannot
/// redirect the run afterwards. `.cargo/config.toml` sets
/// `AWS_ENDPOINT_URL=http://localhost:4566` for every cargo process here, and
/// silently inheriting it is what once sent a "real AWS" run to a LocalStack
/// that was not running — a `dispatch failure` that reads like a network
/// fault.
///
/// Precedence:
///   1. `SNAKETRON_E2E_S3_ENDPOINT=aws` — real AWS, opt-in only. Also needs
///      real credentials in the environment, because the same config file
///      pins `AWS_ACCESS_KEY_ID=test` and env credentials outrank
///      `~/.aws/credentials`.
///   2. `SNAKETRON_E2E_S3_ENDPOINT=<url>` — that endpoint.
///   3. `AWS_ENDPOINT_URL` — the repo's LocalStack default.
///   4. Otherwise `LOCALSTACK`.
fn resolve_endpoint() -> Option<String> {
    let explicit = std::env::var("SNAKETRON_E2E_S3_ENDPOINT").ok();
    let endpoint = match explicit.as_deref() {
        // The one and only way to reach real AWS.
        Some("aws") => None,
        Some(value) if !value.is_empty() => Some(value.to_owned()),
        _ => Some(match std::env::var("AWS_ENDPOINT_URL") {
            Ok(value) if !value.is_empty() => value,
            _ => LOCALSTACK.to_owned(),
        }),
    };

    // Pin the service-specific endpoint so nothing ambient can override the
    // choice made above. Real AWS gets its true regional URL rather than an
    // unset variable, because "unset" is what let LocalStack win before.
    let pinned = endpoint
        .clone()
        .unwrap_or_else(|| format!("https://s3.{REGION}.amazonaws.com"));
    // SAFETY: set once, before any client is built, and every test in this
    // file runs under `--test-threads=1` for exactly this reason.
    unsafe {
        std::env::set_var("AWS_ENDPOINT_URL_S3", &pinned);
    }
    endpoint
}

fn s3_endpoint() -> Option<String> {
    resolve_endpoint()
}

/// The bucket is a fixed name, overridable so a real-AWS proof can be pointed
/// at one you own rather than at whatever `snaketron-analytics-e2e` happens to
/// be. Per-run isolation lives on the OBJECT PREFIX, not here: each test mints
/// a `Uuid::now_v7()` and folds it into the dataset name, so repeated runs
/// never collide even though they share a bucket.
fn bucket() -> String {
    std::env::var("SNAKETRON_E2E_BUCKET").unwrap_or_else(|_| "snaketron-analytics-e2e".to_owned())
}

fn origin() -> EventOrigin {
    EventOrigin {
        environment: "e2e".to_owned(),
        region: "use1".to_owned(),
        aws_region: REGION.to_owned(),
        instance_id: "1:e2e".to_owned(),
    }
}

/// Whether infrastructure is required rather than optional.
fn e2e_required() -> bool {
    std::env::var("SNAKETRON_E2E").is_ok_and(|value| value == "1")
}

/// Reports a missing dependency. Panics under `SNAKETRON_E2E=1` so an
/// intended proof run cannot pass by doing nothing.
fn unavailable(what: &str) -> Option<std::convert::Infallible> {
    let message = format!("{what} unavailable; run ./test-deps.sh");
    assert!(!e2e_required(), "SNAKETRON_E2E=1 but {message}");
    eprintln!("SKIP: {message}");
    None
}

async fn redis_or_skip() -> Option<redis::aio::MultiplexedConnection> {
    let client = match redis::Client::open(redis_url()) {
        Ok(client) => client,
        Err(_) => {
            unavailable("Valkey")?;
            return None;
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(3),
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(connection)) => Some(connection),
        _ => {
            unavailable(&format!("Valkey on {}", redis_url()))?;
            None
        }
    }
}

async fn s3_or_skip() -> Option<aws_sdk_s3::Client> {
    // LocalStack accepts any credentials but the SDK still requires some.
    // Against real AWS the ambient credential chain is used untouched.
    if s3_endpoint().is_some() && std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        }
    }
    let client = S3ObjectStore::client_for(REGION, s3_endpoint().as_deref()).await;
    let target = s3_endpoint().unwrap_or_else(|| "real AWS".to_owned());

    // The probe asks "is S3 reachable", NOT "does the bucket exist" — those
    // are different questions and conflating them is what made this skip
    // against a perfectly healthy LocalStack. A 404 means the service
    // answered; only a transport failure or a timeout means it is absent.
    //
    // head_bucket rather than list_buckets: the latter needs account-level
    // s3:ListAllMyBuckets, which a scoped credential may legitimately lack,
    // and a permissions error there would look like "no S3 available".
    let probe = client.head_bucket().bucket(bucket()).send();
    let reachable = match tokio::time::timeout(Duration::from_secs(15), probe).await {
        Ok(Ok(_)) => true,
        Ok(Err(error)) => {
            // A modeled service error is a RESPONSE: S3 is up and said no.
            // A dispatch/transport failure is not.
            let answered = error.raw_response().is_some();
            if !answered {
                eprintln!("S3 probe error: {error}");
                if let Some(source) = std::error::Error::source(&error) {
                    eprintln!("  caused by: {source}");
                }
            }
            answered
        }
        Err(_) => {
            unavailable(&format!("S3 at {target} (timed out)"))?;
            return None;
        }
    };

    if !reachable {
        unavailable(&format!("S3 at {target}"))?;
        return None;
    }

    // Reachable — now make sure the bucket is actually usable. A failure here
    // is a real failure, not a skip: S3 answered, so "cannot create the
    // bucket" is a problem with the run, not a missing dependency.
    if let Err(error) = create_bucket(&client).await {
        panic!("S3 at {target} is reachable but the bucket is unusable: {error}");
    }
    Some(client)
}

/// Creates the test bucket, treating already-exists as success.
///
/// `create_bucket` is region-sensitive: outside us-east-1 it requires an
/// explicit location constraint, and LocalStack mirrors that. REGION is
/// us-east-1, so none is sent.
async fn create_bucket(client: &aws_sdk_s3::Client) -> Result<()> {
    match client.create_bucket().bucket(bucket()).send().await {
        Ok(_) => Ok(()),
        Err(error) => {
            let already_mine = matches!(
                error.as_service_error(),
                Some(e) if e.is_bucket_already_owned_by_you() || e.is_bucket_already_exists()
            );
            if already_mine {
                Ok(())
            } else {
                Err(anyhow::anyhow!("creating bucket {}: {error}", bucket()))
            }
        }
    }
}

fn sample_event(mmr: i64) -> proto::Event {
    envelope(
        &origin(),
        EventIdentity {
            user_id: Some(mmr),
            is_guest: true,
            ..Default::default()
        },
        proto::event::Payload::GuestCreated(proto::GuestCreated {
            mmr,
            matchmaking_pool: "public".to_owned(),
        }),
    )
}

/// The whole durable path, with nothing faked.
#[tokio::test]
async fn events_travel_from_emitter_through_valkey_to_s3() -> Result<()> {
    let Some(mut redis) = redis_or_skip().await else {
        return Ok(());
    };
    let Some(s3) = s3_or_skip().await else {
        return Ok(());
    };

    let run = uuid::Uuid::now_v7();
    let stream_key = format!("snaketron:{{snaketron:analytics:e2e}}:events:{run}");
    let group = format!("e2e-exporter-{run}");

    // 1. Emit and serialize, exactly as the flusher does.
    let events: Vec<proto::Event> = (1..=5).map(sample_event).collect();
    let expected_ids: Vec<String> = events.iter().map(|e| e.event_id.clone()).collect();
    let payload = events
        .iter()
        .map(to_json_line)
        .collect::<Result<Vec<_>>>()?
        .join("\n");

    // 2. Through a real Valkey stream, bounded exactly as production is.
    let entry_id: String = redis
        .xadd_maxlen(
            &stream_key,
            redis::streams::StreamMaxlen::Approx(1_000),
            "*",
            &[("data", payload.as_str())],
        )
        .await?;
    assert!(!entry_id.is_empty(), "the stream must accept the batch");

    // 3. Drain through a real consumer group.
    let _: redis::RedisResult<()> = redis
        .xgroup_create_mkstream(&stream_key, &group, "0-0")
        .await;
    let read: redis::streams::StreamReadReply = redis
        .xread_options(
            &[&stream_key],
            &[">"],
            &redis::streams::StreamReadOptions::default()
                .group(&group, "e2e-consumer")
                .count(64),
        )
        .await?;
    let claimed: Vec<(String, String)> = read
        .keys
        .into_iter()
        .flat_map(|key| key.ids)
        .filter_map(|entry| entry.get::<String>("data").map(|d| (entry.id.clone(), d)))
        .collect();
    assert_eq!(claimed.len(), 1, "one entry carries the whole batch");

    // 4. Batch and write to real S3.
    let mut batcher = EventBatcher::new(default_limits());
    for (id, data) in &claimed {
        for line in data.split('\n').filter(|l| !l.is_empty()) {
            batcher.push(BufferedEvent {
                line: line.to_owned(),
                date: "2026-08-19".to_owned(),
                cursor: id.clone(),
            });
        }
    }
    let files = batcher.drain();
    assert_eq!(files.len(), 1, "one date bucket");

    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), bucket()));
    let target = ExportTarget {
        dataset: format!("game-events-{run}"),
        host: "use1-1".to_owned(),
    };
    let metrics = EmitterMetrics::default();
    let acked = write_batch(&store, &target, &files, &metrics).await?;
    assert_eq!(
        acked.as_deref(),
        Some(entry_id.as_str()),
        "ack the written cursor"
    );

    // 5. Ack only after the write, exactly as the exporter does.
    let ids: Vec<String> = claimed.iter().map(|(id, _)| id.clone()).collect();
    let acked_count: i64 = redis.xack(&stream_key, &group, &ids).await?;
    assert_eq!(acked_count, 1, "the entry retires from the pending list");

    // 6. Read the object back and prove the bytes are what was emitted.
    let listed = s3
        .list_objects_v2()
        .bucket(bucket())
        .prefix(format!("raw/game-events-{run}/"))
        .send()
        .await?;
    let objects = listed.contents();
    assert_eq!(objects.len(), 1, "exactly one object for one batch");

    let key = objects[0].key().expect("object key").to_owned();
    assert!(
        key.contains("/dt=2026-08-19/host=use1-1/"),
        "partitioned by event date and host, got {key}"
    );
    assert!(!key.contains("hh="), "no hour partition: {key}");
    assert!(key.ends_with(".json.gz"), "lowercase extension: {key}");

    let body = s3
        .get_object()
        .bucket(bucket())
        .key(&key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();

    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(&body[..]);
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;

    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        5,
        "every emitted event survived the round trip"
    );
    assert!(text.ends_with('\n'), "NDJSON must be newline terminated");

    let recovered: Vec<String> = lines
        .iter()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            value["event_id"].as_str().expect("event_id").to_owned()
        })
        .collect();
    assert_eq!(recovered, expected_ids, "ids and order must round-trip");

    // The payload survived intact, including the canonical string encoding of
    // 64-bit integers that the Athena DDL depends on.
    let first: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(first["event_name"], "guest_created");
    assert_eq!(first["guest_created"]["mmr"], "1");
    assert_eq!(first["identity"]["is_guest"], true);

    // Cleanup so repeated local runs stay clean.
    let _: redis::RedisResult<i64> = redis.del(&stream_key).await;
    Ok(())
}

/// The central idempotency claim: replaying a batch produces the same key,
/// the conditional write refuses it, and no duplicate object appears.
#[tokio::test]
async fn replaying_a_batch_writes_no_duplicate_object() -> Result<()> {
    let Some(s3) = s3_or_skip().await else {
        return Ok(());
    };

    let run = uuid::Uuid::now_v7();
    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), bucket()));
    let target = ExportTarget {
        dataset: format!("replay-{run}"),
        host: "use1-1".to_owned(),
    };
    let metrics = EmitterMetrics::default();

    let mut batcher = EventBatcher::new(default_limits());
    for index in 1..=3 {
        batcher.push(BufferedEvent {
            line: to_json_line(&sample_event(index))?,
            date: "2026-08-19".to_owned(),
            cursor: format!("{index}-0"),
        });
    }
    let files = batcher.drain();

    let first = write_batch(&store, &target, &files, &metrics).await?;
    let second = write_batch(&store, &target, &files, &metrics).await?;
    assert_eq!(first, second, "a replay must be safe to ack identically");

    let listed = s3
        .list_objects_v2()
        .bucket(bucket())
        .prefix(format!("raw/replay-{run}/"))
        .send()
        .await?;
    assert_eq!(
        listed.contents().len(),
        1,
        "the replay must not create a second object"
    );
    Ok(())
}

/// Proves the store actually reports 412 as success rather than an error —
/// the single line the whole additive-and-idempotent guarantee rests on.
#[tokio::test]
async fn a_conditional_write_to_an_existing_key_reports_already_present() -> Result<()> {
    let Some(s3) = s3_or_skip().await else {
        return Ok(());
    };

    let store = S3ObjectStore::new(s3, bucket());
    let key = format!("raw/conditional/{}.json.gz", uuid::Uuid::now_v7());

    assert_eq!(
        store.put_if_absent(&key, b"first".to_vec()).await?,
        PutOutcome::Written,
        "a fresh key must be written"
    );
    assert_eq!(
        store.put_if_absent(&key, b"second".to_vec()).await?,
        PutOutcome::AlreadyPresent,
        "an existing key must report AlreadyPresent, not an error"
    );
    Ok(())
}

/// The websocket path, which reaches S3 without touching Valkey.
///
/// Worth its own coverage because it shares almost nothing with the durable
/// path above: no stream, no consumer group, no elected exporter. Every task
/// buffers in memory and writes straight to S3, so the only thing standing
/// between a graceful stop and silent data loss is the final flush — and a
/// missing flush leaves no trace anywhere downstream to notice.
///
/// This is the only test in this binary that installs the process-global ws
/// sink. Adding a second installer would be a no-op against the `OnceLock` and
/// would quietly assert nothing.
#[tokio::test]
async fn websocket_events_reach_s3_when_the_exporter_stops_gracefully() -> Result<()> {
    let Some(s3) = s3_or_skip().await else {
        return Ok(());
    };

    let run = uuid::Uuid::now_v7();
    let dataset = format!("ws-events-{run}");
    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), bucket()));

    let cancel = tokio_util::sync::CancellationToken::new();
    let mut config = server::analytics::ws_exporter::WsExporterConfig::from_env(ExportTarget {
        dataset: dataset.clone(),
        host: "use1-1".to_owned(),
    });
    config.sample_rate = 1.0;
    // Pin the flush triggers out of reach so the ONLY thing that can move
    // these events to S3 is the shutdown flush. Without this the test would
    // still pass on a size or age trigger, and would stop covering R5.4 the
    // day a default changed.
    config.limits.max_buffer_events = 1_000_000;
    config.limits.max_buffer_bytes = 1 << 30;
    config.limits.max_batch_age = std::time::Duration::from_secs(3600);
    let (sink, task) = server::analytics::ws_exporter::create(store, config, cancel.clone());
    server::analytics::ws_sink::install(sink, origin(), 1.0);
    let driver = tokio::spawn(task);

    let connection = server::analytics::ws_sink::WsConnection::new(&format!("conn-{run}"));
    connection.bind_session(&format!("s-{run}"));
    for i in 0..4 {
        server::analytics::ws_sink::record_inbound(&connection, "Authenticate", 64 + i);
        server::analytics::ws_sink::record_outbound(&connection, "GameState", 512 + i);
    }

    cancel.cancel();
    driver.await.expect("the exporter task must join");

    use server::analytics::committer::SourceListing;
    let listing = server::analytics::source_listing::S3SourceListing::new(s3, bucket());
    let keys = listing
        .list_after(&dataset, None)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert!(
        !keys.is_empty(),
        "a graceful stop must flush the buffer to S3; nothing was written"
    );

    let ndjson = listing
        .fetch(&dataset, &keys[0])
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let lines: Vec<&str> = ndjson.lines().collect();
    assert_eq!(lines.len(), 8, "every recorded frame must be present");

    let mut directions: Vec<String> = Vec::new();
    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line)?;
        assert_eq!(value["event_name"], "websocket_message");
        assert_eq!(
            value["identity"]["session_id"],
            serde_json::Value::String(format!("s-{run}")),
            "every frame must carry the session it belongs to"
        );
        directions.push(
            value["websocket_message"]["direction"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    directions.sort();
    directions.dedup();
    assert_eq!(
        directions,
        vec!["in", "out"],
        "both directions must be recorded"
    );
    Ok(())
}
