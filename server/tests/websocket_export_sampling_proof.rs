//! Proof that websocket sampling includes and excludes whole connections.
//!
//! Its own binary on purpose. The process-global sink is a `OnceLock`, so a
//! second `install` is silently ignored — a sampling test sharing a binary
//! with a test that installs a different rate would assert against whichever
//! rate won the race, which is to say it would assert nothing.
//!
//! Touches no S3 Tables, so LocalStack is enough and this runs in CI. The
//! endpoint is resolved the same way `analytics_pipeline_e2e` resolves it —
//! LocalStack needs path-style addressing, and a virtual-host request against
//! it comes back as `ListAllMyBucketsResult` and fails to parse.

use std::sync::Arc;

use server::analytics::committer::SourceListing;
use server::analytics::event::EventOrigin;
use server::analytics::exporter::ExportTarget;
use server::analytics::object_store::{ObjectStore, S3ObjectStore};
use server::analytics::source_listing::S3SourceListing;
use server::analytics::ws_exporter::{WsExporterConfig, create, is_sampled};
use server::analytics::ws_sink::WsConnection;
use tokio_util::sync::CancellationToken;

const REGION: &str = "us-east-1";
const RATE: f64 = 0.5;

fn bucket() -> String {
    std::env::var("SNAKETRON_E2E_BUCKET").unwrap_or_else(|_| "snaketron-analytics-e2e".to_owned())
}

/// LocalStack unless told otherwise, never real AWS by default — the same
/// floor `analytics_pipeline_e2e` uses, for the same reason: a test that
/// writes to a real bucket by default is one stray environment away from
/// writing to a real bucket someone cares about.
fn endpoint() -> Option<String> {
    match std::env::var("SNAKETRON_E2E_S3_ENDPOINT") {
        Ok(value) if value == "aws" => None,
        Ok(value) if !value.is_empty() => Some(value),
        _ => Some(
            std::env::var("AWS_ENDPOINT_URL")
                .unwrap_or_else(|_| "http://localhost:4566".to_owned()),
        ),
    }
}

/// Whether S3 is reachable. Skips when it is not, and hard-fails under
/// `SNAKETRON_E2E=1` so an intended run cannot pass by doing nothing.
async fn s3_or_skip() -> Option<aws_sdk_s3::Client> {
    if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
        // SAFETY: set before any client is built, and this binary runs one test.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        }
    }
    let client = S3ObjectStore::client_for(REGION, endpoint().as_deref()).await;
    let reachable = match client.head_bucket().bucket(bucket()).send().await {
        Ok(_) => true,
        // A modeled service error is a RESPONSE: S3 is up and said no.
        Err(error) => error.raw_response().is_some(),
    };
    if reachable {
        let _ = client.create_bucket().bucket(bucket()).send().await;
        return Some(client);
    }
    assert!(
        std::env::var("SNAKETRON_E2E").is_err(),
        "SNAKETRON_E2E=1 but S3 is unavailable; run ./test-deps.sh"
    );
    eprintln!("SKIP: no S3 available");
    None
}

/// Two connection keys that land on opposite sides of the same rate.
///
/// Found by probing rather than hardcoded: the hash is an implementation
/// detail, and a literal pair would silently stop testing anything the day it
/// changes — both keys would land on the same side and the test would still
/// pass.
fn keys_either_side_of(rate: f64) -> (String, String) {
    let mut sampled = None;
    let mut excluded = None;
    for i in 0..10_000 {
        let key = format!("conn-probe-{i}");
        if is_sampled(&key, rate) {
            sampled.get_or_insert(key);
        } else {
            excluded.get_or_insert(key);
        }
        if sampled.is_some() && excluded.is_some() {
            break;
        }
    }
    (
        sampled.expect("some key must fall inside the sample"),
        excluded.expect("some key must fall outside the sample"),
    )
}

/// Both halves in ONE run, through ONE exporter.
///
/// The negative assertion alone would pass if recording were broken for every
/// connection, or if the exporter never wrote at all. Pairing it with a
/// connection that must be recorded turns "nothing was written" from a result
/// that could mean anything into one that can only mean exclusion.
#[tokio::test]
async fn sampling_includes_and_excludes_whole_connections() {
    let Some(s3) = s3_or_skip().await else {
        return;
    };
    let run = uuid::Uuid::now_v7();
    let dataset = format!("ws-sampling-{run}");
    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), bucket()));

    let cancel = CancellationToken::new();
    let mut config = WsExporterConfig::from_env(ExportTarget {
        dataset: dataset.clone(),
        host: "use1-9".to_owned(),
    });
    config.sample_rate = RATE;
    // Nothing may flush before the cancel, so what lands in S3 is exactly what
    // the shutdown flush chose to write and not what a size or age trigger
    // happened to push out first.
    config.limits.max_buffer_events = 1_000_000;
    config.limits.max_buffer_bytes = 1 << 30;
    config.limits.max_batch_age = std::time::Duration::from_secs(3600);
    let (sink, task) = create(store, config, cancel.clone());
    server::analytics::ws_sink::install(
        sink,
        EventOrigin {
            environment: "proof".to_owned(),
            region: "use1".to_owned(),
            aws_region: REGION.to_owned(),
            instance_id: "1:ws-sampling".to_owned(),
        },
        RATE,
    );
    let driver = tokio::spawn(task);

    let (sampled_key, excluded_key) = keys_either_side_of(RATE);
    let sampled_session = format!("s-in-{run}");
    let excluded_session = format!("s-out-{run}");

    let inside = WsConnection::new(&sampled_key);
    inside.bind_session(&sampled_session);
    let outside = WsConnection::new(&excluded_key);
    outside.bind_session(&excluded_session);

    for i in 0..6 {
        server::analytics::ws_sink::record_inbound(&inside, "Authenticate", 10 + i);
        server::analytics::ws_sink::record_outbound(&inside, "GameState", 20 + i);
        server::analytics::ws_sink::record_inbound(&outside, "Authenticate", 30 + i);
        server::analytics::ws_sink::record_outbound(&outside, "GameState", 40 + i);
    }

    cancel.cancel();
    driver.await.expect("the exporter task must join");

    let listing = S3SourceListing::new(s3, bucket());
    let keys = listing.list_after(&dataset, None).await.expect("list");
    let mut sessions: Vec<String> = Vec::new();
    for key in &keys {
        for line in listing.fetch(&dataset, key).await.expect("fetch").lines() {
            let value: serde_json::Value = serde_json::from_str(line).expect("json");
            if let Some(session) = value["identity"]["session_id"].as_str() {
                sessions.push(session.to_owned());
            }
        }
    }
    let inside_rows = sessions.iter().filter(|s| **s == sampled_session).count();
    let outside_rows = sessions.iter().filter(|s| **s == excluded_session).count();
    println!("SAMPLED_ROWS: {inside_rows}  EXCLUDED_ROWS: {outside_rows}");

    // Positive control: without this, "nothing written" proves nothing.
    assert_eq!(
        inside_rows, 12,
        "a sampled connection must be recorded completely"
    );
    assert_eq!(
        outside_rows, 0,
        "an excluded connection must produce no rows at all"
    );
}
