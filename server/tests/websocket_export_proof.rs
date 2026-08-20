//! End-to-end proof of the WEBSOCKET export path against real AWS.
//!
//! This path is deliberately unlike the game-events one and needs its own
//! proof. It is non-durable (R5.3): there is no Valkey stream and no elected
//! exporter, so every task buffers in memory and writes straight to S3. Two
//! properties therefore have no analogue on the durable side and are what this
//! test exists to pin:
//!
//!   * a graceful stop MUST flush (R5.4) — a task that is cancelled but never
//!     joined drops its final batch silently, and no downstream check would
//!     ever notice the gap;
//!   * sampling is per connection, so a connection is wholly recorded or
//!     wholly absent — never half.
//!
//! Requires real AWS: LocalStack does not implement S3 Tables. Set
//! PROOF_TABLE_ARN and PROOF_RAW_BUCKET, and run the test BINARY directly —
//! `.cargo/config.toml` pins AWS_ENDPOINT_URL at LocalStack for every cargo
//! process here and the S3 Tables FileIO builds a malformed URL from it.

use std::sync::Arc;

use server::analytics::committer::{CommitTarget, IcebergCatalog, SourceListing, fold_once};
use server::analytics::exporter::ExportTarget;
use server::analytics::iceberg_catalog::{S3TablesConfig, S3TablesIcebergCatalog};
use server::analytics::object_store::{ObjectStore, S3ObjectStore};
use server::analytics::source_listing::S3SourceListing;
use server::analytics::ws_exporter::{WsExporterConfig, create};
use server::analytics::ws_sink::{Account, WsConnection};
use server::analytics::{event::EventOrigin, schema};
use tokio_util::sync::CancellationToken;

const REGION: &str = "us-east-1";

fn proof_target() -> Option<(String, String)> {
    match (
        std::env::var("PROOF_TABLE_ARN"),
        std::env::var("PROOF_RAW_BUCKET"),
    ) {
        (Ok(arn), Ok(bucket)) if !arn.is_empty() && !bucket.is_empty() => Some((arn, bucket)),
        _ => {
            assert!(
                std::env::var("SNAKETRON_ICEBERG_PROOF").is_err(),
                "SNAKETRON_ICEBERG_PROOF=1 but PROOF_TABLE_ARN / PROOF_RAW_BUCKET are unset"
            );
            eprintln!("SKIP: no real S3 Tables bucket configured");
            None
        }
    }
}

fn origin() -> EventOrigin {
    EventOrigin {
        environment: "proof".to_owned(),
        region: "use1".to_owned(),
        aws_region: REGION.to_owned(),
        instance_id: "1:ws-proof".to_owned(),
    }
}

/// The whole websocket path: record -> buffer -> flush on graceful stop -> S3
/// -> fold -> `websocket_events` rows read back out of Iceberg.
#[tokio::test]
async fn websocket_events_reach_iceberg_and_a_graceful_stop_loses_nothing() {
    let Some((arn, raw_bucket)) = proof_target() else {
        return;
    };
    let run = uuid::Uuid::now_v7();
    let dataset = format!("ws-proof-{run}");
    let table = format!("ws_proof_{}", run.simple());

    let s3 = S3ObjectStore::client_for_region(REGION).await;
    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), raw_bucket.clone()));

    let target = ExportTarget {
        dataset: dataset.clone(),
        host: "use1-7".to_owned(),
    };
    let cancel = CancellationToken::new();
    let mut config = WsExporterConfig::from_env(target);
    config.sample_rate = 1.0;
    // Pin the flush triggers out of reach so the ONLY thing that can move
    // these events to S3 is the shutdown flush. Without this the test would
    // still pass on a size or age trigger, and would stop covering R5.4 the
    // day a default changed.
    config.limits.max_buffer_events = 1_000_000;
    config.limits.max_buffer_bytes = 1 << 30;
    config.limits.max_batch_age = std::time::Duration::from_secs(3600);
    let (sink, task) = create(store.clone(), config, cancel.clone());

    server::analytics::ws_sink::install(sink, origin(), 1.0);
    let driver = tokio::spawn(task);

    let connection = WsConnection::new(&format!("conn-{run}"));
    connection.bind_session(&format!("s-{run}"));
    connection.set_game_id(Some(4242));

    // One frame from before authentication. It must come back with NO account
    // — absent, not zero — or a handshake would join to a real user.
    server::analytics::ws_sink::record_inbound(&connection, "Token", 42);

    connection.set_account(Some(Account {
        user_id: 987_654,
        is_guest: false,
        is_stress_test: false,
    }));
    for i in 0..5 {
        server::analytics::ws_sink::record_inbound(&connection, "Authenticate", 100 + i);
        server::analytics::ws_sink::record_outbound(&connection, "GameState", 900 + i);
    }

    // Nothing has been flushed yet. Cancelling and JOINING is the only thing
    // that can move these 10 events to S3.
    cancel.cancel();
    driver.await.expect("exporter task must join cleanly");

    let listing: Arc<dyn SourceListing> =
        Arc::new(S3SourceListing::new(s3.clone(), raw_bucket.clone()));
    let keys = listing.list_after(&dataset, None).await.expect("list");
    println!("OBJECTS_AFTER_GRACEFUL_STOP: {}", keys.len());
    assert!(
        !keys.is_empty(),
        "a graceful stop must flush its buffer; nothing reached S3"
    );
    for key in &keys {
        assert!(
            key.contains(&format!("raw/{dataset}/")),
            "ws objects must not share the game-events prefix: {key}"
        );
    }

    let ndjson = listing.fetch(&dataset, &keys[0]).await.expect("fetch");
    let first: serde_json::Value =
        serde_json::from_str(ndjson.lines().next().expect("a line")).expect("json");
    println!(
        "FIRST_EVENT name={} direction={} type={}",
        first["event_name"],
        first["websocket_message"]["direction"],
        first["websocket_message"]["message_type"]
    );
    assert_eq!(first["event_name"], "websocket_message");

    // Fold into Iceberg and read the rows back.
    let catalog: Arc<dyn IcebergCatalog> = Arc::new(
        S3TablesIcebergCatalog::connect(&S3TablesConfig {
            table_bucket_arn: arn.clone(),
            namespace: "snaketron".to_owned(),
            region: REGION.to_owned(),
            catalog_name: "proof".to_owned(),
        })
        .await
        .expect("connect"),
    );
    let pool = schema::descriptor_pool().expect("pool");
    let desc = schema::event_descriptor(&pool).expect("descriptor");
    catalog
        .ensure_table(&table, &schema::derive_columns(&desc))
        .await
        .expect("ensure_table");

    let commit_target = CommitTarget {
        dataset: dataset.clone(),
        table: table.clone(),
    };
    let folded = fold_once(&catalog, &listing, &commit_target, 1)
        .await
        .expect("fold");
    println!("FOLDED_OBJECTS: {folded}");
    assert!(folded > 0, "the fold must pick up the websocket objects");

    let loaded = S3TablesIcebergCatalog::connect(&S3TablesConfig {
        table_bucket_arn: arn,
        namespace: "snaketron".to_owned(),
        region: REGION.to_owned(),
        catalog_name: "proof".to_owned(),
    })
    .await
    .expect("reconnect")
    .load(&table)
    .await
    .expect("load");

    use arrow_array::Array;
    use futures_util::TryStreamExt;
    let batches: Vec<_> = loaded
        .scan()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("collect");

    let mut directions: Vec<String> = Vec::new();
    for batch in &batches {
        let ws = batch
            .column_by_name("websocket_message")
            .expect("websocket_message struct column")
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .expect("struct");
        let dir = ws
            .column_by_name("direction")
            .expect("direction field")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("string");
        for i in 0..dir.len() {
            if dir.is_valid(i) {
                directions.push(dir.value(i).to_owned());
            }
        }
    }
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    directions.sort();
    directions.dedup();
    println!("ICEBERG_ROWS: {rows} DIRECTIONS: {directions:?}");

    assert_eq!(rows, 11, "every recorded message must be in the table");

    // The whole point of this change: these rows join to an account.
    let mut accounts: Vec<Option<i64>> = Vec::new();
    for batch in &batches {
        let identity = batch
            .column_by_name("identity")
            .expect("identity struct column")
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .expect("struct");
        let user_id = identity
            .column_by_name("user_id")
            .expect("user_id field")
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("int64");
        for i in 0..user_id.len() {
            accounts.push(user_id.is_valid(i).then(|| user_id.value(i)));
        }
    }
    let named = accounts.iter().filter(|a| **a == Some(987_654)).count();
    let absent = accounts.iter().filter(|a| a.is_none()).count();
    println!("ACCOUNT_ROWS: {named}  NO_ACCOUNT_ROWS: {absent}");
    assert_eq!(
        named, 10,
        "every post-authentication frame must name the account: {accounts:?}"
    );
    assert_eq!(
        absent, 1,
        "the pre-authentication frame must carry no account at all: {accounts:?}"
    );
    assert!(
        directions.iter().any(|d| d == "in") && directions.iter().any(|d| d == "out"),
        "both directions must survive the round trip: {directions:?}"
    );
}
