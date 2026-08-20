//! Proof that the connection lifecycle events survive the fold into Iceberg.
//!
//! `connection_started` and `connection_ended` add two new struct columns to a
//! schema derived from the protos. Their unit tests prove the events are
//! EMITTED correctly; nothing there exercises schema derivation, the Arrow
//! projection of a newly added nested struct, or the Parquet round trip. A new
//! column that fails to derive would not show up as a broken test — it would
//! show up as a column that is silently always null.
//!
//! Requires real AWS: LocalStack does not implement S3 Tables. Set
//! PROOF_TABLE_ARN and PROOF_RAW_BUCKET, and run the test BINARY directly —
//! `.cargo/config.toml` pins AWS_ENDPOINT_URL at LocalStack for every cargo
//! process here and the S3 Tables FileIO builds a malformed URL from it.

use std::sync::Arc;

use server::analytics::batch::{BufferedEvent, EventBatcher};
use server::analytics::committer::{CommitTarget, IcebergCatalog, SourceListing, fold_once};
use server::analytics::emitter::EmitterMetrics;
use server::analytics::event::{EventOrigin, to_json_line};
use server::analytics::exporter::{ExportTarget, default_limits, write_batch};
use server::analytics::iceberg_catalog::{S3TablesConfig, S3TablesIcebergCatalog};
use server::analytics::object_store::{ObjectStore, S3ObjectStore};
use server::analytics::schema;
use server::analytics::source_listing::S3SourceListing;

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
        instance_id: "1:conn-proof".to_owned(),
    }
}

#[tokio::test]
async fn a_connection_pair_survives_the_fold_and_stays_joinable() {
    let Some((arn, raw_bucket)) = proof_target() else {
        return;
    };
    let run = uuid::Uuid::now_v7();
    let dataset = format!("conn-proof-{run}");
    let table = format!("conn_proof_{}", run.simple());
    let connection_id = format!("ws-{run}");

    // Driven through the REAL emission path — install the sink, drive a
    // connection, drain what the emitter actually produced. Calling a
    // projection directly would prove the projection folds, not that what the
    // server emits folds.
    let (emitter, mut events) =
        server::analytics::AnalyticsEmitter::new(server::analytics::EmitterConfig::default());
    server::analytics::sink::install(emitter, origin());

    let connection = server::analytics::ws_sink::WsConnection::new(&connection_id);
    server::analytics::sink::record_connection_started(&connection);
    server::analytics::sink::record_connection_closed(&connection);

    let mut emitted = Vec::new();
    while let Ok(event) = events.try_recv() {
        emitted.push(event);
    }
    assert_eq!(
        emitted.len(),
        2,
        "a connection with no session emits exactly the accept and the close"
    );

    let s3 = S3ObjectStore::client_for_region(REGION).await;
    let store: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(s3.clone(), raw_bucket.clone()));
    let metrics = EmitterMetrics::default();
    let mut batcher = EventBatcher::new(default_limits());
    for (i, event) in emitted.into_iter().enumerate() {
        batcher.push(BufferedEvent {
            line: to_json_line(&event).expect("serialize"),
            date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            cursor: format!("{}-0", 1_000_000_000_000u64 + i as u64),
        });
    }
    let files = batcher.drain();
    write_batch(
        &store,
        &ExportTarget {
            dataset: dataset.clone(),
            host: "use1-1".to_owned(),
        },
        &files,
        &metrics,
    )
    .await
    .expect("write to S3");

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
    let columns = schema::derive_columns(&desc);
    assert!(
        columns
            .iter()
            .any(|c| c.name.starts_with("connection_started")),
        "the derived schema must contain the new struct"
    );
    catalog
        .ensure_table(&table, &columns)
        .await
        .expect("ensure_table");

    let listing: Arc<dyn SourceListing> =
        Arc::new(S3SourceListing::new(s3.clone(), raw_bucket.clone()));
    let folded = fold_once(
        &catalog,
        &listing,
        &CommitTarget {
            dataset: dataset.clone(),
            table: table.clone(),
        },
        1,
    )
    .await
    .expect("fold");
    println!("FOLDED_OBJECTS: {folded}");

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

    // Pull the connection id out of BOTH structs. The whole point of the id is
    // that an accept and its close name the same socket.
    let mut ids: Vec<(String, String)> = Vec::new();
    for batch in &batches {
        let names = batch
            .column_by_name("event_name")
            .expect("event_name")
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("string");
        for column in ["connection_started", "connection_ended"] {
            let Some(structure) = batch.column_by_name(column) else {
                panic!("{column} column missing from the folded table");
            };
            let structure = structure
                .as_any()
                .downcast_ref::<arrow_array::StructArray>()
                .expect("struct");
            let id = structure
                .column_by_name("connection_id")
                .expect("connection_id field")
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("string");
            for i in 0..id.len() {
                if id.is_valid(i) {
                    ids.push((names.value(i).to_owned(), id.value(i).to_owned()));
                }
            }
        }
    }
    ids.sort();
    println!("PAIRED_ROWS: {ids:?}");

    assert_eq!(
        ids,
        vec![
            ("connection_ended".to_owned(), connection_id.clone()),
            ("connection_started".to_owned(), connection_id.clone()),
        ],
        "both halves must land, carrying the same connection id"
    );
}
