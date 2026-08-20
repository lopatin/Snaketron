//! Proof that a fold spanning two regions loses nothing, run against real
//! AWS S3 and real S3 Tables.
//!
//! This exists because a single lexicographic resume mark is wrong for these
//! keys and the failure is invisible in a one-shot test. Objects are laid out
//! `raw/{dataset}/dt={date}/host={region}-{server_id}/...`, and `euw1-*` sorts
//! BELOW `use1-*`. Fold once with only a US object present and the mark lands
//! on a US key; every EU object written afterwards for that day sorts beneath
//! it. A test that folds once and checks its rows passes happily while an
//! entire region is dropped, so this test folds TWICE with a write in between.
//!
//! Requires real AWS. Set PROOF_TABLE_ARN and PROOF_RAW_BUCKET. Run the test
//! BINARY directly rather than through cargo: `.cargo/config.toml` pins
//! AWS_ENDPOINT_URL at LocalStack for every cargo process in this repo, and
//! the S3 Tables FileIO builds a malformed URL from it.

use std::sync::Arc;

use server::analytics::committer::{CommitTarget, IcebergCatalog, SourceListing, fold_once};
use server::analytics::event::{EventIdentity, EventOrigin, envelope, to_json_line};
use server::analytics::iceberg_catalog::{S3TablesConfig, S3TablesIcebergCatalog};
use server::analytics::object_store::{ObjectStore, S3ObjectStore, compress, object_key};
use server::analytics::source_listing::S3SourceListing;
use server::analytics::{proto, schema};

const REGION: &str = "us-east-1";
/// Today, in UTC. Pinning a literal date would make this test start failing
/// on a date unrelated to the bug: the committer only lists within a retention
/// window, so a fixed day eventually falls outside it and the EU rows go
/// missing for the wrong reason.
fn day() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

/// Real S3 Tables has no local emulator — LocalStack does not implement it —
/// so unlike the raw-tier e2e test this one cannot run in CI. It skips unless
/// pointed at a real table bucket, and `SNAKETRON_ICEBERG_PROOF=1` turns a
/// missing one into a hard failure so an intended proof run cannot pass by
/// doing nothing.
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
            eprintln!(
                "SKIP: no real S3 Tables bucket configured. \
                 Set PROOF_TABLE_ARN and PROOF_RAW_BUCKET, and run the test BINARY \
                 directly — .cargo/config.toml pins AWS_ENDPOINT_URL at LocalStack for \
                 every cargo process here and the S3 Tables FileIO builds a malformed \
                 URL from it."
            );
            None
        }
    }
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this proof"))
}

fn line(mmr: i64, region: &str) -> String {
    let origin = EventOrigin {
        environment: "proof".to_owned(),
        region: region.to_owned(),
        aws_region: REGION.to_owned(),
        instance_id: format!("{region}:proof"),
    };
    let event = envelope(
        &origin,
        EventIdentity {
            user_id: Some(mmr),
            ..Default::default()
        },
        proto::event::Payload::GuestCreated(proto::GuestCreated {
            mmr,
            matchmaking_pool: region.to_owned(),
        }),
    );
    to_json_line(&event).expect("serialize")
}

/// Writes one raw object exactly as the exporter would.
async fn put_raw(
    store: &S3ObjectStore,
    dataset: &str,
    host: &str,
    cursor: &str,
    body: &str,
) -> String {
    let key = object_key(dataset, &day(), host, cursor, cursor, "hash");
    let compressed = compress(body).expect("gzip");
    store.put_if_absent(&key, compressed).await.expect("put");
    key
}

#[tokio::test]
async fn a_fold_spanning_two_regions_loses_neither() {
    let Some((arn, raw_bucket)) = proof_target() else {
        return;
    };
    let run = uuid::Uuid::now_v7();
    // Overridable so the same objects can be folded a second time under a
    // different credential set — the committer's S3 reads need verifying
    // against the task role's policy, not against admin keys.
    let dataset = std::env::var("PROOF_DATASET").unwrap_or_else(|_| format!("two-region-{run}"));
    let table = std::env::var("PROOF_TABLE_NAME")
        .unwrap_or_else(|_| format!("two_region_{}", run.simple()));
    let write = std::env::var("PROOF_SKIP_WRITES").is_err();

    let s3 = S3ObjectStore::client_for_region(REGION).await;
    let store = S3ObjectStore::new(s3.clone(), raw_bucket.clone());
    let listing: Arc<dyn SourceListing> =
        Arc::new(S3SourceListing::new(s3.clone(), raw_bucket.clone()));

    let catalog: Arc<dyn IcebergCatalog> = Arc::new(
        S3TablesIcebergCatalog::connect(&S3TablesConfig {
            table_bucket_arn: arn,
            namespace: "snaketron".to_owned(),
            region: REGION.to_owned(),
            catalog_name: "proof".to_owned(),
        })
        .await
        .expect("connect to S3 Tables"),
    );

    let pool = schema::descriptor_pool().expect("pool");
    let event = schema::event_descriptor(&pool).expect("descriptor");
    catalog
        .ensure_table(&table, &schema::derive_columns(&event))
        .await
        .expect("ensure_table");

    let target = CommitTarget {
        dataset: dataset.clone(),
        table: table.clone(),
    };

    // 1. Only the US host has written. Folding sets the mark to a use1 key.
    if write {
        put_raw(
            &store,
            &dataset,
            "use1-3",
            "1000000000000-0",
            &line(1, "use1"),
        )
        .await;
    }
    let folded_us = fold_once(&catalog, &listing, &target, 1)
        .await
        .expect("first fold");
    println!("FOLD_1 objects={folded_us}");

    // 2. The EU host writes afterwards. Its key sorts BELOW the US key that is
    //    now the mark — this is the whole point of the test.
    let eu_key = if write {
        put_raw(
            &store,
            &dataset,
            "euw1-5",
            "2000000000000-0",
            &line(2, "euw1"),
        )
        .await
    } else {
        object_key(
            &dataset,
            &day(),
            "euw1-5",
            "2000000000000-0",
            "2000000000000-0",
            "hash",
        )
    };
    let us_key = object_key(
        &dataset,
        &day(),
        "use1-3",
        "1000000000000-0",
        "1000000000000-0",
        "hash",
    );
    assert!(
        eu_key < us_key,
        "the premise of this test is that the EU key sorts below the US key:\n  eu={eu_key}\n  us={us_key}"
    );

    let folded_eu = fold_once(&catalog, &listing, &target, 1)
        .await
        .expect("second fold");
    println!("FOLD_2 objects={folded_eu}");

    // 3. Both regions must be in the table.
    let loaded = S3TablesIcebergCatalog::connect(&S3TablesConfig {
        table_bucket_arn: required("PROOF_TABLE_ARN"),
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

    let mut regions: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch.column_by_name("region").expect("region column");
        let arr = col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("region is a string column");
        for i in 0..arr.len() {
            regions.push(arr.value(i).to_owned());
        }
    }
    regions.sort();
    println!("REGIONS_IN_TABLE: {regions:?}");

    assert!(
        regions.iter().any(|r| r == "use1"),
        "US rows missing: {regions:?}"
    );
    assert!(
        regions.iter().any(|r| r == "euw1"),
        "EU ROWS MISSING — an entire region was silently dropped by the resume mark: {regions:?}"
    );
}
