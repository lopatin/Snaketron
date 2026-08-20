//! The Iceberg write path, exercised end to end against a real warehouse.
//!
//! Everything here runs against `MemoryCatalog` backed by `LocalFsStorage` in a
//! `TempDir`: real Parquet files on a real filesystem, real manifests, real
//! snapshots — with no network and no AWS, so these run in CI by default rather
//! than behind `#[ignore]`.
//!
//! What that buys over the unit tests: the unit tests pin what the code
//! *intends* (this JSON becomes that Arrow array, this column diff becomes that
//! plan). These pin what actually happens when the bytes go to disk and come
//! back — that the batch survives the Parquet writer's schema check, that the
//! partition transform resolves, that `fast_append` produces a readable
//! snapshot, and that a column added after a write reads as null against the
//! files that predate it. Every one of those has failed silently at some point
//! in a proto→Iceberg pipeline; none of them is provable without a round trip.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{FormatVersion, Transform};
use snaketron_service_api::ServiceError;
use tempfile::TempDir;

use super::committer::{CommitOutcome, CommitTarget, IcebergCatalog, SourceListing, fold_once};
use super::iceberg_catalog::S3TablesIcebergCatalog;
use super::schema::{DerivedColumn, derive_columns, descriptor_pool, event_descriptor};

const NAMESPACE: &str = "analytics";
const TABLE: &str = "game_events";

/// 2025-08-19T00:00:00.123Z.
const DAY_ONE_MS: i64 = 1_755_561_600_123;
/// 2025-08-20T00:00:00.456Z.
const DAY_TWO_MS: i64 = 1_755_648_000_456;

/// A warehouse on disk plus a catalog pointed at it.
///
/// `LocalFsStorageFactory` rather than the builder's default in-memory
/// storage: the point of these tests is that the Parquet bytes are real, and
/// an in-memory store would not exercise the file IO the production path uses.
async fn warehouse() -> (TempDir, S3TablesIcebergCatalog) {
    let dir = TempDir::new().expect("temp warehouse");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "local",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                format!("file://{}", dir.path().display()),
            )]),
        )
        .await
        .expect("memory catalog");

    (
        dir,
        S3TablesIcebergCatalog::new(Arc::new(catalog), NAMESPACE),
    )
}

/// The real proto-derived schema, not a fixture. A hand-written schema would
/// pass these tests while the shipped one failed.
fn derived_columns() -> Vec<DerivedColumn> {
    let pool = descriptor_pool().expect("descriptors");
    derive_columns(&event_descriptor(&pool).expect("Event descriptor"))
}

/// One canonical proto3 JSON line, with 64-bit integers QUOTED exactly as
/// `event::to_json_line` writes them.
fn game_completed_line(event_id: &str, occurred_at_ms: i64, region: &str, game_id: i64) -> String {
    format!(
        r#"{{"event_id":"{event_id}","event_name":"game_completed","event_version":"1","occurred_at_ms":"{occurred_at_ms}","environment":"test","region":"{region}","aws_region":"us-east-1","instance_id":"1:boot","game_completed":{{"game_id":"{game_id}","game_type":"ffa","queue_mode":"ranked","duration_ms":"90000","player_count":"4","completed_by_inactivity":false,"winner_user_id":"7","end_reason":"victory"}}}}"#
    )
}

/// A `user_login` line. The proto message is EMPTY, so the column is a boolean
/// presence flag and the JSON value is `{}`.
fn user_login_line(event_id: &str, occurred_at_ms: i64, region: &str, user_id: i64) -> String {
    format!(
        r#"{{"event_id":"{event_id}","event_name":"user_login","event_version":"1","occurred_at_ms":"{occurred_at_ms}","environment":"test","region":"{region}","aws_region":"us-east-1","instance_id":"1:boot","identity":{{"user_id":"{user_id}","anon_id":"anon","session_id":"sess","is_guest":false,"is_stress_test":false}},"user_login":{{}}}}"#
    )
}

/// Reads the whole table back through a real Iceberg scan.
async fn scan(catalog: &S3TablesIcebergCatalog, table: &str) -> Vec<RecordBatch> {
    let loaded = catalog.load(table).await.expect("load table");
    loaded
        .scan()
        .build()
        .expect("scan plan")
        .to_arrow()
        .await
        .expect("arrow stream")
        .try_collect()
        .await
        .expect("collect batches")
}

/// Every row's value for one top-level string column, keyed by `event_id`, so
/// assertions do not depend on file or row order.
fn strings_by_event_id(batches: &[RecordBatch], column: &str) -> HashMap<String, Option<String>> {
    let mut out = HashMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("event_id")
            .expect("event_id column")
            .as_string::<i32>();
        let values = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            out.insert(
                ids.value(row).to_owned(),
                values.is_valid(row).then(|| values.value(row).to_owned()),
            );
        }
    }
    out
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Finds the row index of an `event_id` within a batch.
fn find(batches: &[RecordBatch], event_id: &str) -> (usize, usize) {
    for (index, batch) in batches.iter().enumerate() {
        let ids = batch
            .column_by_name("event_id")
            .expect("event_id column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            if ids.value(row) == event_id {
                return (index, row);
            }
        }
    }
    panic!("event {event_id} is not in the table");
}

// ---------------------------------------------------------------------------
// Table creation
// ---------------------------------------------------------------------------

/// The layout is a ONE-WAY DOOR: iceberg-rust has no partition-spec evolution,
/// so a wrong spec here can only be fixed by rebuilding the table. This test is
/// the barrier against changing it by accident.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_table_is_v2_partitioned_by_day_and_region_and_sorted() {
    let (_dir, catalog) = warehouse().await;
    catalog
        .ensure_table(TABLE, &derived_columns())
        .await
        .expect("create table");

    let table = catalog.load(TABLE).await.expect("load");
    let metadata = table.metadata();

    // Athena implements Iceberg spec 1.4.2 and reads v2 only.
    assert_eq!(metadata.format_version(), FormatVersion::V2);

    let spec = metadata.default_partition_spec();
    let fields: Vec<(&str, &Transform)> = spec
        .fields()
        .iter()
        .map(|field| (field.name.as_str(), &field.transform))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("occurred_at_day", &Transform::Day),
            ("region", &Transform::Identity),
        ],
        "R8.10: hidden partitioning by day(occurred_at) and region"
    );

    let schema = metadata.current_schema();
    let order = metadata
        .sort_orders_iter()
        .find(|order| order.order_id == metadata.default_sort_order_id())
        .expect("default sort order");
    let sorted_by: Vec<&str> = order
        .fields
        .iter()
        .map(|field| {
            schema
                .field_by_id(field.source_id)
                .expect("sort source column")
                .name
                .as_str()
        })
        .collect();
    assert_eq!(sorted_by, vec!["event_name", "occurred_at_ms"]);
}

/// The partition column is not a proto field, so nothing else in the pipeline
/// would notice its absence until the spec failed to bind.
#[tokio::test(flavor = "multi_thread")]
async fn the_created_table_carries_every_proto_column_plus_the_derived_timestamp() {
    let (_dir, catalog) = warehouse().await;
    let columns = derived_columns();
    catalog.ensure_table(TABLE, &columns).await.expect("create");

    let present = catalog.current_columns(TABLE).await.expect("columns");
    for column in &columns {
        assert!(
            present.contains(&column.name),
            "{} is missing from the created table",
            column.name
        );
    }
    assert!(present.contains(&"occurred_at".to_owned()));
}

/// Creation must be idempotent: every fold pass calls it, and a second create
/// that failed would take the committer down on its second tick.
#[tokio::test(flavor = "multi_thread")]
async fn ensuring_an_existing_table_is_a_no_op() {
    let (_dir, catalog) = warehouse().await;
    let columns = derived_columns();
    catalog.ensure_table(TABLE, &columns).await.expect("create");
    let first = catalog.load(TABLE).await.expect("load").metadata().uuid();

    catalog
        .ensure_table(TABLE, &columns)
        .await
        .expect("re-ensure");
    let second = catalog.load(TABLE).await.expect("load").metadata().uuid();

    assert_eq!(first, second, "the table must not be recreated");
}

// ---------------------------------------------------------------------------
// The round trip
// ---------------------------------------------------------------------------

/// The whole point of the exercise: NDJSON in, Parquet on disk, rows back out
/// with their values intact.
#[tokio::test(flavor = "multi_thread")]
async fn ndjson_rows_round_trip_through_parquet_and_back() {
    let (_dir, catalog) = warehouse().await;
    catalog
        .ensure_table(TABLE, &derived_columns())
        .await
        .expect("create");

    let ndjson = format!(
        "{}\n{}\n",
        game_completed_line("evt-a", DAY_ONE_MS, "use1", 42),
        user_login_line("evt-b", DAY_TWO_MS, "euw1", 7),
    );

    assert_eq!(
        catalog
            .commit(TABLE, "raw/game-events/dt=2025-08-19/a.json.gz", 1, &ndjson)
            .await
            .expect("commit"),
        CommitOutcome::Committed
    );

    let batches = scan(&catalog, TABLE).await;
    assert_eq!(row_count(&batches), 2);

    // A quoted 64-bit integer nested inside a oneof arm. This is the value that
    // a naive `as_i64()` reader turns into a silent null.
    let (batch_index, row) = find(&batches, "evt-a");
    let batch = &batches[batch_index];
    let game_completed = batch
        .column_by_name("game_completed")
        .expect("game_completed column")
        .as_struct();
    assert_eq!(
        game_completed
            .column_by_name("game_id")
            .expect("game_id")
            .as_primitive::<Int64Type>()
            .value(row),
        42
    );
    assert_eq!(
        game_completed
            .column_by_name("end_reason")
            .expect("end_reason")
            .as_string::<i32>()
            .value(row),
        "victory"
    );

    // The derived partition timestamp, in microseconds, with the source
    // millisecond precision intact.
    assert_eq!(
        batch
            .column_by_name("occurred_at")
            .expect("occurred_at")
            .as_primitive::<TimestampMicrosecondType>()
            .value(row),
        DAY_ONE_MS * 1_000
    );

    // The empty proto message became a boolean presence flag, and the row that
    // is not a login carries null rather than false.
    let (login_batch, login_row) = find(&batches, "evt-b");
    let logins = batches[login_batch]
        .column_by_name("user_login")
        .expect("user_login")
        .as_boolean();
    assert!(logins.value(login_row), "the login row is flagged");
    assert!(
        batch
            .column_by_name("user_login")
            .expect("user_login")
            .as_boolean()
            .is_null(row),
        "a non-login row must be null, not false"
    );

    // Nested identity survived as a struct, not as flattened columns.
    let identity = batches[login_batch]
        .column_by_name("identity")
        .expect("identity")
        .as_struct();
    assert_eq!(
        identity
            .column_by_name("user_id")
            .expect("user_id")
            .as_primitive::<Int64Type>()
            .value(login_row),
        7
    );

    let regions = strings_by_event_id(&batches, "region");
    assert_eq!(regions["evt-a"], Some("use1".to_owned()));
    assert_eq!(regions["evt-b"], Some("euw1".to_owned()));
}

/// Hidden partitioning is only real if the files actually land in partition
/// directories; a spec that binds but never routes would look identical in
/// metadata and destroy every query's pruning.
#[tokio::test(flavor = "multi_thread")]
async fn rows_fan_out_into_one_file_per_day_and_region() {
    let (_dir, catalog) = warehouse().await;
    catalog
        .ensure_table(TABLE, &derived_columns())
        .await
        .expect("create");

    let ndjson = format!(
        "{}\n{}\n{}\n",
        game_completed_line("evt-a", DAY_ONE_MS, "use1", 1),
        game_completed_line("evt-b", DAY_ONE_MS, "use1", 2),
        game_completed_line("evt-c", DAY_TWO_MS, "euw1", 3),
    );
    catalog
        .commit(TABLE, "a.json.gz", 1, &ndjson)
        .await
        .expect("commit");

    let table = catalog.load(TABLE).await.expect("load");
    let paths: Vec<String> = table
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect::<Vec<_>>()
        .await
        .expect("files")
        .into_iter()
        .map(|task| task.data_file_path)
        .collect();

    assert_eq!(paths.len(), 2, "two partitions, two files: {paths:?}");
    let day_one = chrono::DateTime::from_timestamp_millis(DAY_ONE_MS)
        .expect("day one")
        .format("%Y-%m-%d")
        .to_string();
    let day_two = chrono::DateTime::from_timestamp_millis(DAY_TWO_MS)
        .expect("day two")
        .format("%Y-%m-%d")
        .to_string();
    assert!(
        paths
            .iter()
            .any(|path| path.contains(&format!("occurred_at_day={day_one}/region=use1"))),
        "no use1 partition in {paths:?}"
    );
    assert!(
        paths
            .iter()
            .any(|path| path.contains(&format!("occurred_at_day={day_two}/region=euw1"))),
        "no euw1 partition in {paths:?}"
    );
}

/// The evolution property, proven rather than argued: a column added after data
/// was written reads as NULL against the older files, and carries values in the
/// newer ones. This is the guarantee that lets the schema be additive-only.
#[tokio::test(flavor = "multi_thread")]
async fn a_column_added_after_a_write_reads_null_against_the_older_rows() {
    let (_dir, catalog) = warehouse().await;

    // A table created by an older binary: everything except the identity
    // subtree.
    let all = derived_columns();
    let (identity, older): (Vec<DerivedColumn>, Vec<DerivedColumn>) = all
        .into_iter()
        .partition(|column| column.name == "identity" || column.name.starts_with("identity."));
    assert!(!identity.is_empty(), "the protos must still have identity");

    catalog.ensure_table(TABLE, &older).await.expect("create");
    assert!(
        !catalog
            .current_columns(TABLE)
            .await
            .expect("columns")
            .contains(&"identity".to_owned())
    );

    // Data written before the column existed.
    catalog
        .commit(
            TABLE,
            "a.json.gz",
            1,
            &user_login_line("evt-old", DAY_ONE_MS, "use1", 111),
        )
        .await
        .expect("first commit");

    // The newer binary adds the column...
    catalog
        .add_columns(TABLE, &identity)
        .await
        .expect("add identity");
    assert!(
        catalog
            .current_columns(TABLE)
            .await
            .expect("columns")
            .contains(&"identity.user_id".to_owned())
    );

    // ...and writes through it.
    catalog
        .commit(
            TABLE,
            "b.json.gz",
            1,
            &user_login_line("evt-new", DAY_ONE_MS, "use1", 222),
        )
        .await
        .expect("second commit");

    let batches = scan(&catalog, TABLE).await;
    assert_eq!(row_count(&batches), 2);

    let (old_batch, old_row) = find(&batches, "evt-old");
    let old_identity = batches[old_batch]
        .column_by_name("identity")
        .expect("identity column")
        .as_struct();
    assert!(
        old_identity.is_null(old_row),
        "a file written before the column must project as null, not fail to read"
    );

    let (new_batch, new_row) = find(&batches, "evt-new");
    let new_identity = batches[new_batch]
        .column_by_name("identity")
        .expect("identity column")
        .as_struct();
    assert_eq!(
        new_identity
            .column_by_name("user_id")
            .expect("user_id")
            .as_primitive::<Int64Type>()
            .value(new_row),
        222
    );
}

// ---------------------------------------------------------------------------
// The fold, end to end
// ---------------------------------------------------------------------------

/// A source of NDJSON objects held in memory, standing in for the raw S3 tier.
struct InMemorySource {
    objects: Vec<(String, String)>,
}

#[async_trait]
impl SourceListing for InMemorySource {
    async fn list_after(
        &self,
        _dataset: &str,
        after: Option<&str>,
    ) -> Result<Vec<String>, ServiceError> {
        Ok(self
            .objects
            .iter()
            .map(|(key, _)| key.clone())
            .filter(|key| after.is_none_or(|mark| key.as_str() > mark))
            .collect())
    }

    async fn fetch(&self, _dataset: &str, key: &str) -> Result<String, ServiceError> {
        self.objects
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, body)| body.clone())
            .ok_or_else(|| ServiceError::failed(format!("no such object {key}")))
    }
}

/// The full loop against a real table: create, reconcile, fetch, encode,
/// append, commit — and then do it again and land nothing, because a crashed
/// run must be safe to repeat.
#[tokio::test(flavor = "multi_thread")]
async fn folding_writes_rows_and_a_second_fold_adds_none() {
    let (_dir, concrete) = warehouse().await;
    let concrete = Arc::new(concrete);
    let catalog: Arc<dyn IcebergCatalog> = concrete.clone();
    let listing: Arc<dyn SourceListing> = Arc::new(InMemorySource {
        objects: vec![
            (
                "raw/game-events/dt=2025-08-19/host=a/0-1-aa.json.gz".to_owned(),
                format!("{}\n", game_completed_line("evt-a", DAY_ONE_MS, "use1", 1)),
            ),
            (
                "raw/game-events/dt=2025-08-20/host=a/2-3-bb.json.gz".to_owned(),
                format!("{}\n", user_login_line("evt-b", DAY_TWO_MS, "euw1", 9)),
            ),
        ],
    });
    let target = CommitTarget {
        dataset: "game-events".to_owned(),
        table: TABLE.to_owned(),
    };

    assert_eq!(
        fold_once(&catalog, &listing, &target, 1)
            .await
            .expect("fold"),
        2
    );
    assert_eq!(
        fold_once(&catalog, &listing, &target, 1)
            .await
            .expect("second fold"),
        0,
        "nothing new to fold"
    );

    let batches = scan(&concrete, TABLE).await;
    assert_eq!(row_count(&batches), 2, "no duplicate rows");
    let regions = strings_by_event_id(&batches, "region");
    assert_eq!(regions["evt-a"], Some("use1".to_owned()));
    assert_eq!(regions["evt-b"], Some("euw1".to_owned()));
}

/// The encode runs on `spawn_blocking` and drives the async writer with
/// `Handle::block_on`, which is only sound if the runtime can still make
/// progress while a blocking thread waits on it. Production runs multi-threaded,
/// but a single-threaded runtime is the case that would DEADLOCK rather than
/// fail, so it is pinned here — a deadlock in the committer would hang the task
/// that also owns games.
#[tokio::test(flavor = "current_thread")]
async fn the_write_path_does_not_deadlock_on_a_single_threaded_runtime() {
    let (_dir, catalog) = warehouse().await;
    catalog
        .ensure_table(TABLE, &derived_columns())
        .await
        .expect("create");
    catalog
        .commit(
            TABLE,
            "a.json.gz",
            1,
            &game_completed_line("evt-a", DAY_ONE_MS, "use1", 1),
        )
        .await
        .expect("commit");
    assert_eq!(row_count(&scan(&catalog, TABLE).await), 1);
}
