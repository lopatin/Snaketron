//! The S3 -> Iceberg fold, as a hosted service.
//!
//! Registered by the operator rather than hardcoded, and globally excluded for
//! a concrete reason: every Iceberg commit appends a snapshot entry to
//! `metadata.json`, S3 Tables retains snapshots with no count cap, and the
//! endpoint refuses EVERY operation once that file exceeds 50 MB. Many
//! concurrent committers therefore brick the table within about a day. The
//! universal pattern applies — writers produce files, one coordinator commits.
//!
//! Exclusion here is layered, because no single mechanism is sufficient:
//!
//! 1. The lease keeps the steady state at one committer per table, so metadata
//!    stays small. That is an efficiency property, not a correctness one.
//! 2. The commit is conditioned on the lease epoch, so a stale committer's
//!    commit is REJECTED rather than applied.
//! 3. A marker plus `event_id` dedup make even an unfenced overlap harmless.
//!
//! Each covers a case the others do not: (1) alone double-commits across a GC
//! pause, (2) alone still lets metadata grow under churn, and (3) alone would
//! let two committers race every hour.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use snaketron_service_api::{
    ExclusionKey, HostedService, HostedServiceFactory, ServiceContext, ServiceError,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::resume::{ResumeMarks, listing_floor, today_utc, window_start};
use super::schema::{DerivedColumn, derive_columns, descriptor_pool, event_descriptor};

/// A fold target. One table per dataset, keyed independently so the datasets
/// fold concurrently instead of serializing behind one global lock.
#[derive(Debug, Clone)]
pub struct CommitTarget {
    pub dataset: String,
    pub table: String,
}

/// The catalog operations the committer needs, behind a trait so the fold
/// logic is testable without a live S3 Tables bucket.
#[async_trait]
pub trait IcebergCatalog: Send + Sync + 'static {
    /// Creates the table if it is absent, and is a no-op if it is not.
    ///
    /// `columns` is the proto-derived schema, passed in rather than derived
    /// here so the catalog stays free of protobuf knowledge and remains
    /// testable against a local warehouse.
    ///
    /// Implementations own the partition spec and sort order, and both are
    /// **one-way doors**: iceberg-rust has no partition-spec evolution, so the
    /// layout chosen at creation is the layout the table has for life.
    async fn ensure_table(
        &self,
        table: &str,
        columns: &[DerivedColumn],
    ) -> Result<(), ServiceError>;

    /// Columns currently present on the table.
    async fn current_columns(&self, table: &str) -> Result<Vec<String>, ServiceError>;

    /// Adds columns. Always optional: a required column would demand an
    /// initial default and would destroy the distinction between proto's zero
    /// value and absence.
    async fn add_columns(&self, table: &str, columns: &[DerivedColumn])
    -> Result<(), ServiceError>;

    /// Appends one source object's rows and advances that key's prefix mark,
    /// fenced on `epoch`.
    ///
    /// `rows` is the DECODED NDJSON of the source object — proto3 canonical
    /// JSON, one event per line. It is passed by value rather than fetched
    /// here because the encoding of the raw tier (gzip, and the key layout it
    /// implies) belongs to the source, not to the catalog.
    ///
    /// Implementations MUST reject the commit when a higher epoch has already
    /// committed. A rejection means "I am no longer the holder" and must not
    /// be retried. The append and the resume marks MUST land in one atomic
    /// commit, or a crash between them either duplicates or loses the object.
    ///
    /// The mark check is repeated here rather than trusted from the caller's
    /// snapshot: this is the authoritative one, taken under the same read that
    /// checks the fence.
    async fn commit(
        &self,
        table: &str,
        source_key: &str,
        epoch: u64,
        rows: &str,
    ) -> Result<CommitOutcome, ServiceError>;

    /// The highest folded key **per partition prefix**, used to skip.
    ///
    /// Not a listing bound: the fold always lists a fixed retention window,
    /// because a host that starts writing today produces keys sorting below
    /// marks that already exist, and any floor derived from the marks would
    /// skip exactly those. See `resume.rs`.
    async fn resume_marks(&self, table: &str) -> Result<ResumeMarks, ServiceError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed,
    /// Already folded. The idempotent-replay path, not an error.
    AlreadyPresent,
    /// A newer epoch holds the table. Stop; do not retry.
    Fenced,
}

/// Lists and reads source objects to fold.
#[async_trait]
pub trait SourceListing: Send + Sync + 'static {
    /// Keys strictly after `after`, in lexicographic order.
    async fn list_after(
        &self,
        dataset: &str,
        after: Option<&str>,
    ) -> Result<Vec<String>, ServiceError>;

    /// Reads one object and returns its NDJSON.
    ///
    /// Decoding lives here, not in the catalog: the raw tier's gzip is a
    /// property of how the exporter writes objects (R6.3), and a catalog that
    /// knew about it would have to be taught again for every future encoding.
    async fn fetch(&self, dataset: &str, key: &str) -> Result<String, ServiceError>;
}

/// Reconciles the table schema against the embedded proto descriptors.
///
/// Additive only: it computes the columns the protos describe, diffs by name,
/// and adds what is missing. It never renames or retypes, because the Rust
/// Iceberg writer cannot express either — a rename would become drop+add and
/// silently orphan the old column's data. CI blocks those changes at the proto
/// level so this can stay simple.
pub async fn reconcile_schema(
    catalog: &Arc<dyn IcebergCatalog>,
    table: &str,
) -> Result<Vec<DerivedColumn>, ServiceError> {
    let pool =
        descriptor_pool().map_err(|e| ServiceError::failed(format!("descriptor pool: {e}")))?;
    let event = event_descriptor(&pool)
        .map_err(|e| ServiceError::failed(format!("event descriptor: {e}")))?;
    let desired = derive_columns(&event);

    catalog.ensure_table(table, &desired).await?;

    let existing = catalog.current_columns(table).await?;
    let missing: Vec<DerivedColumn> = desired
        .into_iter()
        .filter(|column| !existing.contains(&column.name))
        .collect();

    if !missing.is_empty() {
        info!(
            "analytics table {table} gaining {} column(s): {}",
            missing.len(),
            missing
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        catalog.add_columns(table, &missing).await?;
    }
    Ok(missing)
}

/// One fold pass. Returns how many objects were committed.
pub async fn fold_once(
    catalog: &Arc<dyn IcebergCatalog>,
    listing: &Arc<dyn SourceListing>,
    target: &CommitTarget,
    epoch: u64,
) -> Result<usize, ServiceError> {
    reconcile_schema(catalog, &target.table).await?;

    let marks = catalog.resume_marks(&target.table).await?;
    // The floor is a retention window, never `min(marks)`. A host that starts
    // writing today emits keys BELOW every existing mark — `host` is
    // `{region}-{server_id}`, so a new region or a new server id can sort
    // anywhere — and a marks-derived floor would list right past them.
    let floor = listing_floor(&target.dataset, window_start(today_utc()));
    let keys = listing.list_after(&target.dataset, Some(&floor)).await?;

    let mut committed = 0usize;
    for key in keys {
        // A local skip so the window is not re-fetched and re-encoded every
        // tick. It is an optimization only: `commit` repeats the check against
        // the table itself, which is the authoritative one.
        if marks.already_folded(&key) {
            continue;
        }
        let rows = listing.fetch(&target.dataset, &key).await?;
        match catalog.commit(&target.table, &key, epoch, &rows).await? {
            CommitOutcome::Committed => committed += 1,
            CommitOutcome::AlreadyPresent => {}
            CommitOutcome::Fenced => {
                // Losing the fence is not retryable: a newer committer owns
                // the table and this one must stand down immediately.
                warn!("analytics committer fenced at epoch {epoch}; standing down");
                return Err(ServiceError::failed("fenced by a newer epoch"));
            }
        }
    }
    Ok(committed)
}

pub struct IcebergCommitterFactory {
    catalog: Arc<dyn IcebergCatalog>,
    listing: Arc<dyn SourceListing>,
    target: CommitTarget,
    interval: Duration,
}

impl IcebergCommitterFactory {
    pub fn new(
        catalog: Arc<dyn IcebergCatalog>,
        listing: Arc<dyn SourceListing>,
        target: CommitTarget,
        interval: Duration,
    ) -> Self {
        Self {
            catalog,
            listing,
            target,
            interval,
        }
    }
}

#[async_trait]
impl HostedServiceFactory for IcebergCommitterFactory {
    fn name(&self) -> &'static str {
        "iceberg-committer"
    }

    /// Global, and keyed by table.
    ///
    /// Global because Valkey does not span regions, so a regional key would
    /// elect two committers and reintroduce the metadata problem. Keyed by
    /// table so `game_events` and `websocket_events` fold concurrently.
    fn exclusion_key(&self, _ctx: &ServiceContext) -> Option<ExclusionKey> {
        Some(ExclusionKey::global(format!(
            "iceberg-committer/{}",
            self.target.table
        )))
    }

    async fn build(&self, ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        let epoch = ctx
            .lease
            .as_ref()
            .map(|lease| lease.epoch())
            .ok_or_else(|| {
                // Without a lease there is no fencing token, and an unfenced
                // committer is exactly the thing that bricks the table.
                ServiceError::MissingDependency(
                    "iceberg committer requires an exclusion lease".to_owned(),
                )
            })?;

        Ok(Box::new(IcebergCommitter {
            catalog: self.catalog.clone(),
            listing: self.listing.clone(),
            target: self.target.clone(),
            interval: self.interval,
            epoch,
            ctx,
        }))
    }
}

struct IcebergCommitter {
    catalog: Arc<dyn IcebergCatalog>,
    listing: Arc<dyn SourceListing>,
    target: CommitTarget,
    interval: Duration,
    epoch: u64,
    ctx: ServiceContext,
}

#[async_trait]
impl HostedService for IcebergCommitter {
    async fn run(&mut self, cancel: CancellationToken) -> Result<(), ServiceError> {
        self.ctx.mark_ready();
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {}
            }

            match fold_once(&self.catalog, &self.listing, &self.target, self.epoch).await {
                Ok(0) => {}
                Ok(count) => info!(
                    "analytics committer folded {count} object(s) into {}",
                    self.target.table
                ),
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeCatalog {
        columns: Mutex<Vec<String>>,
        committed: Mutex<Vec<String>>,
        rows: Mutex<Vec<String>>,
        added: Mutex<Vec<Vec<String>>>,
        fence_at: Mutex<Option<u64>>,
        created: Mutex<usize>,
    }

    #[async_trait]
    impl IcebergCatalog for FakeCatalog {
        async fn ensure_table(
            &self,
            _table: &str,
            _columns: &[DerivedColumn],
        ) -> Result<(), ServiceError> {
            *self.created.lock().unwrap() += 1;
            Ok(())
        }

        async fn current_columns(&self, _table: &str) -> Result<Vec<String>, ServiceError> {
            Ok(self.columns.lock().unwrap().clone())
        }

        async fn add_columns(
            &self,
            _table: &str,
            columns: &[DerivedColumn],
        ) -> Result<(), ServiceError> {
            let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            self.columns.lock().unwrap().extend(names.clone());
            self.added.lock().unwrap().push(names);
            Ok(())
        }

        async fn commit(
            &self,
            _table: &str,
            source_key: &str,
            epoch: u64,
            rows: &str,
        ) -> Result<CommitOutcome, ServiceError> {
            if let Some(fence) = *self.fence_at.lock().unwrap()
                && epoch < fence
            {
                return Ok(CommitOutcome::Fenced);
            }
            let mut committed = self.committed.lock().unwrap();
            if committed.iter().any(|k| k == source_key) {
                return Ok(CommitOutcome::AlreadyPresent);
            }
            committed.push(source_key.to_owned());
            self.rows.lock().unwrap().push(rows.to_owned());
            Ok(CommitOutcome::Committed)
        }

        /// Built from what was actually committed, exactly as the real
        /// catalog derives it from the marks property — so a fold test here
        /// exercises the same skip rule production uses.
        async fn resume_marks(&self, _table: &str) -> Result<ResumeMarks, ServiceError> {
            let mut marks = ResumeMarks::default();
            for key in self.committed.lock().unwrap().iter() {
                marks.record(key);
            }
            Ok(marks)
        }
    }

    /// Objects in a bucket, filtered exactly as `ListObjectsV2(StartAfter=…)`
    /// filters: strictly greater, lexicographic.
    struct FakeListing {
        keys: Vec<String>,
    }

    #[async_trait]
    impl SourceListing for FakeListing {
        async fn list_after(
            &self,
            _dataset: &str,
            after: Option<&str>,
        ) -> Result<Vec<String>, ServiceError> {
            Ok(match after {
                None => self.keys.clone(),
                Some(mark) => self
                    .keys
                    .iter()
                    .filter(|k| k.as_str() > mark)
                    .cloned()
                    .collect(),
            })
        }

        async fn fetch(&self, _dataset: &str, key: &str) -> Result<String, ServiceError> {
            Ok(format!("{{\"event_id\":\"{key}\"}}"))
        }
    }

    fn target() -> CommitTarget {
        CommitTarget {
            dataset: "game-events".to_owned(),
            table: "game_events".to_owned(),
        }
    }

    /// Today in UTC, computed here rather than through the production window
    /// helper, so a bug in that helper cannot make these tests agree with it.
    fn today() -> String {
        chrono::Utc::now().format("%Y-%m-%d").to_string()
    }

    /// A key exactly as the exporter writes one, on a given day.
    ///
    /// Real keys, not `a.json.gz`: the fold's listing floor is a retention
    /// window over `raw/{dataset}/dt=…`, so a fixture outside that layout
    /// would be filtered out and every fold test would pass while folding
    /// nothing.
    fn key_on(date: &str, host: &str, cursor: &str) -> String {
        crate::analytics::object_store::object_key("game-events", date, host, cursor, cursor, "h")
    }

    /// A key inside the retention window.
    fn raw_key(host: &str, cursor: &str) -> String {
        key_on(&today(), host, cursor)
    }

    fn parts(
        keys: &[String],
    ) -> (
        Arc<dyn IcebergCatalog>,
        Arc<dyn SourceListing>,
        Arc<FakeCatalog>,
    ) {
        let catalog = Arc::new(FakeCatalog::default());
        let listing: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: keys.to_vec(),
        });
        (catalog.clone(), listing, catalog)
    }

    fn two_objects() -> [String; 2] {
        [raw_key("use1-1", "1"), raw_key("use1-1", "2")]
    }

    /// A brand-new table gains every proto-derived column on the first pass.
    #[tokio::test]
    async fn an_empty_table_gains_the_full_derived_schema() {
        let (catalog, _listing, fake) = parts(&[]);
        let added = reconcile_schema(&catalog, "game_events").await.unwrap();
        assert!(!added.is_empty());
        assert!(added.iter().any(|c| c.name == "event_id"));
        assert!(added.iter().any(|c| c.name == "identity.user_id"));
        assert!(
            added.iter().all(|c| c.optional),
            "every column must be optional"
        );
        assert_eq!(fake.added.lock().unwrap().len(), 1);
    }

    /// The steady state: nothing to do, and crucially no DDL is issued.
    #[tokio::test]
    async fn an_up_to_date_table_is_left_alone() {
        let (catalog, _listing, fake) = parts(&[]);
        reconcile_schema(&catalog, "game_events").await.unwrap();
        fake.added.lock().unwrap().clear();

        let added = reconcile_schema(&catalog, "game_events").await.unwrap();
        assert!(added.is_empty());
        assert!(fake.added.lock().unwrap().is_empty(), "no redundant DDL");
    }

    /// Adding a proto field must be an additive column add — no rewrite, and
    /// nothing dropped.
    #[tokio::test]
    async fn a_new_field_is_added_without_touching_existing_columns() {
        let (catalog, _listing, fake) = parts(&[]);
        reconcile_schema(&catalog, "game_events").await.unwrap();
        let before = fake.columns.lock().unwrap().clone();

        // Simulate a table that predates one column.
        fake.columns.lock().unwrap().retain(|c| c != "event_id");
        fake.added.lock().unwrap().clear();

        let added = reconcile_schema(&catalog, "game_events").await.unwrap();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].name, "event_id");
        let after = fake.columns.lock().unwrap().clone();
        assert_eq!(after.len(), before.len(), "no column may be lost");
    }

    #[tokio::test]
    async fn folding_commits_each_source_object_once() {
        let (catalog, listing, fake) = parts(&two_objects());
        let count = fold_once(&catalog, &listing, &target(), 1).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(fake.committed.lock().unwrap().len(), 2);
    }

    /// The fold must hand the catalog the object's BYTES, not just its key —
    /// a commit that moved a resume mark without carrying rows would look
    /// identical from the outside and would lose every event.
    #[tokio::test]
    async fn each_commit_carries_the_rows_read_from_its_object() {
        let objects = two_objects();
        let (catalog, listing, fake) = parts(&objects);
        fold_once(&catalog, &listing, &target(), 1).await.unwrap();

        let rows = fake.rows.lock().unwrap().clone();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains(&objects[0]), "{rows:?}");
        assert!(rows[1].contains(&objects[1]), "{rows:?}");
    }

    /// Every pass ensures the table, because the first fold of a fresh
    /// environment has no table to reconcile against.
    #[tokio::test]
    async fn every_fold_ensures_the_table_exists_first() {
        let (catalog, listing, fake) = parts(&[raw_key("use1-1", "1")]);
        fold_once(&catalog, &listing, &target(), 1).await.unwrap();
        fold_once(&catalog, &listing, &target(), 1).await.unwrap();
        assert_eq!(*fake.created.lock().unwrap(), 2);
    }

    /// Re-running must be a no-op, which is what makes a crashed run safe to
    /// repeat. The listing hands back the same window every tick, so the skip
    /// is doing the work here — an empty second fold is not evidence on its
    /// own, which is why the object count is asserted too.
    #[tokio::test]
    async fn a_second_fold_is_idempotent() {
        let objects = two_objects();
        let (catalog, listing, fake) = parts(&objects);
        fold_once(&catalog, &listing, &target(), 1).await.unwrap();
        let second = fold_once(&catalog, &listing, &target(), 1).await.unwrap();
        assert_eq!(second, 0, "nothing new to commit");
        assert_eq!(fake.committed.lock().unwrap().len(), 2, "no duplicate rows");
        assert_eq!(
            fake.rows.lock().unwrap().len(),
            2,
            "an already-folded object must not even be fetched"
        );
    }

    /// The two-region loss, reproduced at the fold.
    ///
    /// `host` is `{region}-{server_id}` (`exporter_service.rs`), so
    /// `host=euw1-*` sorts BELOW `host=use1-*`. Fold once while only the US
    /// host has written and a single global mark lands on a US key; every EU
    /// object written afterwards for that same day sorts beneath it and is
    /// skipped forever.
    #[tokio::test]
    async fn an_eu_object_written_after_a_us_object_is_still_folded() {
        // Computed here rather than through the production window helper, so a
        // bug in that helper cannot make this test agree with it.
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let raw = |host: &str, cursor: &str| {
            crate::analytics::object_store::object_key(
                "game-events",
                &day,
                host,
                cursor,
                cursor,
                "hash",
            )
        };
        let us = raw("use1-3", "1000000000000-0");
        let eu = raw("euw1-5", "2000000000000-0");
        assert!(
            eu < us,
            "the premise of this test is that the EU key sorts below the US key:\n  eu={eu}\n  us={us}"
        );

        let fake = Arc::new(FakeCatalog::default());
        let catalog: Arc<dyn IcebergCatalog> = fake.clone();

        // 1. Only the US host has written anything.
        let first: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: vec![us.clone()],
        });
        assert_eq!(
            fold_once(&catalog, &first, &target(), 1).await.unwrap(),
            1,
            "the US object must fold"
        );

        // 2. The EU host writes afterwards. Its key sorts below the US key.
        let second: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: vec![eu.clone(), us.clone()],
        });
        assert_eq!(
            fold_once(&catalog, &second, &target(), 1).await.unwrap(),
            1,
            "the EU object must fold too"
        );

        let committed = fake.committed.lock().unwrap().clone();
        assert!(
            committed.contains(&eu),
            "an entire region was silently dropped: {committed:?}"
        );
    }

    /// Resume must pick up only what is new, not re-commit the whole window.
    #[tokio::test]
    async fn folding_commits_only_what_the_marks_do_not_cover() {
        let objects = two_objects();
        let (catalog, listing, fake) = parts(&objects);
        fold_once(&catalog, &listing, &target(), 1).await.unwrap();

        let third = raw_key("use1-1", "3");
        let listing2: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: vec![objects[0].clone(), objects[1].clone(), third.clone()],
        });
        let count = fold_once(&catalog, &listing2, &target(), 1).await.unwrap();
        assert_eq!(count, 1, "only the new object");
        assert_eq!(fake.committed.lock().unwrap().len(), 3);
    }

    /// The same loss as the two-region case, inside one region: exporter
    /// failover moves the server id, and `use1-12` sorts below `use1-3`.
    #[tokio::test]
    async fn an_object_from_a_new_server_id_below_the_existing_mark_is_folded() {
        let three = raw_key("use1-3", "1000000000000-0");
        let twelve = raw_key("use1-12", "2000000000000-0");
        assert!(twelve < three, "the premise:\n  {twelve}\n  {three}");

        let fake = Arc::new(FakeCatalog::default());
        let catalog: Arc<dyn IcebergCatalog> = fake.clone();

        let before: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: vec![three.clone()],
        });
        fold_once(&catalog, &before, &target(), 1).await.unwrap();

        let after: Arc<dyn SourceListing> = Arc::new(FakeListing {
            keys: vec![twelve.clone(), three.clone()],
        });
        assert_eq!(
            fold_once(&catalog, &after, &target(), 1).await.unwrap(),
            1,
            "the failed-over exporter's object must fold"
        );
        assert!(fake.committed.lock().unwrap().contains(&twelve));
    }

    /// The lateness budget, stated as a test rather than left implicit: the
    /// fold lists a retention window, so an object written for a day older
    /// than the window is never folded. This is the cost of not deriving the
    /// floor from the marks, and `RETENTION_DAYS` is the knob for it.
    #[tokio::test]
    async fn an_object_older_than_the_retention_window_is_not_folded() {
        let ancient = key_on("2001-01-01", "use1-1", "1");
        let fresh = raw_key("use1-1", "1");
        let (catalog, listing, fake) = parts(&[ancient.clone(), fresh.clone()]);

        assert_eq!(
            fold_once(&catalog, &listing, &target(), 1).await.unwrap(),
            1
        );
        let committed = fake.committed.lock().unwrap().clone();
        assert_eq!(committed, vec![fresh]);
    }

    /// The fencing property: a stale committer's commit is rejected, and the
    /// rejection stops it rather than being retried.
    #[tokio::test]
    async fn a_stale_epoch_is_fenced_and_stops() {
        let (catalog, listing, fake) = parts(&[raw_key("use1-1", "1")]);
        *fake.fence_at.lock().unwrap() = Some(9);

        let error = fold_once(&catalog, &listing, &target(), 3)
            .await
            .expect_err("a fenced committer must not continue");
        assert!(error.to_string().contains("fenced"));
        assert!(
            fake.committed.lock().unwrap().is_empty(),
            "a fenced commit must not be applied"
        );
    }

    /// The newer epoch still works — fencing rejects the stale writer only.
    #[tokio::test]
    async fn the_current_epoch_commits_normally_while_fencing_is_active() {
        let (catalog, listing, fake) = parts(&[raw_key("use1-1", "1")]);
        *fake.fence_at.lock().unwrap() = Some(9);
        let count = fold_once(&catalog, &listing, &target(), 9).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(fake.committed.lock().unwrap().len(), 1);
    }

    #[test]
    fn the_committer_is_globally_excluded_and_keyed_by_table() {
        let (catalog, listing, _) = parts(&[]);
        let factory =
            IcebergCommitterFactory::new(catalog, listing, target(), Duration::from_secs(3600));
        assert_eq!(factory.name(), "iceberg-committer");

        let ctx = crate::analytics::committer::tests_support::context();
        let key = factory.exclusion_key(&ctx).expect("must be excluded");
        assert_eq!(key.domain, snaketron_service_api::ExclusionDomain::Global);
        assert_eq!(key.key, "iceberg-committer/game_events");
    }

    /// Without a lease there is no fencing token, so building must fail rather
    /// than silently running unfenced.
    #[tokio::test]
    async fn building_without_a_lease_is_refused() {
        let (catalog, listing, _) = parts(&[]);
        let factory =
            IcebergCommitterFactory::new(catalog, listing, target(), Duration::from_secs(3600));
        // `Box<dyn HostedService>` is not Debug, so match rather than unwrap.
        match factory.build(tests_support::context()).await {
            Err(ServiceError::MissingDependency(message)) => {
                assert!(message.contains("lease"), "unexpected message: {message}");
            }
            Err(other) => panic!("wrong error: {other}"),
            Ok(_) => panic!("an unfenced committer must not be built"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use async_trait::async_trait;
    use snaketron_service_api::deps::{CasOutcome, KeyValueStore, LifecycleView};
    use snaketron_service_api::{
        Environment, RegionId, ServiceConfig, ServiceContext, ServiceError, TaskIdentity,
    };
    use std::sync::Arc;
    use std::time::Duration;

    struct NoStore;

    #[async_trait]
    impl KeyValueStore for NoStore {
        async fn get(&self, _key: &str) -> Result<Option<String>, ServiceError> {
            Ok(None)
        }
        async fn try_acquire_lease(
            &self,
            _key: &str,
            _holder: &str,
            _rank: u32,
            _ttl: Duration,
        ) -> Result<CasOutcome, ServiceError> {
            Ok(CasOutcome::Rejected)
        }
        async fn extend_if_equal(
            &self,
            _key: &str,
            _expected: &str,
            _ttl: Duration,
        ) -> Result<CasOutcome, ServiceError> {
            Ok(CasOutcome::Rejected)
        }
        async fn delete_if_equal(
            &self,
            _key: &str,
            _expected: &str,
        ) -> Result<CasOutcome, ServiceError> {
            Ok(CasOutcome::Rejected)
        }
        async fn increment(&self, _key: &str) -> Result<u64, ServiceError> {
            Ok(1)
        }
    }

    struct NeverDrains;

    #[async_trait]
    impl LifecycleView for NeverDrains {
        async fn on_drain(&self) {
            std::future::pending::<()>().await;
        }
        fn is_draining(&self) -> bool {
            false
        }
    }

    pub fn context() -> ServiceContext {
        ServiceContext::new(
            Environment("test".to_owned()),
            RegionId("use1".to_owned()),
            "us-east-1".to_owned(),
            TaskIdentity {
                server_id: 1,
                boot_id: "boot".to_owned(),
                task_boot_id: "1:boot".to_owned(),
            },
            Arc::new(NoStore),
            Arc::new(NeverDrains),
            ServiceConfig::default(),
            None,
        )
    }
}
