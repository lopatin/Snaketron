//! A real [`IcebergCatalog`] backed by AWS S3 Tables.
//!
//! This is the production implementation of the trait the committer defines;
//! `committer.rs` keeps a fake for the fold-logic tests. Everything here is
//! shaped by four constraints of `iceberg` 0.10.1, none of which are optional:
//!
//! 1. **`update_schema` can only add and delete.** No rename, no type
//!    promotion, no partition-spec evolution. So the diff is name-matched and
//!    strictly additive, and `schema.rs` pre-widens every integer so a
//!    promotion is never needed.
//! 2. **A parent path resolves against the schema the transaction started
//!    from**, not against columns added earlier in the same transaction. A new
//!    struct and its children therefore cannot be two adds — the struct must
//!    arrive as one add carrying its whole nested type. That is what
//!    [`plan_additions`] computes.
//! 3. **Adding into an existing struct is a pure add.** The new field takes
//!    `last-column-id + 1`, every sibling id is untouched, and older files
//!    project as null. It is never drop+add, which would orphan data.
//! 4. **Schema evolution and data append never share a transaction.**
//! 5. **There is no partition-spec evolution.** `Transaction` offers
//!    `update_schema`, `replace_sort_order`, and property/location/statistics
//!    updates — and nothing that adds, drops, or re-binds a partition field.
//!    The spec chosen in [`partition_spec`] at creation is therefore the spec
//!    the table has for life, which is why it needs a synthetic `occurred_at`
//!    column: `day()` is undefined on the `long` the protos carry.
//!
//! The append itself is [`write_data_files`] plus `fast_append`, and it shares
//! ONE transaction with the resume mark so a crash cannot land one without the
//! other. The encode runs on `spawn_blocking` because this process is also
//! serving games on a 100 ms tick.
//!
//! The catalog is `iceberg-catalog-s3tables`, which speaks the native
//! `aws-sdk-s3tables` API rather than the Iceberg REST endpoint, so SigV4
//! comes from the AWS SDK and needs no code here.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, NestedField, NestedFieldRef, NullOrder, PartitionSpec,
    PrimitiveType, Schema, SortDirection, SortField, SortOrder, StructType, Transform, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use snaketron_service_api::ServiceError;
use tracing::{info, warn};

use super::arrow_rows::{OCCURRED_AT_COLUMN, OCCURRED_AT_MS_COLUMN, ndjson_to_record_batch};
use super::committer::{CommitOutcome, IcebergCatalog};
use super::schema::DerivedColumn;

/// The lease epoch of the committer that last committed.
///
/// This is the fencing token. It lives in table properties rather than
/// anywhere else because the table is the thing being protected: a writer that
/// can reach the table can always read the epoch that guards it, with no
/// second store to be unavailable at the wrong moment.
pub const EPOCH_PROPERTY: &str = "snaketron.committer.epoch";

/// The highest source key already folded, used to resume.
pub const HIGH_WATER_MARK_PROPERTY: &str = "snaketron.committer.high-water-mark";

/// Property name for the AWS region, as `iceberg-catalog-s3tables` spells it.
const AWS_REGION_NAME: &str = "region_name";

/// Connection details for the S3 Tables catalog.
#[derive(Debug, Clone)]
pub struct S3TablesConfig {
    /// ARN of the S3 table bucket holding the analytics namespace.
    pub table_bucket_arn: String,
    /// Namespace (S3 Tables calls it a "namespace"; Athena sees a database).
    pub namespace: String,
    /// Region the table bucket lives in.
    pub region: String,
    /// Catalog name. Local to this process; it names nothing in AWS.
    pub catalog_name: String,
}

/// The production catalog.
#[derive(Debug)]
pub struct S3TablesIcebergCatalog {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
}

impl S3TablesIcebergCatalog {
    /// Connects to S3 Tables. Credentials come from the standard AWS provider
    /// chain, which on Fargate is the task role.
    pub async fn connect(config: &S3TablesConfig) -> Result<Self, ServiceError> {
        let mut props = HashMap::new();
        props.insert(AWS_REGION_NAME.to_owned(), config.region.clone());

        let catalog = S3TablesCatalogBuilder::default()
            .with_table_bucket_arn(config.table_bucket_arn.clone())
            .load(config.catalog_name.clone(), props)
            .await
            .map_err(|e| ServiceError::failed(format!("loading S3 Tables catalog: {e}")))?;

        Ok(Self::new(Arc::new(catalog), config.namespace.clone()))
    }

    /// Wraps an already-built catalog. Separate from [`Self::connect`] so a
    /// caller that already holds a `Catalog` — an integration test against a
    /// local endpoint, say — does not have to go back through AWS discovery.
    pub fn new(catalog: Arc<dyn Catalog>, namespace: impl Into<String>) -> Self {
        Self {
            catalog,
            namespace: NamespaceIdent::new(namespace.into()),
        }
    }

    fn ident(&self, table: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), table.to_owned())
    }

    /// Loads a table, refusing anything that is not format-version 2.
    ///
    /// Public because the format-version check is the only correct way to get
    /// a handle on one of these tables; a caller that bypassed it could write
    /// happily into a table Athena can no longer read.
    pub async fn load(&self, table: &str) -> Result<Table, ServiceError> {
        let loaded = self
            .catalog
            .load_table(&self.ident(table))
            .await
            .map_err(|e| ServiceError::failed(format!("loading table {table}: {e}")))?;
        require_v2(table, loaded.metadata().format_version())?;
        Ok(loaded)
    }

    /// Creates the namespace if it is absent.
    ///
    /// A table cannot be created under a namespace that does not exist, and the
    /// first fold of a fresh environment is exactly the case where it does not.
    async fn ensure_namespace(&self) -> Result<(), ServiceError> {
        if self
            .catalog
            .namespace_exists(&self.namespace)
            .await
            .map_err(|e| ServiceError::failed(format!("checking namespace: {e}")))?
        {
            return Ok(());
        }
        match self
            .catalog
            .create_namespace(&self.namespace, HashMap::new())
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                if self
                    .catalog
                    .namespace_exists(&self.namespace)
                    .await
                    .unwrap_or(false)
                {
                    return Ok(());
                }
                Err(ServiceError::failed(format!("creating namespace: {error}")))
            }
        }
    }

    /// Tears a table down. `purge_table`, never `drop_table`: the latter is an
    /// explicit `FeatureUnsupported` stub against S3 Tables, so it fails
    /// rather than deleting anything.
    pub async fn purge(&self, table: &str) -> Result<(), ServiceError> {
        self.catalog
            .purge_table(&self.ident(table))
            .await
            .map_err(|e| ServiceError::failed(format!("purging table {table}: {e}")))
    }
}

/// Athena implements Iceberg spec 1.4.2 and reads v2 only. A v3 table would
/// still serve Rust reads and writes, so nothing here would fail — the loss
/// would surface only as Athena refusing to query, far from the cause. Fail at
/// the catalog boundary instead.
fn require_v2(table: &str, version: FormatVersion) -> Result<(), ServiceError> {
    if version == FormatVersion::V2 {
        return Ok(());
    }
    Err(ServiceError::failed(format!(
        "table {table} is format-version {version:?}, but Athena reads v2 only"
    )))
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Maps a [`DerivedColumn`]'s type name onto an Iceberg type.
///
/// The pre-widening in `schema.rs` is respected exactly as encoded there —
/// `uint32` already arrives as `long`, `uint64` as `decimal(20,0)`, enums as
/// `string`, timestamps as `timestamptz` — because `iceberg` has no
/// type-promotion API and a promotion that is needed later cannot be
/// performed at all.
///
/// An unrecognized name is an error rather than a silent `string`: a
/// misspelled type that quietly became a string would be discovered only when
/// the data was already unqueryable.
fn iceberg_type(type_name: &str) -> Result<Type, ServiceError> {
    let primitive = match type_name {
        "boolean" => PrimitiveType::Boolean,
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "string" => PrimitiveType::String,
        "binary" => PrimitiveType::Binary,
        "date" => PrimitiveType::Date,
        "uuid" => PrimitiveType::Uuid,
        // Millisecond precision, per R8.12: Iceberg stores microseconds but
        // Athena reads and writes milliseconds, so the proto layer decides.
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp" => PrimitiveType::Timestamp,
        "struct" => return Ok(Type::Struct(StructType::new(Vec::new()))),
        other => return parse_decimal(other),
    };
    Ok(Type::Primitive(primitive))
}

/// `decimal(p,s)` — the only parameterized type `schema.rs` emits, for
/// `uint64`, whose full unsigned range does not fit a signed `long`.
fn parse_decimal(type_name: &str) -> Result<Type, ServiceError> {
    let unknown = || ServiceError::failed(format!("unmappable derived type `{type_name}`"));

    let args = type_name
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(unknown)?;
    let (precision, scale) = args.split_once(',').ok_or_else(unknown)?;
    let precision: u32 = precision.trim().parse().map_err(|_| unknown())?;
    let scale: u32 = scale.trim().parse().map_err(|_| unknown())?;

    if precision == 0 || precision > 38 || scale > precision {
        return Err(ServiceError::failed(format!(
            "decimal({precision},{scale}) is outside Iceberg's supported range"
        )));
    }
    Ok(Type::Primitive(PrimitiveType::Decimal { precision, scale }))
}

/// The `doc` string that makes table metadata its own registry.
///
/// The proto field NUMBER path is the durable identity; the column name is how
/// that identity is carried forward; the Iceberg field id is Iceberg's private
/// business and is never chosen here. Writing `pb:<path>` into `doc` makes
/// number → field-id recoverable by scanning the current schema, with no
/// external registry to drift out of sync with the running binary.
fn doc_for(column: &DerivedColumn) -> String {
    format!("pb:{}", column.proto_path)
}

/// Splits a dotted column name into its parent path and leaf name.
fn split_path(name: &str) -> (Option<&str>, &str) {
    match name.rsplit_once('.') {
        Some((parent, leaf)) => (Some(parent), leaf),
        None => (None, name),
    }
}

// ---------------------------------------------------------------------------
// Reading the current schema
// ---------------------------------------------------------------------------

/// Flattens a schema into dotted column names, mirroring `derive_columns`.
///
/// Structs contribute their own name AND recurse, exactly as the derivation
/// does, so the two sides of the diff are directly comparable. Lists and maps
/// are leaves on both sides.
pub fn schema_column_names(schema: &Schema) -> Vec<String> {
    let mut names = Vec::new();
    collect_names(schema.as_struct().fields(), "", &mut names);
    names
}

fn collect_names(fields: &[NestedFieldRef], prefix: &str, out: &mut Vec<String>) {
    for field in fields {
        let name = if prefix.is_empty() {
            field.name.clone()
        } else {
            format!("{prefix}.{}", field.name)
        };
        if let Type::Struct(nested) = field.field_type.as_ref() {
            out.push(name.clone());
            collect_names(nested.fields(), &name, out);
        } else {
            out.push(name);
        }
    }
}

// ---------------------------------------------------------------------------
// Planning the additions
// ---------------------------------------------------------------------------

/// One `AddColumn` to issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedAdd {
    /// Dotted path of the existing struct to add into, or `None` for a
    /// top-level add.
    pub parent: Option<String>,
    pub name: String,
    pub field_type: Type,
    pub doc: String,
}

/// Groups missing columns into the smallest set of adds the API can express.
///
/// The rule that forces this shape: a parent path resolves against the schema
/// the transaction STARTED from, so `identity` and `identity.user_id` cannot
/// both be adds in one transaction — the second would fail with "parent not
/// found". A wholly new struct must therefore arrive as a single add carrying
/// its complete nested type, and only columns landing inside a struct that
/// ALREADY exists get a `parent`.
///
/// So each missing column is attached to its shallowest missing ancestor, and
/// each such ancestor becomes one add.
pub fn plan_additions(
    existing: &BTreeSet<String>,
    missing: &[DerivedColumn],
) -> Result<Vec<PlannedAdd>, ServiceError> {
    let by_name: BTreeMap<&str, &DerivedColumn> =
        missing.iter().map(|c| (c.name.as_str(), c)).collect();

    // Roots in sorted order, so the plan is deterministic across processes —
    // the same property that makes the desired schema byte-identical on any
    // two tasks running the same binary.
    let mut roots: BTreeSet<&str> = BTreeSet::new();
    for column in missing {
        roots.insert(attach_root(existing, &column.name));
    }

    let mut plan = Vec::with_capacity(roots.len());
    for root in roots {
        let column = by_name.get(root).copied().ok_or_else(|| {
            // Reachable only if the derivation emitted a child without its
            // parent struct, which would make the add unexpressible.
            ServiceError::failed(format!(
                "cannot add column under missing ancestor `{root}`: the ancestor itself was not derived"
            ))
        })?;

        let (parent, leaf) = split_path(root);
        plan.push(PlannedAdd {
            parent: parent.map(str::to_owned),
            name: leaf.to_owned(),
            field_type: build_type(column, &by_name)?,
            doc: doc_for(column),
        });
    }
    Ok(plan)
}

/// The shallowest prefix of `name` that the table does not already have.
///
/// `identity.user_id` against a table that has `identity` yields
/// `identity.user_id`; against an empty table it yields `identity`.
fn attach_root<'a>(existing: &BTreeSet<String>, name: &'a str) -> &'a str {
    let mut end = 0;
    while let Some(offset) = name[end..].find('.') {
        end += offset;
        if !existing.contains(&name[..end]) {
            return &name[..end];
        }
        end += 1;
    }
    name
}

/// Builds the Iceberg type for one column, recursing into struct children.
///
/// Field ids here are placeholders. `update_schema` reassigns every id from
/// `last-column-id` at commit time, and an id chosen by a caller is not
/// expressible in any Iceberg implementation.
fn build_type(
    column: &DerivedColumn,
    by_name: &BTreeMap<&str, &DerivedColumn>,
) -> Result<Type, ServiceError> {
    let mut next_id = 0;
    build_type_inner(column, by_name, &mut next_id)
}

fn build_type_inner(
    column: &DerivedColumn,
    by_name: &BTreeMap<&str, &DerivedColumn>,
    next_id: &mut i32,
) -> Result<Type, ServiceError> {
    let base = iceberg_type(&column.type_name)?;
    let Type::Struct(_) = base else {
        return Ok(base);
    };

    let prefix = format!("{}.", column.name);
    let mut fields: Vec<NestedFieldRef> = Vec::new();
    for (name, child) in by_name {
        let Some(rest) = name.strip_prefix(&prefix) else {
            continue;
        };
        if rest.contains('.') {
            continue; // A grandchild; it is handled by its own parent.
        }
        let child_type = build_type_inner(child, by_name, next_id)?;
        *next_id += 1;
        // Optional, always. A required column demands an initial_default and
        // destroys the distinction between proto's zero value and absence.
        let mut field = NestedField::optional(*next_id, rest.to_owned(), child_type);
        field.doc = Some(doc_for(child));
        fields.push(Arc::new(field));
    }

    if fields.is_empty() {
        // An empty proto message — `message UserLogin {}`, a oneof arm whose
        // only information is that it happened — carries exactly one bit, so
        // the column is a boolean presence flag.
        //
        // It cannot stay a struct: an empty Iceberg struct becomes a Parquet
        // group with no children, which most writers refuse to emit and Athena
        // refuses to read. Deciding it here, rather than letting an empty
        // struct reach the table, keeps the failure out of query time.
        //
        // A proto `map` would also land here — `derive_columns` types a map as
        // a childless `struct` — and a boolean would be wrong for it. That
        // ambiguity is not live: `no_map_is_used_in_the_analytics_protos`
        // pins that these protos have no maps at all.
        return Ok(Type::Primitive(PrimitiveType::Boolean));
    }
    Ok(Type::Struct(StructType::new(fields)))
}

// ---------------------------------------------------------------------------
// Fencing
// ---------------------------------------------------------------------------

/// Whether a commit at `mine` must be rejected.
///
/// Rejection means "a newer committer owns this table, so I am no longer the
/// holder". It is never retryable. Equality proceeds: it is this same holder
/// resuming, which is the ordinary case across a restart.
fn is_fenced(stored: Option<u64>, mine: u64) -> bool {
    matches!(stored, Some(stored) if stored > mine)
}

/// Reads the stored epoch. A malformed value is an error, never "unfenced" —
/// treating garbage as absence would silently disable the fence.
fn read_epoch(properties: &HashMap<String, String>) -> Result<Option<u64>, ServiceError> {
    properties
        .get(EPOCH_PROPERTY)
        .map(|raw| {
            raw.parse::<u64>().map_err(|_| {
                ServiceError::failed(format!(
                    "table property {EPOCH_PROPERTY} is not a u64: {raw}"
                ))
            })
        })
        .transpose()
}

/// Whether `source_key` is at or below the high-water mark.
///
/// Keys sort chronologically within a prefix, so this is the whole resume
/// story: everything at or below the mark is already folded.
fn already_folded(high_water_mark: Option<&str>, source_key: &str) -> bool {
    matches!(high_water_mark, Some(mark) if source_key <= mark)
}

// ---------------------------------------------------------------------------
// Table creation
// ---------------------------------------------------------------------------

/// Partition field name for the event day.
///
/// Named separately from the source column because Iceberg forbids a
/// non-identity partition field from taking a schema column's name.
const OCCURRED_AT_DAY_PARTITION: &str = "occurred_at_day";

/// The identity-partitioned region column. It is a schema column AND a
/// partition field, which Iceberg allows only for the identity transform.
const REGION_COLUMN: &str = "region";

/// The first sort key. Almost every query filters on it, so clustering by it
/// is what lets row-group statistics skip most of a day's files.
const EVENT_NAME_COLUMN: &str = "event_name";

/// The `doc` string on the one column that is not a proto field.
///
/// Deliberately not spelled `pb:` — that prefix means "this is proto field
/// number N", and claiming a number for a synthetic column would corrupt the
/// registry that the `doc` strings ARE.
const OCCURRED_AT_DOC: &str = "derived:occurred_at_ms";

/// The schema a brand-new table is created with.
///
/// It is the proto-derived schema plus exactly one synthetic column,
/// [`OCCURRED_AT_COLUMN`]. That column exists because the partition spec needs
/// it: `day()` is defined on a date or a timestamp, `occurred_at_ms` is a
/// `long` (R8.12 puts millisecond precision at the proto layer, because Athena
/// reads milliseconds), and **there is no partition-spec evolution in
/// iceberg-rust** — `Transaction` exposes `update_schema`, `replace_sort_order`,
/// and property/location/statistics updates, and nothing that adds, drops, or
/// re-binds a partition field. So the spec chosen here is the spec the table
/// has forever, and getting it wrong means rebuilding the table.
///
/// Field ids are assigned here only to satisfy the schema builder's uniqueness
/// check. `TableMetadataBuilder::new` reassigns every one of them from
/// `FIRST_FIELD_ID` and remaps the partition spec and sort order along with
/// them, so the values below never reach the table.
pub fn initial_schema(columns: &[DerivedColumn]) -> Result<Schema, ServiceError> {
    let plan = plan_additions(&BTreeSet::new(), columns)?;

    let mut next_id = 0;
    let mut fields: Vec<NestedFieldRef> = Vec::with_capacity(plan.len() + 1);

    next_id += 1;
    let mut occurred_at = NestedField::optional(
        next_id,
        OCCURRED_AT_COLUMN,
        Type::Primitive(PrimitiveType::Timestamptz),
    );
    occurred_at.doc = Some(OCCURRED_AT_DOC.to_owned());
    fields.push(Arc::new(occurred_at));

    for add in plan {
        next_id += 1;
        let id = next_id;
        let field_type = assign_field_ids(&add.field_type, &mut next_id);
        let mut field = NestedField::optional(id, add.name, field_type);
        field.doc = Some(add.doc);
        fields.push(Arc::new(field));
    }

    Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(|e| ServiceError::failed(format!("building initial schema: {e}")))
}

/// Rewrites a planned type's nested field ids from a single running counter.
///
/// [`plan_additions`] restarts its counter per column, which is correct for
/// `add_column` (Iceberg reassigns ids there) but produces duplicates across
/// columns, and a schema with duplicate ids does not build.
fn assign_field_ids(field_type: &Type, next_id: &mut i32) -> Type {
    let Type::Struct(struct_type) = field_type else {
        return field_type.clone();
    };
    let fields: Vec<NestedFieldRef> = struct_type
        .fields()
        .iter()
        .map(|field| {
            *next_id += 1;
            let id = *next_id;
            let inner = assign_field_ids(&field.field_type, next_id);
            let mut fresh = NestedField::optional(id, field.name.clone(), inner);
            fresh.doc = field.doc.clone();
            Arc::new(fresh)
        })
        .collect();
    Type::Struct(StructType::new(fields))
}

/// `day(occurred_at)` and `region`, per R8.10.
///
/// Hidden partitioning, with no hand-rolled `dt` string column: a predicate on
/// `occurred_at` alone prunes correctly, and a redundant string column would be
/// a second thing to keep honest across a replay.
///
/// `region` is an identity partition sharing its name with the schema column it
/// comes from, which Iceberg permits **only** for the identity transform — the
/// day field therefore has to be named [`OCCURRED_AT_DAY_PARTITION`] rather
/// than `occurred_at`.
pub fn partition_spec(schema: &Schema) -> Result<PartitionSpec, ServiceError> {
    PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .add_partition_field(
            OCCURRED_AT_COLUMN,
            OCCURRED_AT_DAY_PARTITION,
            Transform::Day,
        )
        .and_then(|builder| {
            builder.add_partition_field(REGION_COLUMN, REGION_COLUMN, Transform::Identity)
        })
        .and_then(|builder| builder.build())
        .map_err(|e| ServiceError::failed(format!("building partition spec: {e}")))
}

/// `(event_name, occurred_at_ms)`, per R8.10.
///
/// Nulls last in both positions: a null event name is a corrupt row and belongs
/// at the end of a file, not at the front of every scan.
pub fn sort_order(schema: &Schema) -> Result<SortOrder, ServiceError> {
    let mut builder = SortOrder::builder();
    for name in [EVENT_NAME_COLUMN, OCCURRED_AT_MS_COLUMN] {
        let field = schema.field_by_name(name).ok_or_else(|| {
            ServiceError::failed(format!("sort column `{name}` is missing from the schema"))
        })?;
        builder.with_sort_field(
            SortField::builder()
                .source_id(field.id)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::Last)
                .build(),
        );
    }
    builder
        .build(schema)
        .map_err(|e| ServiceError::failed(format!("building sort order: {e}")))
}

// ---------------------------------------------------------------------------
// The data append
// ---------------------------------------------------------------------------

/// Encodes NDJSON into Parquet and returns the data files, WITHOUT committing.
///
/// The whole encode runs on `spawn_blocking`. This is not defensive: the
/// committer is elected onto a task that is also serving games on a 100 ms tick,
/// Parquet encoding is CPU-bound with no yield points, and an hour's events
/// encoded on a runtime worker would stall every game that worker owns. The
/// async writer is driven with `Handle::block_on` from inside the blocking
/// thread, so the I/O still uses the shared runtime while the CPU stays off it.
async fn write_data_files(table: &Table, ndjson: &str) -> Result<Vec<DataFile>, ServiceError> {
    let table = table.clone();
    let ndjson = ndjson.to_owned();
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || handle.block_on(encode_data_files(table, ndjson)))
        .await
        .map_err(|e| ServiceError::failed(format!("parquet encode task: {e}")))?
}

async fn encode_data_files(table: Table, ndjson: String) -> Result<Vec<DataFile>, ServiceError> {
    let schema = table.metadata().current_schema().clone();
    let Some(batch) = ndjson_to_record_batch(&schema, &ndjson)? else {
        // No rows means no file. An empty Parquet file would still cost a
        // manifest entry for the life of the table.
        return Ok(Vec::new());
    };

    let location_generator = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| ServiceError::failed(format!("data file location: {e}")))?;
    // A per-commit uuid suffix: two committers overlapping across a lease
    // handover must never generate the same object name, because a conditional
    // overwrite is not available here and a collision would lose data.
    let file_name_generator = DefaultFileNameGenerator::new(
        "events".to_owned(),
        Some(uuid::Uuid::now_v7().to_string()),
        DataFileFormat::Parquet,
    );
    let table_properties = table
        .metadata()
        .table_properties()
        .map_err(|e| ServiceError::failed(format!("reading table properties: {e}")))?;
    let parquet_writer_builder =
        ParquetWriterBuilder::from_table_properties(&table_properties, schema.clone());
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let spec = table.metadata().default_partition_spec().clone();
    if spec.is_unpartitioned() {
        let mut writer = data_file_writer_builder
            .build(None)
            .await
            .map_err(|e| ServiceError::failed(format!("building data file writer: {e}")))?;
        writer
            .write(batch)
            .await
            .map_err(|e| ServiceError::failed(format!("writing parquet: {e}")))?;
        return writer
            .close()
            .await
            .map_err(|e| ServiceError::failed(format!("closing parquet writer: {e}")));
    }

    // Fanout, not clustered: the source objects are in emission order, not
    // partition order, and a clustered writer would silently produce one file
    // per partition CHANGE rather than one per partition.
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec)
        .map_err(|e| ServiceError::failed(format!("building partition splitter: {e}")))?;
    let partitioned = splitter
        .split(&batch)
        .map_err(|e| ServiceError::failed(format!("computing partition values: {e}")))?;

    let mut writer = FanoutWriter::new(data_file_writer_builder);
    for (partition_key, partition_batch) in partitioned {
        writer
            .write(partition_key, partition_batch)
            .await
            .map_err(|e| ServiceError::failed(format!("writing parquet: {e}")))?;
    }
    PartitioningWriter::close(writer)
        .await
        .map_err(|e| ServiceError::failed(format!("closing parquet writers: {e}")))
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

#[async_trait]
impl IcebergCatalog for S3TablesIcebergCatalog {
    async fn ensure_table(
        &self,
        table: &str,
        columns: &[DerivedColumn],
    ) -> Result<(), ServiceError> {
        // The namespace check comes FIRST because `table_exists` against a
        // missing namespace is an ERROR, not a `false`, in both the memory
        // catalog and S3 Tables — asking the questions in the other order turns
        // a fresh environment into a hard failure.
        self.ensure_namespace().await?;

        let ident = self.ident(table);
        if self
            .catalog
            .table_exists(&ident)
            .await
            .map_err(|e| ServiceError::failed(format!("checking table {table}: {e}")))?
        {
            // Load rather than return early: `load` is where the
            // format-version check lives, and a v3 table has to be caught
            // before anything is written into it.
            self.load(table).await?;
            return Ok(());
        }

        let schema = initial_schema(columns)?;
        let spec = partition_spec(&schema)?;
        let order = sort_order(&schema)?;
        let creation = TableCreation::builder()
            .name(table.to_owned())
            .schema(schema)
            .partition_spec(spec.into_unbound())
            .sort_order(order)
            // Explicit, not defaulted. Athena implements Iceberg spec 1.4.2 and
            // reads v2 only, and a v3 table would work everywhere in Rust and
            // fail only at query time.
            .format_version(FormatVersion::V2)
            .build();

        match self.catalog.create_table(&self.namespace, creation).await {
            Ok(_) => {
                info!("created analytics table {table} (format-version 2, day+region partitioned)");
                Ok(())
            }
            Err(error) => {
                // Two committers can race the create exactly once: at the
                // moment a dataset first appears, before any lease exists for
                // its table. Losing that race means the table is there, which
                // is the outcome asked for.
                if self.catalog.table_exists(&ident).await.unwrap_or(false) {
                    warn!("lost the create race for {table}; using the existing table");
                    return Ok(());
                }
                Err(ServiceError::failed(format!(
                    "creating table {table}: {error}"
                )))
            }
        }
    }

    async fn current_columns(&self, table: &str) -> Result<Vec<String>, ServiceError> {
        let loaded = self.load(table).await?;
        Ok(schema_column_names(loaded.metadata().current_schema()))
    }

    async fn add_columns(
        &self,
        table: &str,
        columns: &[DerivedColumn],
    ) -> Result<(), ServiceError> {
        if columns.is_empty() {
            return Ok(());
        }

        let loaded = self.load(table).await?;
        let existing: BTreeSet<String> = schema_column_names(loaded.metadata().current_schema())
            .into_iter()
            .collect();
        let plan = plan_additions(&existing, columns)?;

        let tx = Transaction::new(&loaded);
        let mut action = tx.update_schema();
        for add in &plan {
            action = action.add_column(planned_to_add_column(add));
        }
        let tx = action
            .apply(tx)
            .map_err(|e| ServiceError::failed(format!("planning schema update: {e}")))?;

        // Schema evolution commits ALONE, before any append. A transaction
        // carrying both would make the append depend on a schema the table
        // does not yet have.
        tx.commit(self.catalog.as_ref())
            .await
            .map_err(|e| ServiceError::failed(format!("committing schema update: {e}")))?;

        info!(
            "analytics table {table} gained {} column add(s)",
            plan.len()
        );
        Ok(())
    }

    async fn commit(
        &self,
        table: &str,
        source_key: &str,
        epoch: u64,
        rows: &str,
    ) -> Result<CommitOutcome, ServiceError> {
        let loaded = self.load(table).await?;
        let properties = loaded.metadata().properties();

        if is_fenced(read_epoch(properties)?, epoch) {
            return Ok(CommitOutcome::Fenced);
        }
        if already_folded(
            properties.get(HIGH_WATER_MARK_PROPERTY).map(String::as_str),
            source_key,
        ) {
            return Ok(CommitOutcome::AlreadyPresent);
        }

        // Encoded BEFORE the transaction is opened. The data files are just
        // objects until a snapshot references them, so an encode that fails
        // here leaves the table untouched, and one that succeeds without a
        // commit leaves orphans that S3 Tables' managed cleanup removes.
        let data_files = write_data_files(&loaded, rows).await?;
        let row_count: u64 = data_files.iter().map(|file| file.record_count()).sum();

        let tx = Transaction::new(&loaded);
        let action = tx
            .update_table_properties()
            .set(EPOCH_PROPERTY.to_owned(), epoch.to_string())
            .set(HIGH_WATER_MARK_PROPERTY.to_owned(), source_key.to_owned());
        let tx = action
            .apply(tx)
            .map_err(|e| ServiceError::failed(format!("planning commit: {e}")))?;

        // The append and the high-water mark move in ONE transaction. Split
        // across two, a crash between them either re-folds the object (adding
        // duplicate rows) or advances the mark past data that was never
        // appended (losing it silently).
        let tx = if data_files.is_empty() {
            tx
        } else {
            let append = tx.fast_append().add_data_files(data_files);
            append
                .apply(tx)
                .map_err(|e| ServiceError::failed(format!("planning append: {e}")))?
        };

        match tx.commit(self.catalog.as_ref()).await {
            Ok(_) => {
                info!("folded {source_key} into {table}: {row_count} row(s)");
                Ok(CommitOutcome::Committed)
            }
            Err(error) => {
                // S3 Tables gives the commit a version token, so a lost race
                // fails here rather than clobbering. Re-read before reporting:
                // if the winner holds a higher epoch, this committer was
                // fenced during the window and must stand down rather than
                // treat it as a transient error and retry forever.
                let stored = self
                    .load(table)
                    .await
                    .ok()
                    .and_then(|reloaded| read_epoch(reloaded.metadata().properties()).ok())
                    .flatten();
                if is_fenced(stored, epoch) {
                    warn!("analytics commit at epoch {epoch} lost to a newer epoch");
                    return Ok(CommitOutcome::Fenced);
                }
                Err(ServiceError::failed(format!(
                    "committing {source_key} to {table}: {error}"
                )))
            }
        }
    }

    async fn high_water_mark(&self, table: &str) -> Result<Option<String>, ServiceError> {
        let loaded = self.load(table).await?;
        Ok(loaded
            .metadata()
            .properties()
            .get(HIGH_WATER_MARK_PROPERTY)
            .cloned())
    }
}

/// Builds the `AddColumn` for one planned add.
///
/// The TypedBuilder form is the real API. `AddColumn::optional(..).with_parent(..)`
/// appears throughout the crate's doc comments — and in an error message that
/// misdirects callers toward it — but `with_parent` does not exist in 0.10.1.
fn planned_to_add_column(add: &PlannedAdd) -> AddColumn {
    match &add.parent {
        Some(parent) => AddColumn::builder()
            .name(add.name.clone())
            .field_type(add.field_type.clone())
            .doc(add.doc.clone())
            .parent(parent.clone())
            .build(),
        None => AddColumn::builder()
            .name(add.name.clone())
            .field_type(add.field_type.clone())
            .doc(add.doc.clone())
            .build(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::schema::{derive_columns, descriptor_pool, event_descriptor};

    fn column(name: &str, path: &str, type_name: &str) -> DerivedColumn {
        DerivedColumn {
            name: name.to_owned(),
            proto_path: path.to_owned(),
            type_name: type_name.to_owned(),
            optional: true,
        }
    }

    fn existing(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    // -- Type mapping --------------------------------------------------

    /// The pre-widening in `schema.rs` exists because there is no
    /// type-promotion API; this pins that it survives the mapping intact.
    #[test]
    fn the_prewidened_types_map_without_narrowing() {
        assert_eq!(
            iceberg_type("long").unwrap(),
            Type::Primitive(PrimitiveType::Long)
        );
        assert_eq!(
            iceberg_type("decimal(20,0)").unwrap(),
            Type::Primitive(PrimitiveType::Decimal {
                precision: 20,
                scale: 0
            })
        );
        assert_eq!(
            iceberg_type("timestamptz").unwrap(),
            Type::Primitive(PrimitiveType::Timestamptz)
        );
        assert_eq!(
            iceberg_type("boolean").unwrap(),
            Type::Primitive(PrimitiveType::Boolean)
        );
        assert_eq!(
            iceberg_type("binary").unwrap(),
            Type::Primitive(PrimitiveType::Binary)
        );
        // Enums arrive already flattened to their value name.
        assert_eq!(
            iceberg_type("string").unwrap(),
            Type::Primitive(PrimitiveType::String)
        );
    }

    /// Every type `schema.rs` can produce must be mappable. A gap here would
    /// otherwise appear as a runtime failure the first time a proto used it.
    #[test]
    fn every_derived_type_in_the_real_protos_is_mappable() {
        let pool = descriptor_pool().unwrap();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        assert!(!columns.is_empty());
        for column in &columns {
            iceberg_type(&column.type_name).unwrap_or_else(|e| {
                panic!(
                    "{} has unmappable type {}: {e}",
                    column.name, column.type_name
                )
            });
        }
    }

    /// An unknown type must fail loudly. Falling back to `string` would make
    /// the mistake discoverable only once the data was unqueryable.
    #[test]
    fn an_unrecognized_type_is_rejected_rather_than_guessed() {
        assert!(iceberg_type("uint").is_err());
        assert!(iceberg_type("decimal(20)").is_err());
        assert!(iceberg_type("decimal(0,0)").is_err());
        assert!(iceberg_type("decimal(39,0)").is_err());
        assert!(iceberg_type("decimal(2,5)").is_err());
    }

    // -- Doc strings ---------------------------------------------------

    #[test]
    fn the_doc_string_carries_the_proto_number_path() {
        assert_eq!(doc_for(&column("event_id", "1", "string")), "pb:1");
        assert_eq!(
            doc_for(&column("identity.user_id", "9.1", "long")),
            "pb:9.1"
        );
    }

    /// The doc string is what makes the table metadata its own registry, so
    /// every planned add must carry one — including fields buried inside a
    /// newly added struct.
    #[test]
    fn every_planned_field_carries_its_proto_path_including_nested_ones() {
        let missing = vec![
            column("identity", "9", "struct"),
            column("identity.user_id", "9.1", "long"),
        ];
        let plan = plan_additions(&existing(&[]), &missing).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].doc, "pb:9");

        let Type::Struct(fields) = &plan[0].field_type else {
            panic!("identity must be a struct");
        };
        assert_eq!(fields.fields().len(), 1);
        assert_eq!(fields.fields()[0].name, "user_id");
        assert_eq!(fields.fields()[0].doc.as_deref(), Some("pb:9.1"));
    }

    // -- Column diff and planning --------------------------------------

    /// A brand-new struct and its children cannot be separate adds: a parent
    /// path resolves against the schema the transaction started from, so the
    /// child's add would fail with "parent not found". One add, whole type.
    #[test]
    fn a_wholly_new_struct_becomes_one_add_carrying_its_children() {
        let missing = vec![
            column("identity", "9", "struct"),
            column("identity.user_id", "9.1", "long"),
            column("identity.guest", "9.2", "boolean"),
        ];
        let plan = plan_additions(&existing(&[]), &missing).unwrap();

        assert_eq!(plan.len(), 1, "one add, not three");
        assert_eq!(plan[0].name, "identity");
        assert_eq!(plan[0].parent, None);
        let Type::Struct(fields) = &plan[0].field_type else {
            panic!("must be a struct");
        };
        let names: Vec<&str> = fields.fields().iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["guest", "user_id"]);
    }

    /// Adding into a struct that already exists is a pure add with a parent —
    /// never a drop and re-add, which would orphan the existing data.
    #[test]
    fn a_new_field_under_an_existing_struct_is_a_parented_add() {
        let missing = vec![column("identity.user_id", "9.1", "long")];
        let plan = plan_additions(&existing(&["identity"]), &missing).unwrap();

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].parent.as_deref(), Some("identity"));
        assert_eq!(plan[0].name, "user_id");
        assert_eq!(plan[0].field_type, Type::Primitive(PrimitiveType::Long));
    }

    #[test]
    fn a_new_struct_under_an_existing_struct_carries_its_own_children() {
        let missing = vec![
            column("identity.origin", "9.4", "struct"),
            column("identity.origin.region", "9.4.1", "string"),
        ];
        let plan = plan_additions(&existing(&["identity"]), &missing).unwrap();

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].parent.as_deref(), Some("identity"));
        assert_eq!(plan[0].name, "origin");
        let Type::Struct(fields) = &plan[0].field_type else {
            panic!("must be a struct");
        };
        assert_eq!(fields.fields().len(), 1);
        assert_eq!(fields.fields()[0].name, "region");
    }

    #[test]
    fn a_top_level_scalar_is_an_unparented_add() {
        let plan =
            plan_additions(&existing(&[]), &[column("payload_case", "7", "string")]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].parent, None);
        assert_eq!(plan[0].name, "payload_case");
    }

    #[test]
    fn nothing_missing_plans_nothing() {
        assert!(
            plan_additions(&existing(&["event_id"]), &[])
                .unwrap()
                .is_empty()
        );
    }

    /// Deeply nested adds must collapse to the shallowest missing ancestor,
    /// not one add per level.
    #[test]
    fn planning_collapses_to_the_shallowest_missing_ancestor() {
        let missing = vec![
            column("a", "1", "struct"),
            column("a.b", "1.2", "struct"),
            column("a.b.c", "1.2.3", "long"),
        ];
        let plan = plan_additions(&existing(&[]), &missing).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].name, "a");

        let plan = plan_additions(&existing(&["a"]), &missing[1..]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].parent.as_deref(), Some("a"));
        assert_eq!(plan[0].name, "b");

        let plan = plan_additions(&existing(&["a", "a.b"]), &missing[2..]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].parent.as_deref(), Some("a.b"));
        assert_eq!(plan[0].name, "c");
    }

    /// A child without its parent struct is not expressible as an add, so it
    /// must be refused rather than issued and rejected by the service.
    #[test]
    fn a_child_whose_ancestor_was_not_derived_is_refused() {
        let missing = vec![column("identity.user_id", "9.1", "long")];
        let error = plan_additions(&existing(&[]), &missing)
            .expect_err("an orphaned child cannot be added");
        assert!(error.to_string().contains("identity"), "{error}");
    }

    /// An empty proto message is a oneof arm whose only information is that it
    /// happened. It must become a boolean, not an empty struct: an empty
    /// Iceberg struct is a childless Parquet group, which writers refuse to
    /// emit and Athena refuses to read.
    #[test]
    fn an_empty_proto_message_becomes_a_boolean_presence_column() {
        let plan =
            plan_additions(&existing(&[]), &[column("user_login", "103", "struct")]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].name, "user_login");
        assert_eq!(plan[0].field_type, Type::Primitive(PrimitiveType::Boolean));
        assert_eq!(plan[0].doc, "pb:103");
    }

    /// The same rule inside a struct: an empty message nested under a parent
    /// is still one bit, not a childless group.
    #[test]
    fn an_empty_message_nested_in_a_struct_is_also_a_boolean() {
        let missing = vec![
            column("payload", "9", "struct"),
            column("payload.user_login", "9.103", "struct"),
        ];
        let plan = plan_additions(&existing(&[]), &missing).unwrap();
        let Type::Struct(fields) = &plan[0].field_type else {
            panic!("payload must be a struct");
        };
        assert_eq!(
            fields.fields()[0].field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Boolean)
        );
    }

    /// The boolean mapping above is only unambiguous because these protos have
    /// no maps: `derive_columns` types a map as a childless `struct` too, and a
    /// boolean would be wrong for one. If a map is ever introduced, this fails
    /// and the mapping must be revisited before the column reaches a table.
    #[test]
    fn no_map_is_used_in_the_analytics_protos() {
        let pool = descriptor_pool().unwrap();
        for message in pool.all_messages() {
            if !message.full_name().starts_with("snaketron.analytics.v1") {
                continue;
            }
            for field in message.fields() {
                assert!(
                    !field.is_map(),
                    "{}.{} is a map; a childless struct would no longer unambiguously \
                     mean an empty message",
                    message.full_name(),
                    field.name()
                );
            }
        }
    }

    /// The whole real schema must plan against an empty table, and each add
    /// must be a distinct top-level column.
    #[test]
    fn the_full_derived_schema_plans_against_an_empty_table() {
        let pool = descriptor_pool().unwrap();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        let plan = plan_additions(&existing(&[]), &columns).unwrap();

        assert!(!plan.is_empty());
        assert!(
            plan.iter().all(|add| add.parent.is_none()),
            "against an empty table every add is top-level"
        );
        let top_level: BTreeSet<&str> = columns
            .iter()
            .filter(|c| !c.name.contains('.'))
            .map(|c| c.name.as_str())
            .collect();
        let planned: BTreeSet<&str> = plan.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(planned, top_level);
    }

    /// Planning must be deterministic: two tasks on the same binary compute a
    /// byte-identical plan, which is what makes evolution converge.
    #[test]
    fn planning_is_deterministic() {
        let pool = descriptor_pool().unwrap();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        let first = plan_additions(&existing(&[]), &columns).unwrap();
        let second = plan_additions(&existing(&[]), &columns).unwrap();
        assert_eq!(first, second);
    }

    // -- Reading the current schema ------------------------------------

    /// The flattening must mirror `derive_columns` exactly, or the name-matched
    /// diff would re-add columns that are already there.
    #[test]
    fn schema_names_flatten_the_same_way_the_derivation_does() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "event_id",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "identity",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::optional(
                            3,
                            "user_id",
                            Type::Primitive(PrimitiveType::Long),
                        )),
                        Arc::new(NestedField::optional(
                            4,
                            "origin",
                            Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                                5,
                                "region",
                                Type::Primitive(PrimitiveType::String),
                            ))])),
                        )),
                    ])),
                )),
            ])
            .build()
            .unwrap();

        assert_eq!(
            schema_column_names(&schema),
            vec![
                "event_id".to_owned(),
                "identity".to_owned(),
                "identity.user_id".to_owned(),
                "identity.origin".to_owned(),
                "identity.origin.region".to_owned(),
            ]
        );
    }

    /// The steady state: a table already carrying the full schema plans
    /// nothing, so no DDL is issued and no snapshot is appended.
    #[test]
    fn a_table_already_carrying_every_column_plans_no_adds() {
        let pool = descriptor_pool().unwrap();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        let present: BTreeSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let missing: Vec<DerivedColumn> = columns
            .into_iter()
            .filter(|c| !present.contains(&c.name))
            .collect();
        assert!(missing.is_empty());
        assert!(plan_additions(&present, &missing).unwrap().is_empty());
    }

    // -- Fencing -------------------------------------------------------

    /// The fencing property. A strictly higher stored epoch means a newer
    /// committer owns the table; equality is this same holder resuming.
    #[test]
    fn only_a_strictly_higher_stored_epoch_fences() {
        assert!(!is_fenced(None, 1), "an unclaimed table fences nobody");
        assert!(!is_fenced(Some(3), 3), "the holder resumes");
        assert!(!is_fenced(Some(2), 3), "a newer holder proceeds");
        assert!(is_fenced(Some(9), 3), "a stale holder is fenced");
        assert!(is_fenced(Some(u64::MAX), 0));
    }

    #[test]
    fn the_stored_epoch_is_read_from_table_properties() {
        let mut properties = HashMap::new();
        assert_eq!(read_epoch(&properties).unwrap(), None);

        properties.insert(EPOCH_PROPERTY.to_owned(), "42".to_owned());
        assert_eq!(read_epoch(&properties).unwrap(), Some(42));
    }

    /// A malformed epoch must not read as "no epoch" — that would silently
    /// disable the fence, which is the one thing keeping metadata bounded.
    #[test]
    fn a_malformed_epoch_is_an_error_not_an_absent_fence() {
        let properties = HashMap::from([(EPOCH_PROPERTY.to_owned(), "nine".to_owned())]);
        let error = read_epoch(&properties).expect_err("garbage must not disable the fence");
        assert!(error.to_string().contains(EPOCH_PROPERTY), "{error}");
    }

    // -- Resume --------------------------------------------------------

    /// Keys sort chronologically within a prefix, so the mark is the whole
    /// resume story: at or below it is already folded.
    #[test]
    fn the_high_water_mark_decides_what_is_already_folded() {
        assert!(!already_folded(None, "a.json.gz"));
        assert!(already_folded(Some("b.json.gz"), "a.json.gz"));
        assert!(
            already_folded(Some("b.json.gz"), "b.json.gz"),
            "the mark itself is folded"
        );
        assert!(!already_folded(Some("b.json.gz"), "c.json.gz"));
    }

    // -- Format version ------------------------------------------------

    /// Athena reads v2 only, and a v3 table would fail nowhere in Rust — the
    /// loss would surface as Athena refusing to query, far from the cause.
    #[test]
    fn only_format_version_two_is_accepted() {
        assert!(require_v2("game_events", FormatVersion::V2).is_ok());
        let error = require_v2("game_events", FormatVersion::V3).expect_err("v3 evicts Athena");
        assert!(error.to_string().contains("v2 only"), "{error}");
        assert!(require_v2("game_events", FormatVersion::V1).is_err());
    }

    // -- Path splitting ------------------------------------------------

    #[test]
    fn dotted_names_split_into_parent_and_leaf() {
        assert_eq!(split_path("event_id"), (None, "event_id"));
        assert_eq!(
            split_path("identity.user_id"),
            (Some("identity"), "user_id")
        );
        assert_eq!(
            split_path("a.b.c"),
            (Some("a.b"), "c"),
            "the parent is everything but the last segment"
        );
    }
}
