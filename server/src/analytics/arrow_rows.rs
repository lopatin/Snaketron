//! NDJSON -> Arrow, the first half of the Iceberg write path.
//!
//! The raw tier holds **proto3 canonical JSON**, one event per line, exactly as
//! `event::to_json_line` wrote it. Three properties of that encoding decide
//! everything here:
//!
//! 1. **64-bit integers are QUOTED.** `"occurred_at_ms": "1755561600000"` is
//!    the canonical form, not a bug. Every 64-bit column therefore has to
//!    accept a JSON string, and a reader that only calls `as_i64()` silently
//!    nulls the entire column. Numbers are accepted too, because a hand-written
//!    fixture or a future writer may emit them bare.
//! 2. **An unset oneof arm is ABSENT**, not null and not `{}`. Presence in the
//!    JSON object is what distinguishes "this event is a `user_login`" from
//!    "this event is something else".
//! 3. **An empty proto message carries one bit.** `message UserLogin {}` maps
//!    to an Iceberg `boolean` (see `iceberg_catalog::build_type_inner`), so the
//!    JSON value for it is `{}` and the column value is `true`.
//!
//! The output batch's Arrow schema is derived from the table's *current*
//! Iceberg schema via [`schema_to_arrow_schema`], never hand-built: the Parquet
//! writer re-derives the same schema internally and rejects a batch whose
//! schema differs by so much as a timezone string. Deriving it from the same
//! function is what makes the two agree by construction rather than by luck.
//!
//! Columns the table has but the JSON does not are nulls, which is precisely
//! how a schema addition reads against older data. Fields the JSON has but the
//! table does not are dropped — the schema reconciliation that runs before every
//! fold is what adds them, and dropping here means a fold that races an
//! addition loses a field rather than failing.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Fields, TimeUnit};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use serde_json::Value;
use snaketron_service_api::ServiceError;

/// The synthetic event-time column the table is partitioned by.
///
/// It is NOT a proto field. The protos carry `occurred_at_ms` as an `int64`
/// because Athena reads millisecond precision (R8.12), but Iceberg's `day()`
/// transform is only defined on a date or timestamp, and there is no partition
/// spec evolution in iceberg-rust — so a `long` here would be a permanent
/// mistake. The column is derived from `occurred_at_ms` on the way in and is
/// never emitted by the writer.
pub const OCCURRED_AT_COLUMN: &str = "occurred_at";

/// The proto field the partition column is derived from.
pub const OCCURRED_AT_MS_COLUMN: &str = "occurred_at_ms";

/// Parses NDJSON into a batch matching `schema`.
///
/// Returns `None` for input with no rows, which is not an error: an empty
/// source object must produce no data file rather than an empty one, because a
/// zero-row Parquet file still costs a manifest entry forever.
///
/// This is pure CPU with no I/O, which is what lets the caller push the whole
/// encode onto `spawn_blocking` and keep it off the game loop's runtime.
pub fn ndjson_to_record_batch(
    schema: &IcebergSchema,
    ndjson: &str,
) -> Result<Option<RecordBatch>, ServiceError> {
    let mut rows = Vec::new();
    for (index, line) in ndjson.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let mut row: Value = serde_json::from_str(line)
            .map_err(|e| ServiceError::failed(format!("line {} is not JSON: {e}", index + 1)))?;
        if !row.is_object() {
            return Err(ServiceError::failed(format!(
                "line {} is a JSON {}, not an object",
                index + 1,
                kind_of(&row)
            )));
        }
        derive_occurred_at(&mut row);
        rows.push(row);
    }
    if rows.is_empty() {
        return Ok(None);
    }

    let arrow_schema = Arc::new(
        schema_to_arrow_schema(schema)
            .map_err(|e| ServiceError::failed(format!("deriving Arrow schema: {e}")))?,
    );
    let columns = struct_columns(
        schema.as_struct().fields(),
        arrow_schema.fields(),
        &rows.iter().map(present).collect::<Vec<_>>(),
    )?;

    RecordBatch::try_new(arrow_schema, columns)
        .map(Some)
        .map_err(|e| ServiceError::failed(format!("building record batch: {e}")))
}

/// Stamps the derived partition column onto a parsed row.
///
/// Written as microseconds since the epoch because that is Iceberg's storage
/// unit for `timestamptz`; the millisecond precision of the source survives
/// exactly, since the conversion is a multiplication by 1000 and never a
/// rounding.
fn derive_occurred_at(row: &mut Value) {
    let Some(object) = row.as_object_mut() else {
        return;
    };
    if object.contains_key(OCCURRED_AT_COLUMN) {
        return;
    }
    let Some(millis) = object.get(OCCURRED_AT_MS_COLUMN).and_then(json_i64) else {
        return;
    };
    let Some(micros) = millis.checked_mul(1_000) else {
        return;
    };
    object.insert(OCCURRED_AT_COLUMN.to_owned(), Value::from(micros));
}

/// A row's value for a field, with JSON `null` collapsed into absence.
///
/// Iceberg has one null, proto3 JSON has two spellings of it (absent, and an
/// explicit `null` for an unset message), and keeping them distinct here would
/// buy nothing while costing a branch at every leaf.
fn present(value: &Value) -> Option<&Value> {
    (!value.is_null()).then_some(value)
}

fn struct_columns(
    fields: &[iceberg::spec::NestedFieldRef],
    arrow_fields: &Fields,
    rows: &[Option<&Value>],
) -> Result<Vec<ArrayRef>, ServiceError> {
    if fields.len() != arrow_fields.len() {
        return Err(ServiceError::failed(format!(
            "Arrow schema has {} field(s) but the Iceberg schema has {}",
            arrow_fields.len(),
            fields.len()
        )));
    }

    let mut columns = Vec::with_capacity(fields.len());
    for (field, arrow_field) in fields.iter().zip(arrow_fields.iter()) {
        let values: Vec<Option<&Value>> = rows
            .iter()
            .map(|row| row.and_then(|row| row.get(&field.name)).and_then(present))
            .collect();
        columns.push(
            build_array(&field.field_type, arrow_field.data_type(), &values)
                .map_err(|e| ServiceError::failed(format!("column `{}`: {e}", field.name)))?,
        );
    }
    Ok(columns)
}

fn build_array(
    field_type: &Type,
    data_type: &DataType,
    values: &[Option<&Value>],
) -> Result<ArrayRef, ServiceError> {
    match field_type {
        Type::Primitive(primitive) => build_primitive(primitive, data_type, values),
        Type::Struct(struct_type) => {
            let DataType::Struct(arrow_fields) = data_type else {
                return Err(ServiceError::failed(format!(
                    "Iceberg struct maps to Arrow {data_type}, which is not a struct"
                )));
            };
            let children = struct_columns(struct_type.fields(), arrow_fields, values)?;
            // The struct's own null mask: "the submessage is absent" is a fact
            // proto has and a flattened encoding would destroy, so it is
            // carried explicitly rather than inferred from all-null children.
            let nulls = NullBuffer::from_iter(values.iter().map(Option::is_some));
            Ok(Arc::new(StructArray::new(
                arrow_fields.clone(),
                children,
                Some(nulls),
            )))
        }
        // `derive_columns` emits neither, and `no_map_is_used_in_the_analytics_protos`
        // pins that. Refusing loudly is what keeps that pin meaningful.
        other => Err(ServiceError::failed(format!(
            "unsupported Iceberg type in the write path: {other}"
        ))),
    }
}

fn build_primitive(
    primitive: &PrimitiveType,
    data_type: &DataType,
    values: &[Option<&Value>],
) -> Result<ArrayRef, ServiceError> {
    match primitive {
        PrimitiveType::Boolean => {
            let cells = map_cells(values, json_bool, "boolean")?;
            Ok(Arc::new(BooleanArray::from(cells)))
        }
        PrimitiveType::Int => {
            let cells = map_cells(values, json_i32, "int")?;
            Ok(Arc::new(Int32Array::from(cells)))
        }
        PrimitiveType::Long => {
            let cells = map_cells(values, json_i64, "long")?;
            Ok(Arc::new(Int64Array::from(cells)))
        }
        PrimitiveType::Float => {
            let cells = map_cells(values, |v| json_f64(v).map(|f| f as f32), "float")?;
            Ok(Arc::new(Float32Array::from(cells)))
        }
        PrimitiveType::Double => {
            let cells = map_cells(values, json_f64, "double")?;
            Ok(Arc::new(Float64Array::from(cells)))
        }
        PrimitiveType::String => {
            let cells = map_cells(values, json_string, "string")?;
            Ok(Arc::new(StringArray::from(cells)))
        }
        PrimitiveType::Binary => {
            let cells = map_cells(values, json_bytes, "binary")?;
            let refs: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_deref()).collect();
            Ok(Arc::new(LargeBinaryArray::from(refs)))
        }
        PrimitiveType::Date => {
            let cells = map_cells(values, json_date_days, "date")?;
            Ok(Arc::new(Date32Array::from(cells)))
        }
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            let cells = map_cells(values, json_micros, "timestamp")?;
            let array = TimestampMicrosecondArray::from(cells);
            // The timezone is read off the Arrow type rather than assumed:
            // `RecordBatch::try_new` compares `DataType`s including the
            // timezone STRING, so "+00:00" and "UTC" are different types and
            // guessing wrong fails at batch construction.
            let array = match data_type {
                DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => {
                    array.with_timezone(zone.clone())
                }
                _ => array,
            };
            Ok(Arc::new(array))
        }
        PrimitiveType::Decimal { precision, scale } => {
            let cells = map_cells(values, |v| json_decimal(v, *scale), "decimal")?;
            let array = Decimal128Array::from(cells)
                .with_precision_and_scale(*precision as u8, *scale as i8)
                .map_err(|e| ServiceError::failed(format!("decimal({precision},{scale}): {e}")))?;
            Ok(Arc::new(array))
        }
        PrimitiveType::Uuid => {
            let cells = map_cells(values, json_uuid, "uuid")?;
            let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::new(16);
            for cell in &cells {
                match cell {
                    Some(bytes) => builder
                        .append_value(bytes)
                        .map_err(|e| ServiceError::failed(format!("uuid: {e}")))?,
                    None => builder.append_null(),
                }
            }
            let array: FixedSizeBinaryArray = builder.finish();
            Ok(Arc::new(array))
        }
        other => Err(ServiceError::failed(format!(
            "unsupported Iceberg primitive in the write path: {other}"
        ))),
    }
}

/// Applies a leaf decoder across a column, turning a decode failure into a
/// named error rather than a silent null.
///
/// Silent nulls are the failure mode this whole module is written against: a
/// quoted 64-bit integer read with `as_i64()` yields exactly that, and the loss
/// is invisible until someone queries the column months later.
fn map_cells<T>(
    values: &[Option<&Value>],
    decode: impl Fn(&Value) -> Option<T>,
    type_name: &str,
) -> Result<Vec<Option<T>>, ServiceError> {
    values
        .iter()
        .map(|value| match value {
            None => Ok(None),
            Some(value) => decode(value).map(Some).ok_or_else(|| {
                ServiceError::failed(format!(
                    "JSON {} `{}` is not a {type_name}",
                    kind_of(value),
                    truncate(value)
                ))
            }),
        })
        .collect()
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn truncate(value: &Value) -> String {
    let rendered = value.to_string();
    if rendered.len() <= 64 {
        return rendered;
    }
    format!("{}...", &rendered[..64])
}

/// An empty proto message is a `boolean` column whose only content is that it
/// happened, so `{}` decodes to `true`.
fn json_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(b) => Some(*b),
        Value::Object(_) => Some(true),
        _ => None,
    }
}

/// Canonical proto3 JSON quotes 64-bit integers; bare numbers are accepted so a
/// hand-written fixture is not a special case.
fn json_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn json_i32(value: &Value) -> Option<i32> {
    json_i64(value).and_then(|v| i32::try_from(v).ok())
}

fn json_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        // Proto3 JSON spells the non-finite doubles as strings.
        Value::String(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            other => other.trim().parse().ok(),
        },
        _ => None,
    }
}

/// Enums arrive already flattened to their value NAME, so a string is the whole
/// story for every string column.
fn json_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Proto3 JSON encodes `bytes` as standard base64.
fn json_bytes(value: &Value) -> Option<Vec<u8>> {
    value.as_str().and_then(|s| BASE64.decode(s).ok())
}

fn json_date_days(value: &Value) -> Option<i32> {
    let text = value.as_str()?;
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((date - epoch).num_days()).ok()
}

/// Microseconds since the epoch, from either an RFC 3339 string (proto3's
/// `google.protobuf.Timestamp`) or a bare integer (the derived `occurred_at`).
fn json_micros(value: &Value) -> Option<i64> {
    match value {
        Value::Number(_) => json_i64(value),
        Value::String(s) => match s.trim().parse::<i64>() {
            Ok(micros) => Some(micros),
            Err(_) => chrono::DateTime::parse_from_rfc3339(s)
                .ok()?
                .timestamp_micros()
                .into(),
        },
        _ => None,
    }
}

/// `decimal(20,0)` exists for `uint64`, whose full range does not fit a signed
/// `long`. The unscaled value is what Iceberg stores, so a scale of zero means
/// the digits pass through unchanged.
fn json_decimal(value: &Value, scale: u32) -> Option<i128> {
    let text = match value {
        Value::String(s) => s.trim().to_owned(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };

    let (integer, fraction) = match text.split_once('.') {
        Some((integer, fraction)) => (integer.to_owned(), fraction.to_owned()),
        None => (text, String::new()),
    };
    if fraction.len() > scale as usize {
        return None;
    }
    let padded = format!(
        "{integer}{fraction}{}",
        "0".repeat(scale as usize - fraction.len())
    );
    padded.parse().ok()
}

fn json_uuid(value: &Value) -> Option<[u8; 16]> {
    let text = value.as_str()?;
    uuid::Uuid::parse_str(text).ok().map(|u| *u.as_bytes())
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};

    use super::*;

    fn optional(id: i32, name: &str, ty: Type) -> iceberg::spec::NestedFieldRef {
        Arc::new(NestedField::optional(id, name, ty))
    }

    fn primitive(id: i32, name: &str, p: PrimitiveType) -> iceberg::spec::NestedFieldRef {
        optional(id, name, Type::Primitive(p))
    }

    fn schema(fields: Vec<iceberg::spec::NestedFieldRef>) -> Schema {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .expect("schema must build")
    }

    /// The single most important property in this module. Canonical proto3 JSON
    /// quotes every 64-bit integer, and a reader that only accepts numbers
    /// silently nulls the column rather than failing.
    #[test]
    fn quoted_sixty_four_bit_integers_survive_as_longs() {
        let schema = schema(vec![primitive(1, "occurred_at_ms", PrimitiveType::Long)]);
        let batch = ndjson_to_record_batch(&schema, r#"{"occurred_at_ms":"1755561600123"}"#)
            .unwrap()
            .unwrap();
        let column = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(column.value(0), 1_755_561_600_123);
    }

    /// A bare number is not canonical but is unambiguous, so it must not fail.
    #[test]
    fn bare_numbers_are_accepted_for_longs_too() {
        let schema = schema(vec![primitive(1, "n", PrimitiveType::Long)]);
        let batch = ndjson_to_record_batch(&schema, r#"{"n":42}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );
    }

    /// A value that is not a long must be an error, never a null: a silent null
    /// is exactly the failure this module exists to prevent.
    #[test]
    fn an_undecodable_value_is_an_error_not_a_silent_null() {
        let schema = schema(vec![primitive(1, "n", PrimitiveType::Long)]);
        let error = ndjson_to_record_batch(&schema, r#"{"n":{"nested":1}}"#)
            .expect_err("an object is not a long");
        assert!(error.to_string().contains("long"), "{error}");
    }

    /// A column the table has and the JSON lacks reads as null — which is
    /// exactly how an added column reads against older data.
    #[test]
    fn a_column_absent_from_the_json_is_null() {
        let schema = schema(vec![
            primitive(1, "event_id", PrimitiveType::String),
            primitive(2, "added_later", PrimitiveType::String),
        ]);
        let batch = ndjson_to_record_batch(&schema, r#"{"event_id":"a"}"#)
            .unwrap()
            .unwrap();
        assert!(batch.column(1).is_null(0));
    }

    /// A field the JSON has and the table does not is dropped rather than
    /// failing, so a fold that races a schema addition loses a field instead of
    /// the whole object.
    #[test]
    fn a_field_absent_from_the_table_is_dropped() {
        let schema = schema(vec![primitive(1, "event_id", PrimitiveType::String)]);
        let batch = ndjson_to_record_batch(&schema, r#"{"event_id":"a","not_yet":"b"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "a");
    }

    /// An empty proto message maps to a boolean presence flag, so its `{}`
    /// decodes to true and its absence to null.
    #[test]
    fn an_empty_message_object_becomes_a_true_presence_flag() {
        let schema = schema(vec![primitive(1, "user_login", PrimitiveType::Boolean)]);
        let batch = ndjson_to_record_batch(&schema, "{\"user_login\":{}}\n{}")
            .unwrap()
            .unwrap();
        let column = batch.column(0).as_boolean();
        assert!(column.value(0));
        assert!(column.is_null(1));
    }

    /// "Submessage absent" and "submessage present but empty" are different
    /// facts in proto, and nesting exists to preserve the difference.
    #[test]
    fn an_absent_struct_is_null_while_a_present_one_is_not() {
        let schema = schema(vec![optional(
            1,
            "identity",
            Type::Struct(StructType::new(vec![primitive(
                2,
                "user_id",
                PrimitiveType::Long,
            )])),
        )]);
        let batch = ndjson_to_record_batch(&schema, "{\"identity\":{\"user_id\":\"7\"}}\n{}")
            .unwrap()
            .unwrap();
        let identity = batch.column(0).as_struct();
        assert!(identity.is_valid(0));
        assert!(identity.is_null(1));
        assert_eq!(
            identity
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            7
        );
    }

    /// The derived partition column is stamped from `occurred_at_ms`, in
    /// microseconds, with the millisecond precision preserved exactly.
    #[test]
    fn the_partition_timestamp_is_derived_from_occurred_at_ms() {
        let schema = schema(vec![
            primitive(1, OCCURRED_AT_COLUMN, PrimitiveType::Timestamptz),
            primitive(2, OCCURRED_AT_MS_COLUMN, PrimitiveType::Long),
        ]);
        let batch = ndjson_to_record_batch(&schema, r#"{"occurred_at_ms":"1755561600123"}"#)
            .unwrap()
            .unwrap();

        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        let column = batch
            .column(0)
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>();
        assert_eq!(column.value(0), 1_755_561_600_123_000);
    }

    /// A row with no `occurred_at_ms` cannot be assigned an event day, and
    /// inventing one (wall clock, say) would silently misfile it forever.
    #[test]
    fn a_row_without_occurred_at_ms_gets_a_null_partition_timestamp() {
        let schema = schema(vec![primitive(
            1,
            OCCURRED_AT_COLUMN,
            PrimitiveType::Timestamptz,
        )]);
        let batch = ndjson_to_record_batch(&schema, r#"{"event_id":"a"}"#)
            .unwrap()
            .unwrap();
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn empty_input_produces_no_batch() {
        let schema = schema(vec![primitive(1, "n", PrimitiveType::Long)]);
        assert!(ndjson_to_record_batch(&schema, "").unwrap().is_none());
        assert!(ndjson_to_record_batch(&schema, "\n  \n").unwrap().is_none());
    }

    #[test]
    fn a_malformed_line_names_its_line_number() {
        let schema = schema(vec![primitive(1, "n", PrimitiveType::Long)]);
        let error = ndjson_to_record_batch(&schema, "{\"n\":\"1\"}\nnot json")
            .expect_err("malformed JSON must fail");
        assert!(error.to_string().contains("line 2"), "{error}");
    }

    /// `uint64` is pre-widened to `decimal(20,0)` because its full range does
    /// not fit a signed long; the digits must survive that unchanged.
    #[test]
    fn a_uint64_beyond_i64_survives_as_a_decimal() {
        let schema = schema(vec![primitive(
            1,
            "big",
            PrimitiveType::Decimal {
                precision: 20,
                scale: 0,
            },
        )]);
        let batch = ndjson_to_record_batch(&schema, r#"{"big":"18446744073709551615"}"#)
            .unwrap()
            .unwrap();
        let column = batch
            .column(0)
            .as_primitive::<arrow_array::types::Decimal128Type>();
        assert_eq!(column.value(0), 18_446_744_073_709_551_615_i128);
    }

    #[test]
    fn booleans_and_strings_round_trip() {
        let schema = schema(vec![
            primitive(1, "won", PrimitiveType::Boolean),
            primitive(2, "game_type", PrimitiveType::String),
        ]);
        let batch = ndjson_to_record_batch(&schema, r#"{"won":true,"game_type":"ffa"}"#)
            .unwrap()
            .unwrap();
        assert!(batch.column(0).as_boolean().value(0));
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "ffa");
    }
}
