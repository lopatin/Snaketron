//! Schema introspection over the embedded descriptor set.
//!
//! The committer derives an Iceberg schema from this, and CI asserts the
//! evolution rules here so a violation fails at build time rather than
//! silently orphaning a column in production.

use anyhow::{Context, Result};
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};

use super::FILE_DESCRIPTOR_SET;

/// Parses the embedded descriptor set. Cheap enough to call at startup, and
/// deliberately not a global so tests can build their own.
pub fn descriptor_pool() -> Result<DescriptorPool> {
    DescriptorPool::decode(FILE_DESCRIPTOR_SET).context("decoding embedded analytics descriptors")
}

pub fn event_descriptor(pool: &DescriptorPool) -> Result<MessageDescriptor> {
    pool.get_message_by_name("snaketron.analytics.v1.Event")
        .context("analytics Event descriptor missing")
}

/// A column derived from a proto field.
///
/// `proto_path` records the field NUMBER path (e.g. `9.1`), which is written
/// into the Iceberg field's `doc` string. Proto numbers cannot be used as
/// Iceberg field ids — ids must be unique across the whole table while proto
/// numbers are unique only within a message, and lists and maps consume ids
/// that no proto number supplies — so the number is carried as documentation
/// and the id is left to Iceberg. That makes the table metadata its own
/// registry with no external store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedColumn {
    pub name: String,
    pub proto_path: String,
    pub type_name: String,
    /// Always true. Every proto-derived column is optional so a new field can
    /// be added without an initial default, and so proto's zero value stays
    /// distinguishable from absence.
    pub optional: bool,
}

fn scalar_type_name(field: &FieldDescriptor) -> String {
    // Integers are pre-widened because iceberg-rust has no type-promotion API:
    // every promotion that would ever be needed must be designed out now.
    match field.kind() {
        Kind::Bool => "boolean".to_owned(),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => "int".to_owned(),
        Kind::Uint32 | Kind::Fixed32 => "long".to_owned(),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => "long".to_owned(),
        Kind::Uint64 | Kind::Fixed64 => "decimal(20,0)".to_owned(),
        Kind::Float | Kind::Double => "double".to_owned(),
        Kind::String => "string".to_owned(),
        Kind::Bytes => "binary".to_owned(),
        // Value name, not ordinal: adding a variant then needs no schema
        // change, which is the most common proto edit.
        Kind::Enum(_) => "string".to_owned(),
        Kind::Message(_) => "struct".to_owned(),
    }
}

/// Flattens the message into the columns an Iceberg schema would carry.
///
/// Nested messages become structs rather than flattened columns: Parquet fully
/// shreds nested fields so nesting costs nothing, and it preserves the
/// distinction between "submessage absent" and "field unset".
pub fn derive_columns(message: &MessageDescriptor) -> Vec<DerivedColumn> {
    let mut columns = Vec::new();
    collect(message, "", "", &mut columns);
    columns
}

fn collect(
    message: &MessageDescriptor,
    name_prefix: &str,
    path_prefix: &str,
    out: &mut Vec<DerivedColumn>,
) {
    for field in message.fields() {
        let name = if name_prefix.is_empty() {
            field.name().to_owned()
        } else {
            format!("{name_prefix}.{}", field.name())
        };
        let path = if path_prefix.is_empty() {
            field.number().to_string()
        } else {
            format!("{path_prefix}.{}", field.number())
        };

        if let Kind::Message(nested) = field.kind()
            && !field.is_map()
        {
            out.push(DerivedColumn {
                name: name.clone(),
                proto_path: path.clone(),
                type_name: "struct".to_owned(),
                optional: true,
            });
            collect(&nested, &name, &path, out);
            continue;
        }

        out.push(DerivedColumn {
            name,
            proto_path: path,
            type_name: scalar_type_name(&field),
            optional: true,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> DescriptorPool {
        descriptor_pool().expect("descriptors must decode")
    }

    #[test]
    fn the_event_descriptor_is_embedded_and_decodable() {
        let pool = pool();
        let event = event_descriptor(&pool).unwrap();
        assert_eq!(event.full_name(), "snaketron.analytics.v1.Event");
    }

    /// Athena makes a table with any uppercase column invisible to Glue, and
    /// the failure surfaces as a GENERIC_INTERNAL_ERROR far from the cause. So
    /// the check lives here, at build time.
    #[test]
    fn every_field_name_is_lowercase() {
        let pool = pool();
        for message in pool.all_messages() {
            if !message.full_name().starts_with("snaketron.analytics.v1") {
                continue;
            }
            for field in message.fields() {
                assert_eq!(
                    field.name(),
                    field.name().to_lowercase(),
                    "{}.{} must be lowercase",
                    message.full_name(),
                    field.name()
                );
            }
        }
    }

    /// A message inside a map value breaks Iceberg's null-fill guarantee:
    /// after adding a field under <map>.value.<field>, older Parquet files
    /// fail to read instead of returning null (apache/iceberg#14043).
    #[test]
    fn no_map_has_a_message_value() {
        let pool = pool();
        for message in pool.all_messages() {
            if !message.full_name().starts_with("snaketron.analytics.v1") {
                continue;
            }
            for field in message.fields() {
                if !field.is_map() {
                    continue;
                }
                let Kind::Message(entry) = field.kind() else {
                    continue;
                };
                let value = entry.map_entry_value_field();
                assert!(
                    !matches!(value.kind(), Kind::Message(_)),
                    "{}.{} maps to a message value, which breaks schema evolution",
                    message.full_name(),
                    field.name()
                );
            }
        }
    }

    #[test]
    fn columns_are_derived_with_proto_number_paths() {
        let pool = pool();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        let by_name = |name: &str| {
            columns
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("missing column {name}"))
        };
        assert_eq!(by_name("event_id").proto_path, "1");
        assert_eq!(by_name("event_id").type_name, "string");
        // Nested struct plus its children, with the number path composed.
        assert_eq!(by_name("identity").type_name, "struct");
        assert_eq!(by_name("identity.user_id").proto_path, "9.1");
        assert_eq!(by_name("identity.user_id").type_name, "long");
        // Every derived column is optional.
        assert!(columns.iter().all(|c| c.optional));
    }

    /// Pre-widening exists because iceberg-rust has no type-promotion API.
    #[test]
    fn integers_are_pre_widened_and_enums_are_strings() {
        let pool = pool();
        let columns = derive_columns(&event_descriptor(&pool).unwrap());
        // occurred_at_ms is int64 -> long.
        let occurred = columns.iter().find(|c| c.name == "occurred_at_ms").unwrap();
        assert_eq!(occurred.type_name, "long");
        // No derived column may be a 32-bit int for an unsigned proto type.
        assert!(columns.iter().all(|c| c.type_name != "uint"));
    }
}

/// The schema lock: a checked-in snapshot of every proto field number that has
/// ever existed, and the name and type it carries.
///
/// `Transaction::update_schema()` in iceberg-rust exposes exactly `add_column`
/// and `delete_column`. A rename therefore reaches the table as drop+add: the
/// rows already written under the old name stay in the table but become
/// unreachable, and the new column starts empty. A retype and a number reuse
/// are equally destructive, and nothing at runtime can notice any of them —
/// the committer would apply the change and report success. So the barrier is
/// here, diffing the descriptors against a file that a reviewer has to see
/// change.
#[cfg(test)]
mod lock {
    use super::*;
    use std::collections::BTreeMap;
    use std::fmt::Write as _;
    use std::path::PathBuf;

    const LOCK_RELATIVE_PATH: &str = "proto/analytics/v1/events.schema.lock";
    const UPDATE_ENV: &str = "UPDATE_SCHEMA_LOCK";
    const REGENERATE_CMD: &str =
        "UPDATE_SCHEMA_LOCK=1 cargo test -p server --lib analytics::schema";
    const PACKAGE_PREFIX: &str = "snaketron.analytics.v1.";

    /// Type sentinel for a number whose field was deleted from the proto.
    /// Retired numbers are kept forever: a number nobody remembers is exactly
    /// the one a later edit silently reuses.
    const RETIRED: &str = "retired";

    const HEADER: &str = "\
# Analytics schema lock — GENERATED. Do not hand-edit.
#
# Every (message, field number) that has ever been committed is recorded here
# with the name and type it carries. The Iceberg committer can only add and
# delete columns, so a rename reaches the table as drop+add and silently
# orphans everything already written under the old name; a retype and a number
# reuse do the same. This file is what makes those unmergeable.
#
# Adding a new field number is always safe and needs no review of this file
# beyond confirming the added lines.
#
# Regenerate deliberately (and only after the table itself has been migrated):
#   UPDATE_SCHEMA_LOCK=1 cargo test -p server --lib analytics::schema
#
# Format, grouped by message and sorted by number:
#   <field number> <name> <proto type> <iceberg type>
";

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LockedField {
        name: String,
        proto_type: String,
        iceberg_type: String,
    }

    impl LockedField {
        fn retired(name: &str) -> Self {
            Self {
                name: name.to_owned(),
                proto_type: RETIRED.to_owned(),
                iceberg_type: RETIRED.to_owned(),
            }
        }

        fn is_retired(&self) -> bool {
            self.proto_type == RETIRED
        }
    }

    /// message full name -> field number -> field.
    ///
    /// Ordered maps, so the rendered file is byte-identical for a given
    /// descriptor set no matter what order protoc walked it in.
    type Snapshot = BTreeMap<String, BTreeMap<u32, LockedField>>;

    fn lock_path() -> PathBuf {
        // nextest replays archived test binaries from a different checkout than
        // the one that compiled them, and remaps CARGO_MANIFEST_DIR at run
        // time. The compile-time value is only a fallback for a plain
        // `cargo test`.
        let manifest = std::env::var("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned());
        PathBuf::from(manifest).join(LOCK_RELATIVE_PATH)
    }

    /// The proto type, spelled so that any wire-visible change shows up as a
    /// different token. Stricter than the Iceberg type on purpose: `uint32` and
    /// `fixed32` both land on `long`, but swapping them still rewrites the
    /// bytes of every future row.
    fn proto_type_token(field: &FieldDescriptor) -> String {
        if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!("a map field is always a message entry");
            };
            return format!(
                "map<{},{}>",
                proto_kind_token(&entry.map_entry_key_field()),
                proto_kind_token(&entry.map_entry_value_field())
            );
        }
        if field.is_list() {
            return format!("repeated:{}", proto_kind_token(field));
        }
        proto_kind_token(field)
    }

    fn proto_kind_token(field: &FieldDescriptor) -> String {
        match field.kind() {
            Kind::Message(message) => format!("message:{}", message.full_name()),
            Kind::Enum(enumeration) => format!("enum:{}", enumeration.full_name()),
            scalar => format!("{scalar:?}"),
        }
    }

    /// The column type the committer would derive, recorded next to the proto
    /// type so a reviewer can see what a proposed edit does to the table
    /// without replaying the mapping table in their head.
    fn iceberg_type_token(field: &FieldDescriptor) -> String {
        if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!("a map field is always a message entry");
            };
            return format!(
                "map<{},{}>",
                scalar_type_name(&entry.map_entry_key_field()),
                scalar_type_name(&entry.map_entry_value_field())
            );
        }
        if field.is_list() {
            return format!("list<{}>", scalar_type_name(field));
        }
        scalar_type_name(field)
    }

    /// Builds the snapshot from the descriptor set the binary actually embeds,
    /// so the lock cannot describe a schema the server does not emit.
    fn snapshot_from_descriptors(pool: &DescriptorPool) -> Snapshot {
        let mut snapshot = Snapshot::new();
        for message in pool.all_messages() {
            // Map entries are synthetic: they carry no durable identity of
            // their own, only the key/value pair already recorded on the field.
            if !message.full_name().starts_with(PACKAGE_PREFIX) || message.is_map_entry() {
                continue;
            }
            let fields = snapshot.entry(message.full_name().to_owned()).or_default();
            for field in message.fields() {
                fields.insert(
                    field.number(),
                    LockedField {
                        name: field.name().to_owned(),
                        proto_type: proto_type_token(&field),
                        iceberg_type: iceberg_type_token(&field),
                    },
                );
            }
        }
        snapshot
    }

    /// Carries forward every number the previous lock knew about but the proto
    /// no longer defines. Without this a delete-then-reuse across two commits
    /// would look additive.
    fn with_retired_history(mut current: Snapshot, previous: &Snapshot) -> Snapshot {
        for (message, fields) in previous {
            let entry = current.entry(message.clone()).or_default();
            for (number, field) in fields {
                entry
                    .entry(*number)
                    .or_insert_with(|| LockedField::retired(&field.name));
            }
        }
        current
    }

    fn render(snapshot: &Snapshot) -> String {
        let mut out = String::from(HEADER);
        for (message, fields) in snapshot {
            let _ = write!(out, "\nmessage {message}\n");
            for (number, field) in fields {
                let _ = writeln!(
                    out,
                    "  {number} {} {} {}",
                    field.name, field.proto_type, field.iceberg_type
                );
            }
        }
        out
    }

    fn parse(text: &str) -> Snapshot {
        let mut snapshot = Snapshot::new();
        let mut message: Option<String> = None;
        for (index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            let line_number = index + 1;
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(name) = line.strip_prefix("message ") {
                let name = name.trim().to_owned();
                snapshot.entry(name.clone()).or_default();
                message = Some(name);
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(
                parts.len(),
                4,
                "malformed schema lock at line {line_number}: {line:?}"
            );
            let owner = message
                .clone()
                .unwrap_or_else(|| panic!("schema lock line {line_number} precedes any message"));
            let number: u32 = parts[0]
                .parse()
                .unwrap_or_else(|_| panic!("bad field number at schema lock line {line_number}"));
            snapshot.entry(owner).or_default().insert(
                number,
                LockedField {
                    name: parts[1].to_owned(),
                    proto_type: parts[2].to_owned(),
                    iceberg_type: parts[3].to_owned(),
                },
            );
        }
        snapshot
    }

    /// Classifies every way the proto can have moved away from the lock
    /// destructively. Purely additive change returns an empty vector.
    fn violations(locked: &Snapshot, current: &Snapshot) -> Vec<String> {
        let mut problems = Vec::new();
        let empty = BTreeMap::new();

        for (message, locked_fields) in locked {
            let current_fields = current.get(message).unwrap_or(&empty);

            for (number, was) in locked_fields {
                let Some(now) = current_fields.get(number) else {
                    if was.is_retired() {
                        continue;
                    }
                    problems.push(format!(
                        "FIELD REMOVED — {message} field {number} {:?} is gone from the proto.\n  \
                         The committer never drops a column, so the table keeps carrying \
                         {:?} with its history — but the number is now free, and a later \
                         edit that reuses it would repoint the pb:{number} registry entry \
                         in the Iceberg doc string at different data. Mark the number \
                         `reserved` in the proto and record the deletion deliberately.",
                        was.name, was.name
                    ));
                    continue;
                };

                if was.name != now.name {
                    let moved_to = locked_fields
                        .iter()
                        .find(|(other, field)| *other != number && field.name == now.name)
                        .map(|(other, _)| *other);

                    if was.is_retired() || moved_to.is_some() {
                        let previously = match moved_to {
                            Some(other) => {
                                format!("{:?} already holds number {other} in the lock", now.name)
                            }
                            None => format!("number {number} was retired by {:?}", was.name),
                        };
                        problems.push(format!(
                            "NUMBER REUSE — {message} field {number} now carries {:?}, but \
                             {previously}.\n  \
                             The proto number is the durable identity: it is written into \
                             each Iceberg field's doc string as pb:{number}, which makes the \
                             table metadata its own registry. Reusing a number repoints that \
                             entry at a different column, so historical rows end up \
                             attributed to the wrong field. Pick an unused number.",
                            now.name
                        ));
                    } else {
                        problems.push(format!(
                            "RENAME — {message} field {number}: {:?} -> {:?}.\n  \
                             iceberg-rust can only add and delete columns, so this reaches \
                             the table as drop {:?} + add {:?}: every row already written \
                             under {:?} stays in the table but is no longer reachable from \
                             the schema, and the new column starts empty. Field names are \
                             append-only — keep {:?} and add a new field number instead.",
                            was.name, now.name, was.name, now.name, was.name, was.name
                        ));
                    }
                    continue;
                }

                if was.proto_type != now.proto_type || was.iceberg_type != now.iceberg_type {
                    problems.push(format!(
                        "TYPE CHANGE — {message} field {number} {:?}: {} ({}) -> {} ({}).\n  \
                         iceberg-rust has no type-promotion API, so the committer can only \
                         express this as drop+add of the same column name, orphaning every \
                         value already written. Integers are pre-widened and enums are \
                         carried as strings precisely so this never has to happen. Add a new \
                         field number instead.",
                        was.name,
                        was.proto_type,
                        was.iceberg_type,
                        now.proto_type,
                        now.iceberg_type
                    ));
                }
            }
        }

        problems
    }

    /// The gate. Additive change passes; a rename, retype, or number reuse
    /// cannot be merged without rewriting the lock in the same commit, where a
    /// reviewer sees it.
    #[test]
    fn the_schema_lock_admits_no_rename_retype_or_number_reuse() {
        let pool = descriptor_pool().expect("descriptors must decode");
        let current = snapshot_from_descriptors(&pool);
        let path = lock_path();

        let previous = std::fs::read_to_string(&path)
            .map(|text| parse(&text))
            .unwrap_or_default();

        if std::env::var(UPDATE_ENV).is_ok_and(|value| value != "0") {
            let merged = with_retired_history(current, &previous);
            std::fs::write(&path, render(&merged))
                .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
            eprintln!("{} rewrote {}", UPDATE_ENV, path.display());
            return;
        }

        assert!(
            !previous.is_empty(),
            "{} is missing or empty. It is the only thing standing between a proto \
             rename and silently orphaned analytics data. Create it with: {REGENERATE_CMD}",
            path.display()
        );

        let problems = violations(&previous, &current);
        assert!(
            problems.is_empty(),
            "\n\nThe analytics proto changed in a way that would orphan data already \
             written to the Iceberg table:\n\n{}\n\nIf the change is genuinely intended, \
             migrate the table first (DuckDB 1.5 ALTER TABLE ... RENAME COLUMN is \
             metadata-only and preserves field ids and data), then record it in the same \
             commit with:\n    {REGENERATE_CMD}\n",
            problems.join("\n\n")
        );

        // Additive drift is safe, so it does not fail here — but the lock only
        // protects the numbers it knows about, so say so loudly.
        if with_retired_history(current, &previous) != previous {
            eprintln!(
                "note: {} does not yet record every field in the proto. \
                 The added numbers are unprotected until you run: {REGENERATE_CMD}",
                path.display()
            );
        }
    }

    fn field(name: &str, proto_type: &str, iceberg_type: &str) -> LockedField {
        LockedField {
            name: name.to_owned(),
            proto_type: proto_type.to_owned(),
            iceberg_type: iceberg_type.to_owned(),
        }
    }

    fn snapshot(fields: &[(u32, LockedField)]) -> Snapshot {
        Snapshot::from([(
            "snaketron.analytics.v1.Probe".to_owned(),
            fields.iter().cloned().collect::<BTreeMap<_, _>>(),
        )])
    }

    #[test]
    fn a_rename_is_reported_as_a_rename() {
        let locked = snapshot(&[(1, field("event_id", "string", "string"))]);
        let current = snapshot(&[(1, field("event_uuid", "string", "string"))]);

        let problems = violations(&locked, &current);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].starts_with("RENAME —"), "{}", problems[0]);
        assert!(problems[0].contains("\"event_id\" -> \"event_uuid\""));
        assert!(problems[0].contains("orphan") || problems[0].contains("no longer reachable"));
    }

    #[test]
    fn a_type_change_is_reported_as_a_type_change() {
        let locked = snapshot(&[(3, field("event_version", "int64", "long"))]);
        let current = snapshot(&[(3, field("event_version", "string", "string"))]);

        let problems = violations(&locked, &current);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].starts_with("TYPE CHANGE —"), "{}", problems[0]);
        assert!(problems[0].contains("int64 (long) -> string (string)"));
    }

    /// A number freed by a deletion and later handed to a different field.
    #[test]
    fn reusing_a_retired_number_is_reported_as_a_number_reuse() {
        let locked = snapshot(&[(7, LockedField::retired("winner_user_id"))]);
        let current = snapshot(&[(7, field("winner_team_id", "int64", "long"))]);

        let problems = violations(&locked, &current);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].starts_with("NUMBER REUSE —"), "{}", problems[0]);
        assert!(problems[0].contains("retired by \"winner_user_id\""));
    }

    /// Two live fields trading numbers: every row of both is misattributed, and
    /// a per-number name comparison alone would call this two renames.
    #[test]
    fn swapping_two_numbers_is_reported_as_a_number_reuse() {
        let locked = snapshot(&[
            (1, field("score", "int64", "long")),
            (2, field("mmr_delta", "int64", "long")),
        ]);
        let current = snapshot(&[
            (1, field("mmr_delta", "int64", "long")),
            (2, field("score", "int64", "long")),
        ]);

        let problems = violations(&locked, &current);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems.iter().all(|p| p.starts_with("NUMBER REUSE —")));
    }

    #[test]
    fn deleting_a_field_is_reported_as_a_removal() {
        let locked = snapshot(&[(4, field("byte_len", "int64", "long"))]);
        let current = snapshot(&[]);

        let problems = violations(&locked, &current);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].starts_with("FIELD REMOVED —"),
            "{}",
            problems[0]
        );
    }

    /// The whole point of the design: adding fields must never need a table
    /// migration, so it must never trip the gate.
    #[test]
    fn adding_new_field_numbers_is_not_a_violation() {
        let locked = snapshot(&[(1, field("score", "int64", "long"))]);
        let mut current = snapshot(&[
            (1, field("score", "int64", "long")),
            (2, field("assists", "int64", "long")),
        ]);
        current.insert(
            "snaketron.analytics.v1.BrandNew".to_owned(),
            BTreeMap::from([(1, field("first", "string", "string"))]),
        );

        assert!(violations(&locked, &current).is_empty());
    }

    /// A retired number that stays retired is history, not a violation.
    #[test]
    fn a_retired_number_left_alone_is_not_a_violation() {
        let locked = snapshot(&[(9, LockedField::retired("legacy_id"))]);
        let current = snapshot(&[]);

        assert!(violations(&locked, &current).is_empty());
    }

    #[test]
    fn the_lock_file_round_trips_through_render_and_parse() {
        let pool = descriptor_pool().expect("descriptors must decode");
        let current = snapshot_from_descriptors(&pool);
        assert_eq!(parse(&render(&current)), current);
        assert!(current.contains_key("snaketron.analytics.v1.Event"));
    }

    /// The lock records the number path's leaf identity for nested messages
    /// too, because a rename inside `Identity` orphans `identity.<name>` just
    /// as thoroughly as a top-level one.
    #[test]
    fn nested_messages_and_oneof_arms_are_locked() {
        let pool = descriptor_pool().expect("descriptors must decode");
        let current = snapshot_from_descriptors(&pool);

        let identity = &current["snaketron.analytics.v1.Identity"];
        assert_eq!(identity[&1].name, "user_id");
        assert_eq!(identity[&1].proto_type, "int64");
        assert_eq!(identity[&1].iceberg_type, "long");

        let event = &current["snaketron.analytics.v1.Event"];
        assert_eq!(event[&9].name, "identity");
        assert_eq!(
            event[&9].proto_type,
            "message:snaketron.analytics.v1.Identity"
        );
        assert_eq!(event[&100].name, "guest_created");
    }
}
