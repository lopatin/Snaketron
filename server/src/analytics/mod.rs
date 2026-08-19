//! Game-events analytics pipeline.
//!
//! See `snaketron-io/specs/game-events-analytics-prd.md`. The pipeline is
//! best-effort by design and must never be load-bearing for gameplay: every
//! path here drops rather than blocks, and every failure is counted rather
//! than propagated.

pub mod batch;
pub mod committer;
pub mod completion_events;
pub mod emitter;
pub mod event;
pub mod exporter;
pub mod exporter_service;
pub mod flusher;
pub mod iceberg_catalog;
pub mod object_store;
pub mod schema;
pub mod sink;
pub mod ws_exporter;

pub use batch::{BatchLimits, BufferedEvent, EventBatcher, PendingFile};
pub use emitter::{AnalyticsEmitter, DropReason, EmitterConfig, EmitterMetrics};
pub use event::{EventIdentity, EventOrigin, envelope, to_json_line};

/// Generated analytics protobuf types.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/snaketron.analytics.v1.rs"));
}

/// The compiled descriptor set, embedded at build time.
///
/// This is what lets the Iceberg committer derive a table schema from the same
/// definitions the emitter writes, with no registry and no second source of
/// truth that could disagree with the running binary.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/analytics.bin"));
