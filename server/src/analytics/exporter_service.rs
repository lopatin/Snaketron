//! The regional S3 exporter, as a hosted service.
//!
//! Exactly one runs per region, elected by a regional exclusion lease. It
//! drains the analytics stream through a consumer group and writes to S3,
//! acknowledging **only after every object for the batch is durably written**.
//!
//! That ordering is the whole at-least-once guarantee: a crash between the
//! write and the ack replays the same claimed entries, which reproduce the same
//! object key, which the conditional write refuses — so the replay is a no-op
//! rather than a duplicate.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::streams::{StreamAutoClaimReply, StreamReadOptions, StreamReadReply};
use snaketron_service_api::{
    ExclusionKey, HostedService, HostedServiceFactory, ServiceContext, ServiceError,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::redis_utils::RedisConnection;

use super::batch::EventBatcher;
use super::emitter::EmitterMetrics;
use super::exporter::{
    ExportTarget, PoisonTracker, buffer_entries, default_limits, report_discontinuity, write_batch,
};
use super::flusher::split_entry;
use super::object_store::ObjectStore;

/// Minimum idle time before reclaiming a predecessor's pending entries.
///
/// Deliberately NOT zero. The executor's fenced consumer can safely use zero
/// because its partition lease makes it exclusive at the point of effect; this
/// consumer has no such fence, so a non-zero idle window avoids stealing from a
/// predecessor that still believes it holds the lease.
const AUTOCLAIM_MIN_IDLE: Duration = Duration::from_millis(60_000);
const READ_COUNT: usize = 512;
const BLOCK: Duration = Duration::from_millis(2_000);

pub struct ExporterFactory {
    redis: RedisConnection,
    store: Arc<dyn ObjectStore>,
    metrics: Arc<EmitterMetrics>,
    stream_key: String,
    group: String,
    region: String,
}

impl ExporterFactory {
    pub fn new(
        redis: RedisConnection,
        store: Arc<dyn ObjectStore>,
        metrics: Arc<EmitterMetrics>,
        stream_key: String,
        group: String,
        region: String,
    ) -> Self {
        Self {
            redis,
            store,
            metrics,
            stream_key,
            group,
            region,
        }
    }
}

#[async_trait]
impl HostedServiceFactory for ExporterFactory {
    fn name(&self) -> &str {
        "analytics-exporter"
    }

    /// One per region. The stream is regional, so a global key would starve
    /// every region but one.
    fn exclusion_key(&self, _ctx: &ServiceContext) -> Option<ExclusionKey> {
        Some(ExclusionKey::region(format!(
            "analytics-exporter/{}",
            self.region
        )))
    }

    async fn build(&self, ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        Ok(Box::new(Exporter {
            redis: self.redis.clone(),
            store: self.store.clone(),
            metrics: self.metrics.clone(),
            stream_key: self.stream_key.clone(),
            group: self.group.clone(),
            consumer: ctx.identity.task_boot_id.clone(),
            target: ExportTarget {
                dataset: "game-events".to_owned(),
                host: format!("{}-{}", self.region, ctx.identity.server_id),
            },
            ctx,
        }))
    }
}

struct Exporter {
    redis: RedisConnection,
    store: Arc<dyn ObjectStore>,
    metrics: Arc<EmitterMetrics>,
    stream_key: String,
    group: String,
    consumer: String,
    target: ExportTarget,
    ctx: ServiceContext,
}

/// One `XRANGE` entry: its id, and its field/value pairs.
///
/// Named because the bare tuple is what `redis` returns and reads as noise at
/// the call site — the shape is only ever used to look at the id.
type StreamEntry = (String, Vec<(String, String)>);

impl Exporter {
    /// `BUSYGROUP` means someone already created it, which is success.
    async fn ensure_group(&self) -> Result<(), ServiceError> {
        let mut redis = self.redis.clone();
        let result: redis::RedisResult<()> = redis
            .xgroup_create_mkstream(&self.stream_key, &self.group, "0-0")
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
            Err(error) => Err(ServiceError::failed(format!(
                "analytics consumer group creation failed: {error}"
            ))),
        }
    }

    /// Detects entries lost to trimming.
    ///
    /// Trim-horizon loss is acceptable for analytics but must never be silent:
    /// an operator has to be able to tell "no traffic" from "we dropped it".
    async fn check_discontinuity(&self, resume_from: &str) {
        let mut redis = self.redis.clone();
        let oldest: redis::RedisResult<Vec<StreamEntry>> =
            redis.xrange_count(&self.stream_key, "-", "+", 1).await;
        if let Ok(entries) = oldest
            && let Some((oldest_id, _)) = entries.first()
            && oldest_id.as_str() > resume_from
        {
            // Exact counting is impossible once entries are gone; report that
            // a gap exists rather than pretending to know its size.
            report_discontinuity(&self.metrics, 1);
            warn!(
                "analytics stream trimmed past the exporter cursor \
                 (resume {resume_from}, oldest {oldest_id})"
            );
        }
    }

    /// Reclaims a predecessor's pending entries after a lease handover.
    async fn reclaim(&self) -> Result<Vec<(String, String)>, ServiceError> {
        let mut redis = self.redis.clone();
        let reply: redis::RedisResult<StreamAutoClaimReply> = redis
            .xautoclaim_options(
                &self.stream_key,
                &self.group,
                &self.consumer,
                AUTOCLAIM_MIN_IDLE.as_millis() as usize,
                "0-0",
                redis::streams::StreamAutoClaimOptions::default().count(READ_COUNT),
            )
            .await;
        match reply {
            Ok(reply) => Ok(reply
                .claimed
                .into_iter()
                .filter_map(|entry| {
                    entry
                        .get::<String>("data")
                        .map(|payload| (entry.id.clone(), payload))
                })
                .collect()),
            Err(error) => Err(ServiceError::failed(format!(
                "analytics XAUTOCLAIM failed: {error}"
            ))),
        }
    }

    async fn read_new(&self) -> Result<Vec<(String, String)>, ServiceError> {
        let mut redis = self.redis.clone();
        let options = StreamReadOptions::default()
            .group(&self.group, &self.consumer)
            .count(READ_COUNT)
            .block(BLOCK.as_millis() as usize);
        let reply: redis::RedisResult<Option<StreamReadReply>> = redis
            .xread_options(&[&self.stream_key], &[">"], &options)
            .await;
        match reply {
            Ok(None) => Ok(Vec::new()),
            Ok(Some(reply)) => Ok(reply
                .keys
                .into_iter()
                .flat_map(|key| key.ids)
                .filter_map(|entry| {
                    entry
                        .get::<String>("data")
                        .map(|payload| (entry.id.clone(), payload))
                })
                .collect()),
            Err(error) => Err(ServiceError::failed(format!(
                "analytics XREADGROUP failed: {error}"
            ))),
        }
    }

    async fn ack(&self, ids: &[String]) -> Result<(), ServiceError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut redis = self.redis.clone();
        let result: redis::RedisResult<i64> = redis.xack(&self.stream_key, &self.group, ids).await;
        result
            .map(|_| ())
            .map_err(|error| ServiceError::failed(format!("analytics XACK failed: {error}")))
    }
}

#[async_trait]
impl HostedService for Exporter {
    async fn run(&mut self, cancel: CancellationToken) -> Result<(), ServiceError> {
        self.ensure_group().await?;
        self.ctx.mark_ready();

        let mut poison = PoisonTracker::default();
        let mut reclaimed_once = false;

        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }

            // On first pass after acquiring the lease, take over whatever the
            // predecessor left pending before reading anything new.
            let entries = if !reclaimed_once {
                reclaimed_once = true;
                let claimed = self.reclaim().await?;
                if let Some((first, _)) = claimed.first() {
                    self.check_discontinuity(first).await;
                }
                if claimed.is_empty() {
                    self.read_new().await?
                } else {
                    claimed
                }
            } else {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    result = self.read_new() => result?,
                }
            };

            if entries.is_empty() {
                continue;
            }

            let ids: Vec<String> = entries.iter().map(|(id, _)| id.clone()).collect();
            let mut batcher = EventBatcher::new(default_limits());
            let mut parsed = Vec::new();
            for (id, payload) in &entries {
                for line in split_entry(payload) {
                    let occurred_at_ms = occurred_at(line);
                    parsed.push((id.clone(), line.to_owned(), occurred_at_ms));
                }
            }
            buffer_entries(&mut batcher, parsed);
            let files = batcher.drain();

            match write_batch(&self.store, &self.target, &files, &self.metrics).await {
                Ok(_) => {
                    // Ack ONLY after every object is durably written.
                    self.ack(&ids).await?;
                    poison.record_success();
                }
                Err(error) => {
                    let signature = ids.join(",");
                    if poison.record_failure(&signature) {
                        // Never wedge: a batch that cannot be written must not
                        // block export forever.
                        warn!("analytics batch discarded after repeated failure: {error}");
                        self.metrics
                            .record_drops(super::emitter::DropReason::Rejected, ids.len() as u64);
                        self.ack(&ids).await?;
                        poison.record_success();
                    } else {
                        warn!("analytics batch write failed, will retry: {error}");
                        tokio::time::sleep(Duration::from_millis(
                            500 * u64::from(poison.failures()),
                        ))
                        .await;
                    }
                }
            }
        }
    }
}

/// Reads `occurred_at_ms` back out of a serialized line.
///
/// The field is QUOTED on the wire: `to_json_line` sets
/// `stringify_64_bit_integers(true)` for the proto3 JSON mapping, so a bare
/// `as_i64()` returns `None` on every line this pipeline actually emits and
/// silently takes the fallback below. That stamps the batch with wall-clock
/// now, and `event_date` turns it into the `dt=` partition — so a replayed or
/// backlogged batch would be filed under the day it was exported rather than
/// the day it happened, which is precisely the guarantee `event.rs` makes
/// about `occurred_at_ms` never being rewritten downstream.
///
/// Falls back to now only on a genuinely malformed line, so one bad event
/// cannot misfile an entire batch.
fn occurred_at(line: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|value| {
            value.get("occurred_at_ms").and_then(|v| match v {
                serde_json::Value::Number(n) => n.as_i64(),
                // The canonical encoding. Same acceptance as
                // `arrow_rows::json_i64`, so the fold and the partitioner
                // can never disagree about what a line says.
                serde_json::Value::String(s) => s.trim().parse().ok(),
                _ => None,
            })
        })
        .unwrap_or_else(super::event::now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn occurred_at_is_read_from_the_serialized_line() {
        assert_eq!(
            occurred_at("{\"occurred_at_ms\":1755600000123}"),
            1_755_600_000_123
        );
    }

    /// The fixture above is hand-written and does NOT match what this pipeline
    /// emits: `to_json_line` quotes 64-bit integers per the proto3 JSON
    /// mapping. Reading a real line is the only version of this test that
    /// proves the day partition is derived from event time rather than from
    /// whenever the exporter happened to run.
    #[test]
    fn occurred_at_is_read_from_a_line_the_writer_actually_produced() {
        let event = super::super::event::envelope(
            &super::super::event::EventOrigin {
                environment: "test".to_owned(),
                region: "use1".to_owned(),
                aws_region: "us-east-1".to_owned(),
                instance_id: "1:test".to_owned(),
            },
            super::super::event::EventIdentity::default(),
            super::super::proto::event::Payload::GuestCreated(Default::default()),
        );
        let line = super::super::event::to_json_line(&event).unwrap();
        assert!(
            line.contains("\"occurred_at_ms\":\""),
            "the writer quotes 64-bit integers; this test is meaningless otherwise"
        );
        assert_eq!(occurred_at(&line), event.occurred_at_ms);
    }

    /// The reason the above matters: `occurred_at` feeds `event_date`, which
    /// becomes the `dt=` path component. A backdated event -- a replay, or a
    /// batch that sat in the stream across midnight -- must be filed under the
    /// day it HAPPENED, not the day it was exported. When `occurred_at` fell
    /// back to wall-clock now, every such event was silently misfiled.
    #[test]
    fn a_backdated_event_is_partitioned_by_when_it_happened() {
        let mut event = super::super::event::envelope(
            &super::super::event::EventOrigin {
                environment: "test".to_owned(),
                region: "use1".to_owned(),
                aws_region: "us-east-1".to_owned(),
                instance_id: "1:test".to_owned(),
            },
            super::super::event::EventIdentity::default(),
            super::super::proto::event::Payload::GuestCreated(Default::default()),
        );
        // 2024-03-05T06:07:08.009Z, comfortably in the past.
        event.occurred_at_ms = 1_709_618_828_009;
        let line = super::super::event::to_json_line(&event).unwrap();

        assert_eq!(
            super::super::exporter::event_date(occurred_at(&line)),
            "2024-03-05",
            "the partition must follow event time, not export time"
        );
        assert_ne!(
            super::super::exporter::event_date(occurred_at(&line)),
            super::super::exporter::event_date(super::super::event::now_ms()),
            "and must therefore differ from today"
        );
    }

    /// A malformed line must not misfile the whole batch, so it falls back to
    /// now rather than to zero (which would land in 1970).
    #[test]
    fn a_malformed_line_falls_back_to_now() {
        let before = super::super::event::now_ms();
        let parsed = occurred_at("not json");
        assert!(parsed >= before, "must not fall back to the epoch");
    }

    #[test]
    fn the_reclaim_window_is_not_zero() {
        assert!(
            AUTOCLAIM_MIN_IDLE.as_millis() >= 60_000,
            "a zero idle window would steal from a live predecessor"
        );
    }
}
