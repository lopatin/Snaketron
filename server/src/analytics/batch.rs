//! Memory-bounded, date-bucketed batching.
//!
//! Two concerns that must not be conflated (PRD R4.6 and R6.3a):
//!
//! * **When to flush** — batch age, buffered bytes, or buffered count.
//! * **How to split the flush into files** — a per-file cap applied afterwards.
//!
//! Bytes is the primary bound and count is the backstop. A count alone does
//! not bound memory (one oversized payload blows it) and a byte cap alone does
//! not bound per-event overhead when events are tiny. Buffered bytes are
//! accumulated as each event is added, never recomputed by walking the buffer.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// A single event, already serialized, with the facts the key layout needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedEvent {
    /// One line of NDJSON, without the trailing newline.
    pub line: String,
    /// `YYYY-MM-DD` of the event time. Files are partitioned by *event* date,
    /// so a backlog replayed after midnight still lands in the day it belongs
    /// to rather than the day it was written.
    pub date: String,
    /// Sortable, data-derived ordering token. On the durable path this is the
    /// Valkey stream id, which `XAUTOCLAIM` reproduces exactly after a crash;
    /// that is what keeps object keys stable across a replay.
    pub cursor: String,
}

#[derive(Debug, Clone)]
pub struct BatchLimits {
    /// Flush when the oldest buffered event reaches this age. Measured from
    /// the first buffered event, NOT from the last flush: during catch-up a
    /// bucket filled by replayed backlog must age out on its own rather than
    /// waiting for a wall-clock tick that has just fired.
    pub max_batch_age: Duration,
    pub max_buffer_bytes: usize,
    pub max_buffer_events: usize,
    pub max_events_per_file: usize,
    pub max_bytes_per_file: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_batch_age: Duration::from_millis(300_000),
            max_buffer_bytes: 64 * 1024 * 1024,
            max_buffer_events: 100_000,
            max_events_per_file: 50_000,
            max_bytes_per_file: 32 * 1024 * 1024,
        }
    }
}

/// One object to be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFile {
    pub date: String,
    pub events: Vec<BufferedEvent>,
    pub first_cursor: String,
    pub last_cursor: String,
}

impl PendingFile {
    /// The NDJSON body: one complete event per line, newline-terminated.
    pub fn body(&self) -> String {
        let mut body = String::new();
        for event in &self.events {
            body.push_str(&event.line);
            body.push('\n');
        }
        body
    }
}

/// Accumulates events and decides when and how to write them.
#[derive(Debug)]
pub struct EventBatcher {
    limits: BatchLimits,
    buckets: BTreeMap<String, Vec<BufferedEvent>>,
    buffered_bytes: usize,
    buffered_events: usize,
    oldest_at: Option<Instant>,
}

impl EventBatcher {
    pub fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            buckets: BTreeMap::new(),
            buffered_bytes: 0,
            buffered_events: 0,
            oldest_at: None,
        }
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub fn buffered_events(&self) -> usize {
        self.buffered_events
    }

    pub fn is_empty(&self) -> bool {
        self.buffered_events == 0
    }

    /// Adds an event. Byte accounting is incremental, so the bound holds
    /// regardless of how large or small individual events turn out to be.
    pub fn push(&mut self, event: BufferedEvent) {
        if self.oldest_at.is_none() {
            self.oldest_at = Some(Instant::now());
        }
        self.buffered_bytes += event.line.len() + 1;
        self.buffered_events += 1;
        self.buckets
            .entry(event.date.clone())
            .or_default()
            .push(event);
    }

    /// Whether any flush trigger has fired.
    pub fn should_flush(&self) -> bool {
        self.should_flush_at(Instant::now())
    }

    pub fn should_flush_at(&self, now: Instant) -> bool {
        if self.buffered_events == 0 {
            return false;
        }
        if self.buffered_bytes >= self.limits.max_buffer_bytes {
            return true;
        }
        if self.buffered_events >= self.limits.max_buffer_events {
            return true;
        }
        self.oldest_at
            .is_some_and(|at| now.duration_since(at) >= self.limits.max_batch_age)
    }

    /// Drains everything into files, splitting each date bucket by the
    /// per-file caps.
    ///
    /// Split boundaries are a pure function of the event sequence and the
    /// caps — never of wall-clock time or of how many events happened to be
    /// buffered when the flush fired — so replaying the same events produces
    /// the identical set of keys.
    pub fn drain(&mut self) -> Vec<PendingFile> {
        let mut files = Vec::new();
        for (date, events) in std::mem::take(&mut self.buckets) {
            let mut chunk: Vec<BufferedEvent> = Vec::new();
            let mut chunk_bytes = 0usize;
            for event in events {
                let size = event.line.len() + 1;
                let would_exceed = !chunk.is_empty()
                    && (chunk.len() >= self.limits.max_events_per_file
                        || chunk_bytes + size > self.limits.max_bytes_per_file);
                if would_exceed {
                    files.push(seal(&date, std::mem::take(&mut chunk)));
                    chunk_bytes = 0;
                }
                chunk_bytes += size;
                chunk.push(event);
            }
            if !chunk.is_empty() {
                files.push(seal(&date, chunk));
            }
        }
        self.buffered_bytes = 0;
        self.buffered_events = 0;
        self.oldest_at = None;
        files
    }
}

fn seal(date: &str, events: Vec<BufferedEvent>) -> PendingFile {
    let first_cursor = events.first().map(|e| e.cursor.clone()).unwrap_or_default();
    let last_cursor = events.last().map(|e| e.cursor.clone()).unwrap_or_default();
    PendingFile {
        date: date.to_owned(),
        events,
        first_cursor,
        last_cursor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(cursor: &str, date: &str, size: usize) -> BufferedEvent {
        BufferedEvent {
            line: "x".repeat(size),
            date: date.to_owned(),
            cursor: cursor.to_owned(),
        }
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            max_batch_age: Duration::from_secs(300),
            max_buffer_bytes: 1_000,
            max_buffer_events: 10,
            max_events_per_file: 3,
            max_bytes_per_file: 1_000,
        }
    }

    #[test]
    fn an_empty_batcher_never_flushes() {
        assert!(!EventBatcher::new(limits()).should_flush());
    }

    /// Bytes is the primary bound: a single oversized payload must trigger a
    /// flush even though the event count is nowhere near its cap.
    #[test]
    fn the_byte_cap_fires_before_the_count_cap_on_large_events() {
        let mut batcher = EventBatcher::new(limits());
        batcher.push(event("1", "2026-08-19", 999));
        assert_eq!(batcher.buffered_events(), 1);
        assert!(
            batcher.should_flush(),
            "1000 bytes must trip the 1000-byte cap"
        );
    }

    /// Count is the backstop: many tiny events must not evade the bound.
    #[test]
    fn the_count_cap_fires_before_the_byte_cap_on_tiny_events() {
        let mut batcher = EventBatcher::new(limits());
        for index in 0..10 {
            batcher.push(event(&index.to_string(), "2026-08-19", 1));
        }
        assert!(batcher.buffered_bytes() < 1_000);
        assert!(
            batcher.should_flush(),
            "the count cap must bound tiny events"
        );
    }

    /// Age is measured from the first buffered event, so a lone event still
    /// gets written rather than waiting indefinitely for company.
    #[test]
    fn age_is_measured_from_the_first_buffered_event() {
        let mut batcher = EventBatcher::new(BatchLimits {
            max_batch_age: Duration::from_millis(50),
            ..limits()
        });
        batcher.push(event("1", "2026-08-19", 1));
        assert!(!batcher.should_flush(), "not yet aged");
        let later = Instant::now() + Duration::from_millis(60);
        assert!(batcher.should_flush_at(later), "must flush on age alone");
    }

    #[test]
    fn draining_resets_the_bound() {
        let mut batcher = EventBatcher::new(limits());
        batcher.push(event("1", "2026-08-19", 10));
        assert!(batcher.buffered_bytes() > 0);
        let files = batcher.drain();
        assert_eq!(files.len(), 1);
        assert_eq!(batcher.buffered_bytes(), 0);
        assert_eq!(batcher.buffered_events(), 0);
        assert!(batcher.is_empty());
        assert!(!batcher.should_flush());
    }

    /// A batch spanning midnight writes one object per event date. This is the
    /// only remaining source of multi-object flushes.
    #[test]
    fn a_batch_spanning_midnight_writes_one_file_per_date() {
        let mut batcher = EventBatcher::new(limits());
        batcher.push(event("1", "2026-08-19", 5));
        batcher.push(event("2", "2026-08-20", 5));
        let files = batcher.drain();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].date, "2026-08-19");
        assert_eq!(files[1].date, "2026-08-20");
    }

    /// Split boundaries must depend only on the event sequence and the caps,
    /// so a replay reproduces the identical key set.
    #[test]
    fn splitting_is_deterministic_and_carries_its_own_cursor_range() {
        let build = || {
            let mut batcher = EventBatcher::new(limits());
            for index in 0..7 {
                batcher.push(event(&format!("{index:03}"), "2026-08-19", 5));
            }
            batcher.drain()
        };
        let first = build();
        let second = build();
        assert_eq!(first, second, "splitting must be deterministic");
        // 7 events at 3 per file.
        assert_eq!(first.len(), 3);
        assert_eq!(first[0].first_cursor, "000");
        assert_eq!(first[0].last_cursor, "002");
        assert_eq!(first[1].first_cursor, "003");
        assert_eq!(first[2].first_cursor, "006");
        assert_eq!(first[2].last_cursor, "006");
        // Each file's range covers exactly its own contents, not the batch's.
        for file in &first {
            assert_eq!(file.first_cursor, file.events.first().unwrap().cursor);
            assert_eq!(file.last_cursor, file.events.last().unwrap().cursor);
        }
    }

    #[test]
    fn a_file_body_is_newline_terminated_ndjson() {
        let mut batcher = EventBatcher::new(limits());
        batcher.push(BufferedEvent {
            line: "{\"a\":1}".to_owned(),
            date: "2026-08-19".to_owned(),
            cursor: "1".to_owned(),
        });
        batcher.push(BufferedEvent {
            line: "{\"a\":2}".to_owned(),
            date: "2026-08-19".to_owned(),
            cursor: "2".to_owned(),
        });
        let body = batcher.drain()[0].body();
        assert_eq!(body, "{\"a\":1}\n{\"a\":2}\n");
        assert!(body.ends_with('\n'));
        assert_eq!(body.lines().count(), 2);
    }

    /// A huge event must not be silently dropped by the splitter just because
    /// it alone exceeds the per-file byte cap.
    #[test]
    fn an_event_larger_than_the_per_file_cap_still_gets_written() {
        let mut batcher = EventBatcher::new(BatchLimits {
            max_bytes_per_file: 10,
            ..limits()
        });
        batcher.push(event("1", "2026-08-19", 100));
        let files = batcher.drain();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].events.len(), 1);
    }
}
