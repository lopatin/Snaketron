//! Process-global analytics sink.
//!
//! Deliberately shaped like `resilience_metrics`, which is already a
//! process-global facade called from the same completion site. The alternative
//! — threading an emitter through `GameActor`'s constructor and every
//! partition dispatch path — would put analytics plumbing inside the most
//! correctness-critical code in the repo for no behavioural gain.
//!
//! Safe as a global precisely because emission is fire-and-forget: it drops
//! under pressure, returns nothing actionable, and no caller may branch on it.

use std::sync::OnceLock;

use crate::completion::CompletionRecordV1;

use super::emitter::AnalyticsEmitter;
use super::event::EventOrigin;

struct Sink {
    emitter: AnalyticsEmitter,
    origin: EventOrigin,
}

static SINK: OnceLock<Sink> = OnceLock::new();

/// Installs the sink. Called once during server start.
///
/// A second call is ignored rather than panicking: tests start several servers
/// in one process, and a panic there would be a test-only failure mode with no
/// production meaning.
pub fn install(emitter: AnalyticsEmitter, origin: EventOrigin) {
    let _ = SINK.set(Sink { emitter, origin });
}

pub fn is_installed() -> bool {
    SINK.get().is_some()
}

/// Emits `game_completed` plus one `game_player_result` per player.
///
/// A no-op when analytics is not configured, so a deployment without it runs
/// unchanged.
pub fn record_game_completed(record: &CompletionRecordV1) {
    let Some(sink) = SINK.get() else { return };
    for event in super::completion_events::project(record, &sink.origin) {
        sink.emitter.emit(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::emitter::EmitterConfig;

    /// Without a sink installed this must be a silent no-op, never a panic:
    /// gameplay runs in deployments that have no analytics at all.
    #[test]
    fn recording_without_a_sink_is_a_no_op() {
        // Uses whatever global state the test binary has; the assertion is
        // simply that this cannot panic.
        let state = common::GameState::new(
            10,
            10,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(1),
            0,
        );
        let record = CompletionRecordV1 {
            schema_version: crate::completion::COMPLETION_SCHEMA_VERSION,
            game_id: 1,
            partition_id: 0,
            revision: uuid::Uuid::new_v4(),
            ended_at_ms: 1,
            server_id: 1,
            final_state: state,
            effects: Vec::new(),
        };
        record_game_completed(&record);
    }

    #[test]
    fn installing_twice_is_ignored_rather_than_fatal() {
        let (first, _rx1) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        let (second, _rx2) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        let origin = EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "1:boot".to_owned(),
        };
        install(first, origin.clone());
        install(second, origin);
        assert!(is_installed());
    }
}
