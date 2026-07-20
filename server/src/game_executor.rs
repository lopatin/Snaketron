use crate::db::Database;
use crate::game_bus::GameBus;
use crate::pubsub_manager::{PartitionSubscription, SnapshotRequest};
use crate::redis_keys::RedisKeys;
use crate::replication::GameStateReader;
use crate::sync_trace::GameTraceRecorder;
use crate::xp_persistence;
use anyhow::{Context, Result};
use common::trace::{TRACE_FORMAT_VERSION, TraceRecord, TraceSide};
use common::{
    EXECUTOR_POLL_INTERVAL_MS, GameCommandMessage, GameEngine, GameEvent, GameEventMessage,
    GameState, GameStatus, TICK_HASH_INTERVAL_TICKS,
};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub const PARTITION_COUNT: u32 = 10;
const COMPLETED_GAME_PERSIST_ATTEMPTS: usize = 12;
const COMPLETED_GAME_PERSIST_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

async fn persist_completed_game_with_retry(
    db: Arc<dyn Database>,
    game_id: i32,
    server_id: i32,
    game_state: GameState,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=COMPLETED_GAME_PERSIST_ATTEMPTS {
        match tokio::time::timeout(
            COMPLETED_GAME_PERSIST_ATTEMPT_TIMEOUT,
            db.upsert_completed_game(game_id, server_id, &game_state),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(anyhow::anyhow!(
                    "completed-game persistence attempt timed out after {:?}",
                    COMPLETED_GAME_PERSIST_ATTEMPT_TIMEOUT
                ));
            }
        }

        if attempt < COMPLETED_GAME_PERSIST_ATTEMPTS {
            let backoff_ms = (100_u64 << (attempt - 1).min(6)).min(5_000);
            warn!(
                "Completed game {} persistence attempt {}/{} failed; retrying in {}ms: {:?}",
                game_id,
                attempt,
                COMPLETED_GAME_PERSIST_ATTEMPTS,
                backoff_ms,
                last_error.as_ref().expect("attempt recorded an error")
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("completed-game persistence failed")))
}

const BUS_PUBLISH_ATTEMPTS: usize = 3;
const BUS_PUBLISH_INITIAL_BACKOFF_MS: u64 = 25;

/// Publish an event with a small bounded retry so a single transient bus error
/// does not permanently drop the message for every consumer. Retries complete
/// before the caller moves on to the next message, preserving publish order.
/// A double-publish on ambiguous failure is safe: consumers skip stale and
/// duplicate stream_seqs.
async fn publish_event_with_retry(
    bus: &GameBus,
    partition_id: u32,
    event: &GameEventMessage,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=BUS_PUBLISH_ATTEMPTS {
        match bus.publish_event(partition_id, event).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }

        if attempt < BUS_PUBLISH_ATTEMPTS {
            let backoff_ms = BUS_PUBLISH_INITIAL_BACKOFF_MS << (attempt - 1);
            warn!(
                "Event publish attempt {}/{} for game {} stream_seq {} failed; retrying in {}ms: {:?}",
                attempt,
                BUS_PUBLISH_ATTEMPTS,
                event.game_id,
                event.stream_seq,
                backoff_ms,
                last_error.as_ref().expect("attempt recorded an error")
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("event publish failed")))
}

/// Snapshot counterpart of [`publish_event_with_retry`], with the same
/// ordering and duplicate-safety guarantees.
async fn publish_snapshot_with_retry(
    bus: &GameBus,
    partition_id: u32,
    game_id: u32,
    snapshot: &GameState,
    stream_seq: u64,
) -> Result<GameEventMessage> {
    let mut last_error = None;

    for attempt in 1..=BUS_PUBLISH_ATTEMPTS {
        match bus
            .publish_snapshot(partition_id, game_id, snapshot, stream_seq)
            .await
        {
            Ok(snapshot_event) => return Ok(snapshot_event),
            Err(error) => last_error = Some(error),
        }

        if attempt < BUS_PUBLISH_ATTEMPTS {
            let backoff_ms = BUS_PUBLISH_INITIAL_BACKOFF_MS << (attempt - 1);
            warn!(
                "Snapshot publish attempt {}/{} for game {} stream_seq {} failed; retrying in {}ms: {:?}",
                attempt,
                BUS_PUBLISH_ATTEMPTS,
                game_id,
                stream_seq,
                backoff_ms,
                last_error.as_ref().expect("attempt recorded an error")
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("snapshot publish failed")))
}

// Snapshot-bearing events are message envelopes; boxing would add churn without a win.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StreamEvent {
    GameCreated {
        game_id: u32,
        game_state: GameState,
    },
    GameCommandSubmitted {
        game_id: u32,
        user_id: u32,
        command: GameCommandMessage,
    },
    // GameEvent(GameEventMessage),
    StatusUpdated {
        game_id: u32,
        status: GameStatus,
    },
}

/// Decides when to emit a TickHash heartbeat: every `interval_ticks` committed
/// ticks, or the equivalent span of wall time when the committed tick is not
/// advancing (pre-start, completion pending) so it doubles as a liveness
/// signal.
pub(crate) struct TickHashCadence {
    interval_ticks: u32,
    interval_ms: i64,
    last_tick: u32,
    last_ms: i64,
}

impl TickHashCadence {
    pub(crate) fn new(
        interval_ticks: u32,
        tick_duration_ms: u32,
        start_tick: u32,
        start_ms: i64,
    ) -> Self {
        Self {
            interval_ticks,
            interval_ms: interval_ticks as i64 * tick_duration_ms.max(1) as i64,
            last_tick: start_tick,
            last_ms: start_ms,
        }
    }

    pub(crate) fn due(&self, tick: u32, now_ms: i64) -> bool {
        tick >= self.last_tick + self.interval_ticks || now_ms >= self.last_ms + self.interval_ms
    }

    pub(crate) fn mark(&mut self, tick: u32, now_ms: i64) {
        self.last_tick = tick;
        self.last_ms = now_ms;
    }
}

/// Create a game engine and run the game loop for a specific game.
#[allow(clippy::too_many_arguments)]
async fn run_game(
    server_id: u64,
    game_id: u32,
    game_state: GameState,
    bus: Arc<GameBus>,
    mut command_receiver: mpsc::Receiver<GameCommandMessage>,
    mut snapshot_request_receiver: mpsc::Receiver<SnapshotRequest>,
    db: Arc<dyn Database>,
    cancellation_token: CancellationToken,
) {
    info!("run_game called for game {}", game_id);
    let partition_id = game_id % PARTITION_COUNT;

    // Transport-level sequence for every message this executor publishes for
    // the game. Strictly monotonic starting at 1; receivers detect lost
    // messages via contiguity.
    let mut stream_seq: u64 = 0;

    // If the game is in Stopped status, start it before creating the engine
    let mut initial_state = game_state;
    let publish_started = matches!(initial_state.status, GameStatus::Stopped);
    if publish_started {
        info!("Game {} is in Stopped status, starting it", game_id);
        initial_state.status = GameStatus::Started { server_id };
    }

    // Flight recorder: Meta + full initial state (including rng) form the
    // deterministic replay anchor. Recorder failures never break the loop.
    let mut recorder = GameTraceRecorder::for_server_game(game_id);
    let now_ms = chrono::Utc::now().timestamp_millis();
    recorder.record(&TraceRecord::Meta {
        version: TRACE_FORMAT_VERSION,
        side: TraceSide::Server,
        game_id,
        session: server_id.to_string(),
        ts_ms: now_ms,
        build: env!("CARGO_PKG_VERSION").to_string(),
        tick_duration_ms: initial_state.properties.tick_duration_ms,
    });
    recorder.record(&TraceRecord::State {
        ts_ms: now_ms,
        tick: initial_state.tick,
        state: Box::new(initial_state.clone()),
    });

    if publish_started {
        // Emit status update event
        stream_seq += 1;
        let status_event = GameEventMessage {
            game_id,
            tick: initial_state.tick,
            sequence: initial_state.event_sequence + 1,
            stream_seq,
            user_id: None,
            event: GameEvent::StatusUpdated {
                status: GameStatus::Started { server_id },
            },
        };

        let publish_result = publish_event_with_retry(&bus, partition_id, &status_event).await;
        recorder.record(&TraceRecord::EventOut {
            ts_ms: now_ms,
            msg: Box::new(status_event),
        });
        if let Err(e) = publish_result {
            // The initial snapshot published right below re-anchors consumers.
            error!("Failed to publish game started status: {}", e);
            recorder.note(format!("failed to publish game started status: {}", e));
        }
    }

    let mut engine = GameEngine::new_from_state(game_id, initial_state);
    info!(
        "Created game engine for game {} with status: {:?}",
        game_id,
        engine.get_committed_state().status
    );

    // Publish initial snapshot
    stream_seq += 1;
    match publish_snapshot_with_retry(
        &bus,
        partition_id,
        game_id,
        engine.get_committed_state(),
        stream_seq,
    )
    .await
    {
        Ok(snapshot_event) => {
            recorder.record(&TraceRecord::EventOut {
                ts_ms: chrono::Utc::now().timestamp_millis(),
                msg: Box::new(snapshot_event),
            });
        }
        Err(e) => {
            error!("Failed to publish initial snapshot: {}", e);
            recorder.note(format!("failed to publish initial snapshot: {}", e));
        }
    }

    let mut hash_cadence = TickHashCadence::new(
        TICK_HASH_INTERVAL_TICKS,
        engine.get_committed_state().properties.tick_duration_ms,
        engine.current_tick(),
        chrono::Utc::now().timestamp_millis(),
    );

    let mut interval = tokio::time::interval(Duration::from_millis(EXECUTOR_POLL_INTERVAL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;

            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                recorder.note("game loop cancelled");
                break;
            }

            // Handle snapshot requests
            Some(request) = snapshot_request_receiver.recv() => {
                debug!("Received snapshot request for partition {}", request.partition_id);
                // Only publish snapshot if this game belongs to the requested partition
                if game_id % PARTITION_COUNT == request.partition_id {
                    recorder.note(format!(
                        "snapshot requested for partition {} by {:?}",
                        request.partition_id, request.requester_id
                    ));
                    stream_seq += 1;
                    match publish_snapshot_with_retry(&bus, partition_id, game_id, engine.get_committed_state(), stream_seq).await {
                        Ok(snapshot_event) => {
                            recorder.record(&TraceRecord::EventOut {
                                ts_ms: chrono::Utc::now().timestamp_millis(),
                                msg: Box::new(snapshot_event),
                            });
                        }
                        Err(e) => {
                            error!("Failed to publish requested snapshot: {}", e);
                            recorder.note(format!("failed to publish requested snapshot: {}", e));
                        }
                    }
                }
            }

            // Process commands from the channel
            Some(command) = command_receiver.recv() => {
                debug!("Processing command for game {}. Command: {:?}",
                    game_id, command);

                let now_ms = chrono::Utc::now().timestamp_millis();
                recorder.record(&TraceRecord::CmdIn {
                    ts_ms: now_ms,
                    cmd: command.clone(),
                });

                // Process the command through the game engine
                match engine.process_command(command) {
                    Ok(scheduled_command) => {
                        // Emit CommandScheduled event
                        let event = GameEvent::CommandScheduled { command_message: scheduled_command };
                        let current_state = engine.get_committed_state();
                        stream_seq += 1;
                        let event_msg = GameEventMessage {
                            game_id,
                            tick: engine.current_tick(),
                            sequence: current_state.event_sequence + 1,
                            stream_seq,
                            user_id: None,
                            event,
                        };

                        // Publish event via PubSub
                        let publish_result = publish_event_with_retry(&bus, partition_id, &event_msg).await;
                        recorder.record(&TraceRecord::EventOut {
                            ts_ms: now_ms,
                            msg: Box::new(event_msg),
                        });
                        if let Err(e) = publish_result {
                            warn!("Failed to publish command scheduled event: {}", e);
                            recorder.note(format!("failed to publish command scheduled event: {}", e));

                            // The scheduled command is lost for every consumer;
                            // re-anchor them with a fresh snapshot instead of
                            // waiting for stream_seq gap detection.
                            stream_seq += 1;
                            match publish_snapshot_with_retry(&bus, partition_id, game_id, engine.get_committed_state(), stream_seq).await {
                                Ok(snapshot_event) => {
                                    recorder.record(&TraceRecord::EventOut {
                                        ts_ms: now_ms,
                                        msg: Box::new(snapshot_event),
                                    });
                                }
                                Err(e) => {
                                    error!("Failed to publish compensating snapshot: {}", e);
                                    recorder.note(format!("failed to publish compensating snapshot: {}", e));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to process command for game {}: {:?}", game_id, e);
                        recorder.note(format!("failed to process command: {:?}", e));
                    }
                }
            }

            _ = interval.tick() => {
                // Run game ticks
                let now_ms = chrono::Utc::now().timestamp_millis();
                match engine.run_until(now_ms) {
                    Ok(events) => {
                        let game_state = engine.get_committed_state();
                        let game_completed =
                            matches!(game_state.status, GameStatus::Complete { .. });

                        let completed_game_persistence = if game_completed {
                            // Establish the terminal grace-period cache before any Complete event
                            // can evict replicas. This keeps refreshes loadable while durable
                            // persistence retries outside the client-visible completion path.
                            if let Err(e) = bus.store_snapshot(game_id, game_state).await {
                                error!(
                                    "Failed to store terminal reload snapshot for game {}: {:?}",
                                    game_id, e
                                );
                            }

                            match (i32::try_from(game_id), i32::try_from(server_id)) {
                                (Ok(database_game_id), Ok(database_server_id)) => {
                                    let persistence_db = db.clone();
                                    let persistence_state = game_state.clone();
                                    Some(tokio::spawn(async move {
                                        persist_completed_game_with_retry(
                                            persistence_db,
                                            database_game_id,
                                            database_server_id,
                                            persistence_state,
                                        )
                                        .await
                                    }))
                                }
                                _ => {
                                    error!(
                                        "Cannot persist final state for game {} on server {} because an ID exceeds the database range",
                                        game_id, server_id
                                    );
                                    None
                                }
                            }
                        } else {
                            None
                        };

                        let mut event_publish_failed = false;
                        for (tick, sequence, event) in &events {
                            stream_seq += 1;
                            let event_msg = GameEventMessage {
                                game_id,
                                tick: *tick,
                                sequence: *sequence,
                                stream_seq,
                                user_id: None,
                                event: event.clone(),
                            };

                            // Publish event via PubSub
                            let publish_result = publish_event_with_retry(&bus, partition_id, &event_msg).await;
                            recorder.record(&TraceRecord::EventOut {
                                ts_ms: now_ms,
                                msg: Box::new(event_msg),
                            });
                            if let Err(e) = publish_result {
                                event_publish_failed = true;
                                warn!("Failed to publish game event: {}", e);
                                recorder.note(format!("failed to publish game event: {}", e));
                            }
                        }

                        let committed_tick = engine.current_tick();

                        // Publish the TickHash heartbeat: on the tick cadence,
                        // on the equivalent wall-clock cadence when ticks are
                        // not advancing, and immediately on completion (final
                        // authoritative hash).
                        if game_completed || hash_cadence.due(committed_tick, now_ms) {
                            stream_seq += 1;
                            let hash = engine.committed_sync_hash();
                            let hash_msg = GameEventMessage {
                                game_id,
                                tick: committed_tick,
                                sequence: engine.get_committed_state().event_sequence,
                                stream_seq,
                                user_id: None,
                                event: GameEvent::TickHash { hash, server_ts_ms: now_ms },
                            };
                            let publish_result = publish_event_with_retry(&bus, partition_id, &hash_msg).await;
                            recorder.record(&TraceRecord::EventOut {
                                ts_ms: now_ms,
                                msg: Box::new(hash_msg),
                            });
                            if let Err(e) = publish_result {
                                event_publish_failed = true;
                                warn!("Failed to publish tick hash: {}", e);
                                recorder.note(format!("failed to publish tick hash: {}", e));
                            }
                            recorder.record(&TraceRecord::Fingerprint {
                                ts_ms: now_ms,
                                tick: committed_tick,
                                hash,
                            });
                            recorder.flush();
                            hash_cadence.mark(committed_tick, now_ms);

                            // Refresh the stored (not published) snapshot at
                            // the same cadence so a takeover executor can
                            // resume this game from <=1s-stale state.
                            if let Err(e) = bus
                                .store_snapshot(game_id, engine.get_committed_state())
                                .await
                            {
                                warn!("Failed to refresh stored snapshot for game {}: {}", game_id, e);
                                recorder.note(format!("failed to refresh stored snapshot: {}", e));
                            }
                        }

                        // A permanently lost event strands every consumer on a
                        // stale committed state until stream_seq gap detection
                        // kicks in; re-anchor them proactively with a fresh
                        // snapshot. Skipped on completion because the final
                        // snapshot below supersedes it.
                        if event_publish_failed && !game_completed {
                            stream_seq += 1;
                            match publish_snapshot_with_retry(&bus, partition_id, game_id, engine.get_committed_state(), stream_seq).await {
                                Ok(snapshot_event) => {
                                    recorder.record(&TraceRecord::EventOut {
                                        ts_ms: now_ms,
                                        msg: Box::new(snapshot_event),
                                    });
                                }
                                Err(e) => {
                                    error!("Failed to publish compensating snapshot for game {}: {}", game_id, e);
                                    recorder.note(format!("failed to publish compensating snapshot: {}", e));
                                }
                            }
                        }

                        // Check if game has completed
                        if game_completed {
                            info!("Game {} has completed, exiting game loop", game_id);

                            // Publish final snapshot
                            stream_seq += 1;
                            match publish_snapshot_with_retry(&bus, partition_id, game_id, game_state, stream_seq).await {
                                Ok(snapshot_event) => {
                                    recorder.record(&TraceRecord::EventOut {
                                        ts_ms: now_ms,
                                        msg: Box::new(snapshot_event),
                                    });
                                }
                                Err(e) => {
                                    warn!("Failed to publish final snapshot: {}", e);
                                    recorder.note(format!("failed to publish final snapshot: {}", e));
                                }
                            }

                            // Completion events and the final Redis snapshot are already visible
                            // to clients. Wait for the bounded durable retry before tearing down the
                            // executor, without holding up the game-over UI.
                            let completion_is_durable = if let Some(persistence) = completed_game_persistence {
                                match persistence.await {
                                    Ok(Ok(())) => true,
                                    Ok(Err(e)) => {
                                        error!(
                                            "Failed to persist final state for game {} after retries; retaining the replication cache: {:?}",
                                            game_id, e
                                        );
                                        false
                                    }
                                    Err(e) => {
                                        error!(
                                            "Completed-game persistence task for game {} failed; retaining the replication cache: {:?}",
                                            game_id, e
                                        );
                                        false
                                    }
                                }
                            } else {
                                false
                            };

                            // Replicas use this command as the durable-commit marker. Never evict
                            // the authoritative completed state when persistence did not succeed.
                            if completion_is_durable
                                && let Err(e) = bus.publish_command(
                                    partition_id,
                                    &StreamEvent::StatusUpdated {
                                        game_id,
                                        status: game_state.status.clone(),
                                    },
                                ).await {
                                    warn!("Failed to publish game completion command for {}: {}", game_id, e);
                                }

                            // Persist XP to database
                            if !game_state.player_xp.is_empty() {
                                info!("Persisting XP for {} players in game {}", game_state.player_xp.len(), game_id);
                                if let Err(e) = xp_persistence::persist_player_xp(db.as_ref(), game_id, game_state.player_xp.clone()).await {
                                    error!("Failed to persist XP for game {}: {:?}", game_id, e);
                                }
                            }

                            // Persist MMR changes to database
                            if !game_state.players.is_empty() {
                                info!("Persisting MMR for {} players in game {}", game_state.players.len(), game_id);
                                if let Err(e) = crate::mmr_persistence::persist_player_mmr(db.as_ref(), game_id, game_state).await {
                                    error!("Failed to persist MMR for game {}: {:?}", game_id, e);
                                }
                            }

                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error running game tick: {:?}", e);
                        recorder.note(format!("error running game tick: {:?}", e));
                    }
                }
            }
        }
    }

    // Final state + flush regardless of how the loop ended (completion,
    // cancellation, error) so the trace always has a terminal anchor.
    let final_state = engine.get_committed_state();
    recorder.record(&TraceRecord::State {
        ts_ms: chrono::Utc::now().timestamp_millis(),
        tick: final_state.tick,
        state: Box::new(final_state.clone()),
    });
    recorder.flush();
}

/// Load every stored game snapshot from Redis (game:snapshot:* keys, written
/// by publish_snapshot and refreshed at TickHash cadence by live game loops).
/// Unreadable entries are skipped with a warning — resume is best-effort.
pub async fn load_stored_snapshots(redis: &mut ConnectionManager) -> Vec<(u32, GameState)> {
    use redis::AsyncCommands;

    let prefix = crate::redis_keys::RedisKeys::game_snapshot(0);
    let prefix = prefix.trim_end_matches('0');
    let pattern = format!("{}*", prefix);

    let keys: Vec<String> = {
        let mut keys = Vec::new();
        match redis.scan_match::<_, String>(&pattern).await {
            Ok(mut iter) => {
                while let Some(key) = iter.next_item().await {
                    keys.push(key);
                }
            }
            Err(e) => {
                warn!("Failed to scan stored game snapshots: {}", e);
                return Vec::new();
            }
        }
        keys
    };

    let mut games = Vec::new();
    for key in keys {
        let Some(game_id) = key
            .strip_prefix(prefix)
            .and_then(|id| id.parse::<u32>().ok())
        else {
            continue;
        };
        match redis.get::<_, Option<Vec<u8>>>(&key).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<GameState>(&bytes) {
                Ok(state) => games.push((game_id, state)),
                Err(e) => warn!("Skipping unreadable stored snapshot {}: {}", key, e),
            },
            Ok(None) => {} // expired between scan and get
            Err(e) => warn!("Failed to read stored snapshot {}: {}", key, e),
        }
    }
    games
}

/// Choose which games a freshly started partition executor must resume.
/// Replica states (event-current) take precedence over stored snapshots
/// (periodically refreshed, slightly staler). Completed games are never
/// resumed; Stopped ones are (their GameCreated message may have been lost
/// while no executor was listening — run_game starts them).
pub fn select_resumable_games(
    partition_id: u32,
    replica_games: Vec<(u32, GameState)>,
    snapshot_games: Vec<(u32, GameState)>,
) -> Vec<(u32, GameState)> {
    let mut by_id: HashMap<u32, GameState> = HashMap::new();
    for (game_id, state) in snapshot_games {
        by_id.insert(game_id, state);
    }
    for (game_id, state) in replica_games {
        by_id.insert(game_id, state);
    }

    let mut resumable: Vec<(u32, GameState)> = by_id
        .into_iter()
        .filter(|(game_id, _)| game_id % PARTITION_COUNT == partition_id)
        .filter(|(_, state)| !matches!(state.status, GameStatus::Complete { .. }))
        .collect();
    resumable.sort_by_key(|(game_id, _)| *game_id);
    resumable
}

/// Run the game executor service for a specific partition
pub async fn run_game_executor(
    server_id: u64,
    partition_id: u32,
    mut redis: ConnectionManager,
    bus: Arc<GameBus>,
    db: Arc<dyn Database>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!(
        "Starting game executor for server {} partition {}",
        server_id, partition_id
    );

    // Create PubSub manager
    // let mut pubsub = PubSubManager::new(&redis_url)
    //     .await
    //     .context("Failed to create PubSub manager")?;

    // Subscribe to partition commands and snapshot requests
    let partition_sub = bus
        .subscribe_to_partition(partition_id)
        .await
        .context("Failed to subscribe to partition")?;

    let PartitionSubscription {
        partition_id: _,
        mut event_receiver,
        mut command_receiver,
        mut snapshot_request_receiver,
    } = partition_sub;

    // Track game channels
    let mut game_channels: HashMap<
        u32,
        (
            mpsc::Sender<GameCommandMessage>,
            mpsc::Sender<SnapshotRequest>,
        ),
    > = HashMap::new();

    let try_start_game = |game_id: u32,
                          game_state: GameState,
                          bus: Arc<GameBus>,
                          db: Arc<dyn Database>,
                          cancellation_token: CancellationToken,
                          game_channels: &mut HashMap<
        u32,
        (
            mpsc::Sender<GameCommandMessage>,
            mpsc::Sender<SnapshotRequest>,
        ),
    >| {
        if game_id % PARTITION_COUNT != partition_id {
            debug!(
                "Game {} belongs to partition {}, not partition {}",
                game_id,
                game_id % PARTITION_COUNT,
                partition_id
            );
            return false;
        }

        if game_channels.contains_key(&game_id) {
            debug!("Game {} is already running", game_id);
            return true;
        }

        info!("Partition {} will start game {}", partition_id, game_id);

        // Create channels for this game
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (snap_tx, snap_rx) = mpsc::channel(10);
        game_channels.insert(game_id, (cmd_tx, snap_tx));

        tokio::spawn(async move {
            // Run the game loop
            run_game(
                server_id,
                game_id,
                game_state,
                bus,
                cmd_rx,
                snap_rx,
                db,
                cancellation_token,
            )
            .await;
            info!("Game {} has ended", game_id);
        });

        true
    };

    // Resume in-flight games. This executor only starts once this server
    // holds the partition lease, so any game found here belongs to an
    // executor that died or lost its lease — without this, those games were
    // cancelled forever and later player commands silently discarded. The
    // replica state (event-current) wins over stored Redis snapshots
    // (refreshed at TickHash cadence, so <=~1s stale); snapshots cover games
    // this server's replica never saw, including games whose GameCreated
    // pub/sub message was lost while no executor was listening. run_game
    // re-anchors clients by publishing a fresh snapshot first, which resets
    // stream_seq watermarks all the way down.
    {
        let replica_games = replication_manager.get_partition_games(partition_id).await;
        let snapshot_games = load_stored_snapshots(&mut redis).await;
        let resumable = select_resumable_games(partition_id, replica_games, snapshot_games);
        if !resumable.is_empty() {
            info!(
                "Partition {} resuming {} in-flight game(s) after executor (re)start: {:?}",
                partition_id,
                resumable.len(),
                resumable.iter().map(|(id, _)| *id).collect::<Vec<_>>()
            );
        }
        for (game_id, game_state) in resumable {
            // Completion-race guard: the durable store may already hold this
            // game's terminal state while Redis still has the immediately
            // preceding active snapshot (the previous executor died between
            // persisting completion and the snapshot expiring). Resuming
            // would resurrect a finished game and could re-complete it with
            // a different outcome, so the durable verdict wins. DB errors
            // fall through to resume: dropping a live game is worse than
            // briefly resurrecting a finished one.
            match db.get_game_by_id(game_id as i32).await {
                Ok(Some(game)) if game.status == "complete" => {
                    info!(
                        "Partition {} not resuming game {}: durable store already records completion",
                        partition_id, game_id
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => warn!(
                    "Resume guard could not check durable state for game {}: {}",
                    game_id, e
                ),
            }
            try_start_game(
                game_id,
                game_state,
                bus.clone(),
                db.clone(),
                cancellation_token.clone(),
                &mut game_channels,
            );
        }
    }

    loop {
        tokio::select! {
            biased;

            _ = cancellation_token.cancelled() => {
                info!("Game executor service shutting down");
                break;
            }

            // Process events from partition channel
            Some(event) = event_receiver.recv() => {
                // Events flow through to replication manager automatically via PubSub
                // The replication manager is subscribed to the same partition channel
                // We just need to publish snapshots to PubSub
                if let GameEvent::Snapshot { .. } = &event.event {
                    debug!("Received snapshot event for game {} on partition {}", event.game_id, partition_id);
                }
            }

            // Process snapshot requests
            Some(request) = snapshot_request_receiver.recv() => {
                debug!("Received snapshot request for partition {}", request.partition_id);
                // Forward to all games in this partition
                if request.partition_id == partition_id {
                    for (_, snap_tx) in game_channels.values() {
                        let _ = snap_tx.send(request.clone()).await;
                    }
                }
            }

            // Process commands from PubSub
            Some(command_data) = command_receiver.recv() => {
                match command_data {
                    StreamEvent::GameCreated { game_id, game_state } => {
                        info!("Received GameCreated event for game {}", game_id);
                        let bus_clone = bus.clone();
                        let db_clone = db.clone();
                        let cancellation_token_clone = cancellation_token.clone();
                        let accepted = try_start_game(
                            game_id,
                            game_state,
                            bus_clone,
                            db_clone,
                            cancellation_token_clone,
                            &mut game_channels
                        );
                        if accepted {
                            let mut ack_redis = redis.clone();
                            if let Err(e) = ack_redis
                                .set_ex::<_, _, ()>(
                                    RedisKeys::game_creation_ack(game_id),
                                    server_id,
                                    30,
                                )
                                .await
                            {
                                warn!(
                                    "Failed to acknowledge GameCreated for game {}: {}",
                                    game_id, e
                                );
                            }
                        }
                    }
                    StreamEvent::StatusUpdated { game_id, status } => {
                        if let GameStatus::Complete { .. } = status {
                            // Game completed, remove channels
                            game_channels.remove(&game_id);
                            info!("Game {} completed", game_id);
                        }
                    }
                    StreamEvent::GameCommandSubmitted { game_id, user_id: _, command } => {
                        // Route command to the appropriate game
                        if let Some((cmd_tx, _)) = game_channels.get(&game_id) {
                            if let Err(e) = cmd_tx.send(command).await {
                                warn!("Failed to send command to game {}: {}", game_id, e);
                                // The game might have ended, remove from channels
                                game_channels.remove(&game_id);
                            }
                        } else {
                            debug!("Received command for inactive game {}", game_id);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PARTITION_COUNT, TickHashCadence, select_resumable_games};
    use common::{GameState, GameStatus, GameType, QueueMode};

    fn state(status: GameStatus, tick: u32) -> GameState {
        let mut s = GameState::new(
            10,
            10,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(1),
            0,
        );
        s.status = status;
        s.tick = tick;
        s
    }

    #[test]
    fn resume_selects_started_and_stopped_games_for_own_partition() {
        let partition = 3;
        let own_started = partition; // game_id % PARTITION_COUNT == partition
        let own_stopped = partition + PARTITION_COUNT;
        let own_complete = partition + 2 * PARTITION_COUNT;
        let other_partition = partition + 1;

        let snapshot_games = vec![
            (own_started, state(GameStatus::Started { server_id: 1 }, 50)),
            (own_stopped, state(GameStatus::Stopped, 0)),
            (
                own_complete,
                state(
                    GameStatus::Complete {
                        winning_snake_id: None,
                    },
                    99,
                ),
            ),
            (
                other_partition,
                state(GameStatus::Started { server_id: 1 }, 10),
            ),
        ];

        let resumable = select_resumable_games(partition, Vec::new(), snapshot_games);
        let ids: Vec<u32> = resumable.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![own_started, own_stopped]);
    }

    #[test]
    fn resume_prefers_replica_state_over_stored_snapshot() {
        let partition = 0;
        let game_id = PARTITION_COUNT; // partition 0
        let replica = vec![(game_id, state(GameStatus::Started { server_id: 1 }, 120))];
        let snapshot = vec![(game_id, state(GameStatus::Started { server_id: 1 }, 90))];

        let resumable = select_resumable_games(partition, replica, snapshot);
        assert_eq!(resumable.len(), 1);
        assert_eq!(
            resumable[0].1.tick, 120,
            "replica (event-current) state must win"
        );
    }

    #[test]
    fn resume_skips_games_completed_per_replica_even_if_snapshot_is_stale() {
        let partition = 0;
        let game_id = PARTITION_COUNT;
        let replica = vec![(
            game_id,
            state(
                GameStatus::Complete {
                    winning_snake_id: Some(0),
                },
                200,
            ),
        )];
        let snapshot = vec![(game_id, state(GameStatus::Started { server_id: 1 }, 150))];

        let resumable = select_resumable_games(partition, replica, snapshot);
        assert!(
            resumable.is_empty(),
            "a completed game must never be resurrected"
        );
    }

    #[test]
    fn tick_hash_due_on_tick_cadence() {
        let cadence = TickHashCadence::new(10, 100, 0, 0);
        assert!(!cadence.due(5, 100));
        assert!(cadence.due(10, 100));
        assert!(cadence.due(15, 100));
    }

    #[test]
    fn tick_hash_due_on_wall_clock_when_ticks_stall() {
        // 10 ticks * 100ms = 1000ms heartbeat interval.
        let cadence = TickHashCadence::new(10, 100, 0, 0);
        assert!(!cadence.due(0, 999));
        assert!(cadence.due(0, 1000));
    }

    #[test]
    fn tick_hash_mark_resets_both_cadences() {
        let mut cadence = TickHashCadence::new(10, 100, 0, 0);
        assert!(cadence.due(10, 500));
        cadence.mark(10, 500);
        assert!(!cadence.due(19, 1499));
        assert!(cadence.due(20, 600));
        assert!(cadence.due(19, 1500));
    }
}
