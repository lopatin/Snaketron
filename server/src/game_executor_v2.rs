//! Crash-authoritative, fenced partition executor.

use crate::completion::{CompletionRecordV1, EffectApplyResult, materialize_completion};
use crate::db::Database;
use crate::game_bus::{CommandDelivery, CommandDeliveryPayload, GameBus, SnapshotRequest};
use crate::game_executor::{PARTITION_COUNT, StreamEvent, authorize_game_command};
use crate::partition_lease::{PartitionLeaseGuard, PartitionLeaseStore};
use crate::recovery::{
    CommandDecisionV1, CommandOutcome, RecoveryConfig, RecoveryEnvelopeV2, ResolvedCommandState,
    stream_id_leq, validate_client_command_identity,
};
use anyhow::{Context, Result, bail};
use common::{
    ClientCommandIdentityV2, GameCommandMessage, GameEngine, GameEvent, GameEventMessage,
    GameStatus,
};
use futures_util::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

const HANDOFF_BARRIER_TIMEOUT: Duration = Duration::from_secs(10);
const SNAPSHOT_FANOUT_TIMEOUT: Duration = Duration::from_secs(3);
const LEASE_RENEW_INTERVAL: Duration = Duration::from_millis(150);
const COMPLETION_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const COMPLETION_MATERIALIZATION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

fn is_retryable_checkpoint_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<redis::RedisError>().is_some()
            || cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
    })
}

fn persisted_checkpoint_age(
    checkpointed_at_ms: i64,
    now_ms: i64,
    max_checkpoint_age: Duration,
) -> Duration {
    let age_ms =
        u64::try_from(now_ms.saturating_sub(checkpointed_at_ms).max(0)).unwrap_or(u64::MAX);
    Duration::from_millis(age_ms).min(max_checkpoint_age)
}

async fn materialize_completion_game_local<F>(
    game_id: u32,
    retry_at: &mut Option<Instant>,
    attempt: F,
) -> Option<CompletionRecordV1>
where
    F: std::future::Future<Output = Result<CompletionRecordV1>>,
{
    let now = Instant::now();
    if retry_at.is_some_and(|deadline| now < deadline) {
        return None;
    }
    match attempt.await {
        Ok(record) => {
            *retry_at = None;
            Some(record)
        }
        Err(error) => {
            // Competitive completion needs a DynamoDB MMR read before the
            // immutable Redis record can be created. That read is game-local
            // work: a transient failure must not cancel the shared partition
            // or unrelated actors.
            *retry_at = Some(Instant::now() + COMPLETION_MATERIALIZATION_RETRY_INTERVAL);
            warn!(
                game_id,
                %error,
                "completion materialization failed; retrying this game"
            );
            None
        }
    }
}

pub struct PartitionExecutorV2Handle {
    control: mpsc::Sender<ExecutorControl>,
    handoff_cancel: CancellationToken,
}

impl Clone for PartitionExecutorV2Handle {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            handoff_cancel: self.handoff_cancel.clone(),
        }
    }
}

impl PartitionExecutorV2Handle {
    /// Stops intake, barriers every game actor, then compare-deletes the exact
    /// lease. Uncheckpointed commands remain in the durable PEL/decision
    /// journal for the successor's crash-authoritative recovery path. The
    /// caller owns the global ECS deadline.
    pub async fn handoff(&self) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        if self
            .control
            .send(ExecutorControl::Handoff { reply })
            .await
            .is_err()
        {
            // Assignment watching is an equally authoritative cooperative
            // handoff initiator. A closed control channel means that path (or
            // crash recovery) already owns progress; there is no local actor
            // left for this caller to drain.
            return Ok(());
        }
        // Publish the typed request before setting the sticky cancellation
        // flag. If a caller is aborted while a full channel is being awaited,
        // this prevents a flag-only executor that stops dispatching but keeps
        // renewing indefinitely. After enqueue succeeds, cancellation promptly
        // interrupts any partition-local bootstrap or completion work that is
        // delaying the control loop.
        self.handoff_cancel.cancel();
        match receive.await {
            Ok(result) => result,
            Err(_) => Ok(()),
        }
    }
}

enum ExecutorControl {
    Handoff { reply: oneshot::Sender<Result<()>> },
}

pub(crate) enum LeaseWatchdogEvent {
    AssignmentMoved,
    AuthorityLost,
    Failed(anyhow::Error),
}

#[derive(Debug)]
struct LeaseWatchdogBudget {
    last_confirmed_at: Instant,
    ttl: Duration,
    operation_timeout: Duration,
}

impl LeaseWatchdogBudget {
    fn new(last_confirmed_at: Instant, ttl: Duration, operation_timeout: Duration) -> Self {
        debug_assert!(operation_timeout < ttl);
        Self {
            last_confirmed_at,
            ttl,
            operation_timeout,
        }
    }

    /// Stop accepting work with one full bounded Redis operation still left
    /// on the last confirmed lease. This also covers response latency from the
    /// acquire/renew operation that established `last_confirmed_at`.
    fn fail_closed_at(&self) -> Instant {
        self.last_confirmed_at + (self.ttl - self.operation_timeout)
    }

    fn confirm_at(&mut self, confirmed_at: Instant) {
        self.last_confirmed_at = confirmed_at;
    }

    fn can_retry_at(&self, now: Instant) -> bool {
        now < self.fail_closed_at()
    }
}

async fn probe_partition_lease(
    lease_store: &PartitionLeaseStore,
    guard: &PartitionLeaseGuard,
) -> Result<Option<LeaseWatchdogEvent>> {
    match lease_store.renew(guard).await {
        Ok(true) => Ok(None),
        Ok(false) => match lease_store.validate(guard).await {
            Ok(true) => Ok(Some(LeaseWatchdogEvent::AssignmentMoved)),
            Ok(false) => Ok(Some(LeaseWatchdogEvent::AuthorityLost)),
            Err(error) => Err(error.context("partition lease validation failed transiently")),
        },
        Err(error) => Err(error.context("partition lease renewal failed transiently")),
    }
}

pub(crate) fn spawn_lease_watchdog(
    lease_store: PartitionLeaseStore,
    guard: PartitionLeaseGuard,
    stop: CancellationToken,
    handoff_cancel: CancellationToken,
) -> (JoinHandle<()>, mpsc::Receiver<LeaseWatchdogEvent>) {
    let (events, receiver) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let mut renew = tokio::time::interval(LEASE_RENEW_INTERVAL);
        renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut budget = LeaseWatchdogBudget::new(
            guard.acquired_at(),
            lease_store.ttl(),
            lease_store.operation_timeout(),
        );
        let mut last_transient_error = None;
        loop {
            let fail_closed_at = budget.fail_closed_at();
            tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep_until(fail_closed_at) => {
                    let error = last_transient_error.take().unwrap_or_else(|| {
                        anyhow::anyhow!(
                            "partition lease renewal did not complete before its fail-closed deadline"
                        )
                    });
                    let _ = events.send(LeaseWatchdogEvent::Failed(error.context(
                        "partition lease renewal budget exhausted before possible lease expiry"
                    ))).await;
                    return;
                }
                _ = renew.tick() => {
                    match tokio::time::timeout_at(
                        fail_closed_at,
                        probe_partition_lease(&lease_store, &guard),
                    ).await {
                        Ok(Ok(None)) => {
                            budget.confirm_at(Instant::now());
                            last_transient_error = None;
                        }
                        Ok(Ok(Some(event))) => {
                            if let LeaseWatchdogEvent::AssignmentMoved = &event {
                                // Wake live command dispatch immediately. The
                                // event channel remains the typed authority
                                // result used by bootstrap and the main loop.
                                handoff_cancel.cancel();
                            }
                            let _ = events.send(event).await;
                            return;
                        }
                        Ok(Err(error)) if budget.can_retry_at(Instant::now()) => {
                            warn!(
                                partition = guard.partition(),
                                %error,
                                "partition lease probe failed transiently; retrying within lease budget"
                            );
                            last_transient_error = Some(error);
                        }
                        Ok(Err(error)) => {
                            let _ = events.send(LeaseWatchdogEvent::Failed(error.context(
                                "partition lease renewal budget exhausted before possible lease expiry"
                            ))).await;
                            return;
                        }
                        Err(error) => {
                            let _ = events.send(LeaseWatchdogEvent::Failed(
                                anyhow::Error::new(error).context(
                                    "partition lease probe reached its fail-closed deadline"
                                )
                            )).await;
                            return;
                        }
                    }
                }
            }
        }
    });
    (task, receiver)
}

fn watchdog_event_error(partition: u32, event: Option<LeaseWatchdogEvent>) -> anyhow::Error {
    match event {
        Some(LeaseWatchdogEvent::AssignmentMoved) => {
            anyhow::anyhow!("partition {partition} handoff requested during bootstrap")
        }
        Some(LeaseWatchdogEvent::AuthorityLost) | None => {
            anyhow::anyhow!("partition {partition} lease authority was lost")
        }
        Some(LeaseWatchdogEvent::Failed(error)) => {
            error.context("partition lease renewal failed closed")
        }
    }
}

async fn release_bootstrap_authority(
    lease_store: &PartitionLeaseStore,
    guard: &PartitionLeaseGuard,
    fatal: &CancellationToken,
    watchdog_stop: &CancellationToken,
) -> Result<()> {
    // Bootstrap may own detached actors or an in-flight fenced stream read.
    // Cancel those futures before compare-deleting the exact token. Redis
    // serializes the release with every same-slot fenced mutation: an old
    // operation either commits before the release while the successor is
    // still excluded, or observes the missing token and is rejected after it.
    fatal.cancel();
    watchdog_stop.cancel();
    // `false` means the exact token is already absent. That is also a
    // successful authority exit, and compare-delete can never remove a
    // successor's different token.
    let _ = lease_store
        .release(guard)
        .await
        .context("failed to release bootstrap partition authority")?;
    Ok(())
}

// Deliveries are hot-path values; avoid boxing every command for small control variants.
#[allow(clippy::large_enum_variant)]
enum GameActorMessage {
    Delivery {
        delivery: CommandDelivery,
        reply: oneshot::Sender<Result<DeliveryDisposition>>,
    },
    Activate {
        reply: oneshot::Sender<Result<()>>,
    },
    Snapshot {
        reply: oneshot::Sender<Result<()>>,
    },
    Barrier {
        reply: oneshot::Sender<Result<()>>,
    },
}

enum DeliveryDisposition {
    Incorporated,
    Quarantine { reason: String },
}

enum V2Incorporation {
    Incorporated,
    PrunedDuplicate,
    Quarantine(String),
}

enum LiveExecutorWork {
    CompletionRetry,
    SnapshotRequest(Option<SnapshotRequest>),
    Deliveries(Result<Vec<CommandDelivery>>),
}

struct GameActorSlot {
    sender: mpsc::Sender<GameActorMessage>,
    terminally_completed: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

impl GameActorSlot {
    fn has_terminal_completion(&self) -> bool {
        self.terminally_completed.load(Ordering::Acquire)
    }

    /// Returns `None` only when durable terminal completion makes rejecting
    /// and ACKing the delivery safe. Every abnormal closure remains an error,
    /// leaving the Redis pending entry available for crash recovery.
    async fn deliver(&self, delivery: CommandDelivery) -> Result<Option<DeliveryDisposition>> {
        if self._task.is_finished() {
            if self.has_terminal_completion() {
                return Ok(None);
            }
            bail!("game actor stopped before command delivery");
        }

        let (reply, receive) = oneshot::channel();
        if self
            .sender
            .send(GameActorMessage::Delivery { delivery, reply })
            .await
            .is_err()
        {
            if self.has_terminal_completion() {
                return Ok(None);
            }
            bail!("game actor stopped before command delivery");
        }

        match receive.await {
            Ok(result) => result.map(Some),
            Err(_) if self.has_terminal_completion() => Ok(None),
            Err(error) => {
                Err(anyhow::Error::new(error).context("game actor dropped delivery reply"))
            }
        }
    }
}

struct GameActor {
    server_id: u64,
    game_id: u32,
    engine: GameEngine,
    resolved: ResolvedCommandState,
    command_cursor: String,
    next_event_stream_sequence: u64,
    pending_stream_ids: Vec<String>,
    live: bool,
    start_event_pending: bool,
    completion_committed: bool,
    pending_completion: Option<CompletionRecordV1>,
    completion_materialization_retry_at: Option<Instant>,
    last_checkpoint_success: Instant,
    bus: Arc<GameBus>,
    guard: PartitionLeaseGuard,
    db: Arc<dyn Database>,
    config: RecoveryConfig,
    receiver: mpsc::Receiver<GameActorMessage>,
    fatal: CancellationToken,
    completion_cancel: CancellationToken,
}

pub(crate) fn autonomous_actor_failure(
    partition: u32,
    game_id: u32,
    error: anyhow::Error,
) -> anyhow::Error {
    error.context(format!(
        "game {game_id} actor failed autonomously in partition {partition}"
    ))
}

fn report_autonomous_actor_failure(
    actor_failures: &mpsc::UnboundedSender<anyhow::Error>,
    fatal: &CancellationToken,
    partition: u32,
    game_id: u32,
    error: anyhow::Error,
) {
    let _ = actor_failures.send(autonomous_actor_failure(partition, game_id, error));
    fatal.cancel();
}

/// An actor reports its source error before dropping any in-flight reply
/// channel. Preserve that source when dispatch observes the secondary channel
/// closure first, so Redis/timeout failures retain their partition-local
/// recovery classification instead of being promoted to a task-fatal
/// invariant error.
fn prefer_actor_failure<T>(
    dispatch_result: Result<T>,
    actor_failures: &mut mpsc::UnboundedReceiver<anyhow::Error>,
) -> Result<T> {
    match dispatch_result {
        Ok(value) => Ok(value),
        Err(dispatch_error) => Err(actor_failures.try_recv().unwrap_or(dispatch_error)),
    }
}

async fn supervise_actor_run(
    future: impl std::future::Future<Output = Result<()>>,
    actor_failures: &mpsc::UnboundedSender<anyhow::Error>,
    fatal: &CancellationToken,
    partition: u32,
    game_id: u32,
) -> bool {
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            error!(game_id, %error, "v2 game actor failed");
            report_autonomous_actor_failure(actor_failures, fatal, partition, game_id, error);
            false
        }
        Err(_) => {
            error!(game_id, "v2 game actor panicked");
            report_autonomous_actor_failure(
                actor_failures,
                fatal,
                partition,
                game_id,
                anyhow::anyhow!("game actor panicked"),
            );
            false
        }
    }
}

impl GameActor {
    // Recovery construction intentionally receives the complete fenced actor context.
    #[allow(clippy::too_many_arguments)]
    fn from_envelope(
        server_id: u64,
        envelope: RecoveryEnvelopeV2,
        bus: Arc<GameBus>,
        guard: PartitionLeaseGuard,
        db: Arc<dyn Database>,
        config: RecoveryConfig,
        receiver: mpsc::Receiver<GameActorMessage>,
        fatal: CancellationToken,
        completion_cancel: CancellationToken,
    ) -> Self {
        let persisted_checkpoint_age = persisted_checkpoint_age(
            envelope.checkpointed_at_ms,
            chrono::Utc::now().timestamp_millis(),
            config.max_checkpoint_age,
        );
        let now = Instant::now();
        let last_checkpoint_success = now.checked_sub(persisted_checkpoint_age).unwrap_or(now);
        let mut game_state = envelope.game_state;
        let start_event_pending = matches!(game_state.status, GameStatus::Stopped);
        if start_event_pending {
            game_state.status = GameStatus::Started { server_id };
        }
        Self {
            server_id,
            game_id: envelope.game_id,
            engine: GameEngine::new_from_state_with_command_counter(
                envelope.game_id,
                game_state,
                envelope.next_server_command_sequence,
            ),
            resolved: envelope.resolved_client_commands,
            command_cursor: envelope.command_cursor,
            next_event_stream_sequence: envelope.next_event_stream_sequence,
            pending_stream_ids: Vec::new(),
            live: false,
            start_event_pending,
            completion_committed: false,
            pending_completion: None,
            completion_materialization_retry_at: None,
            last_checkpoint_success,
            bus,
            guard,
            db,
            config,
            receiver,
            fatal,
            completion_cancel,
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut checkpoint = tokio::time::interval(self.config.checkpoint_interval);
        checkpoint.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first interval tick; creation/activation writes
        // the required initial checkpoint explicitly.
        checkpoint.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = self.fatal.cancelled() => return Ok(()),
                _ = checkpoint.tick(), if self.live => {
                    self.checkpoint().await?;
                }
                _ = tick.tick(), if self.live => {
                    self.advance_live().await?;
                    if self.completion_committed {
                        return Ok(());
                    }
                }
                message = self.receiver.recv() => {
                    let Some(message) = message else { return Ok(()); };
                    match message {
                        GameActorMessage::Delivery { delivery, reply } => {
                            let result = self.incorporate(delivery).await;
                            if result.is_err() { self.fatal.cancel(); }
                            let _ = reply.send(result);
                        }
                        GameActorMessage::Activate { reply } => {
                            let result = self.activate().await;
                            if result.is_err() { self.fatal.cancel(); }
                            let _ = reply.send(result);
                        }
                        GameActorMessage::Snapshot { reply } => {
                            let result = self.publish_fresh_snapshot().await;
                            if result.is_err() { self.fatal.cancel(); }
                            let _ = reply.send(result);
                        }
                        GameActorMessage::Barrier { reply } => {
                            self.live = false;
                            // FIFO mailbox ordering proves every delivery this
                            // actor accepted has finished. Do not add a second
                            // full-state checkpoint here: the periodic
                            // checkpoint plus unacked PEL entries and durable
                            // decision journal are already the authoritative
                            // crash-recovery source. The successor checkpoints
                            // recovered state before activation.
                            let _ = reply.send(Ok(()));
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn incorporate(&mut self, delivery: CommandDelivery) -> Result<DeliveryDisposition> {
        if stream_id_leq(&delivery.stream_id, &self.command_cursor)? {
            self.bus
                .xack_fenced(&self.guard, &[delivery.stream_id])
                .await?;
            return Ok(DeliveryDisposition::Incorporated);
        }
        let decision = delivery.decision;
        let stream_id = delivery.stream_id;
        let mut publish_resolution_snapshot = false;
        match delivery.payload {
            CommandDeliveryPayload::Poison { .. } => {
                bail!("poison delivery must be disposed by the partition executor")
            }
            CommandDeliveryPayload::Command(StreamEvent::GameCreated { game_id, .. }) => {
                if decision.is_some() {
                    bail!("command decision journal was attached to GameCreated");
                }
                if game_id != self.game_id {
                    bail!("GameCreated routed to the wrong actor");
                }
            }
            CommandDeliveryPayload::Command(StreamEvent::GameCommandSubmittedV2 {
                game_id,
                user_id,
                command_id,
                command,
            }) => {
                if game_id != self.game_id {
                    bail!("v2 command routed to the wrong actor");
                }
                if command_id.game_id != game_id || command_id.user_id != user_id {
                    bail!("v2 command identity was not canonicalized before actor delivery");
                }
                let recorded_outcome = if let Some(decision) = &decision {
                    decision.validate()?;
                    let (recorded_identity, recorded_outcome) = decision.identity_and_outcome()?;
                    if recorded_identity != &command_id
                        || decision.source_stream_id != stream_id
                        || decision.event.game_id != game_id
                    {
                        bail!("durable command decision does not match its stream delivery");
                    }
                    Some(recorded_outcome)
                } else {
                    None
                };
                if self.engine.get_committed_state().is_complete() {
                    if let Some(existing) = self.resolved.get(&command_id) {
                        if recorded_outcome
                            .as_ref()
                            .is_some_and(|outcome| outcome != existing)
                        {
                            bail!("completed game has a conflicting durable command decision");
                        }
                        crate::resilience_metrics::record_command_deduplications(1);
                        crate::resilience_metrics::record_command_resends(1);
                        self.bus
                            .xack_fenced(&self.guard, std::slice::from_ref(&stream_id))
                            .await?;
                        return Ok(DeliveryDisposition::Incorporated);
                    }
                    if self.resolved.is_terminally_resolved(&command_id) && decision.is_none() {
                        crate::resilience_metrics::record_command_deduplications(1);
                        crate::resilience_metrics::record_command_resends(1);
                        self.bus
                            .xack_fenced(&self.guard, std::slice::from_ref(&stream_id))
                            .await?;
                        return Ok(DeliveryDisposition::Incorporated);
                    }
                    if decision.is_some() {
                        bail!("unresolved durable command decision targets a completed checkpoint");
                    }
                    return Ok(DeliveryDisposition::Quarantine {
                        reason: "command targets a completed game".to_string(),
                    });
                }
                match self
                    .incorporate_v2_command(&stream_id, command_id, command, decision.as_ref())
                    .await?
                {
                    V2Incorporation::Incorporated => {}
                    V2Incorporation::PrunedDuplicate => {
                        publish_resolution_snapshot = self.live;
                    }
                    V2Incorporation::Quarantine(reason) => {
                        return Ok(DeliveryDisposition::Quarantine { reason });
                    }
                }
            }
            CommandDeliveryPayload::Command(StreamEvent::StatusUpdated { .. }) => {
                if decision.is_some() {
                    bail!("command decision journal was attached to a status marker");
                }
                bail!("status marker routed to a live game actor")
            }
        }
        self.command_cursor = stream_id.clone();
        self.pending_stream_ids.push(stream_id);
        if publish_resolution_snapshot {
            // The exact old outcome was intentionally pruned, but the durable
            // contiguous watermark still proves terminal resolution. Never
            // run the command again: checkpoint/ACK the duplicate and publish
            // a fresh snapshot so gateways resend that watermark.
            self.publish_fresh_snapshot().await?;
        }
        Ok(DeliveryDisposition::Incorporated)
    }

    async fn incorporate_v2_command(
        &mut self,
        stream_id: &str,
        identity: ClientCommandIdentityV2,
        command: GameCommandMessage,
        decision: Option<&CommandDecisionV1>,
    ) -> Result<V2Incorporation> {
        if let Some(decision) = decision {
            return self.incorporate_recorded_decision(&identity, decision);
        }
        if let Some(outcome) = self.resolved.get(&identity).cloned() {
            crate::resilience_metrics::record_command_deduplications(1);
            crate::resilience_metrics::record_command_resends(1);
            if self.live {
                self.publish_outcome(stream_id, &identity, &outcome, true)
                    .await?;
            }
            return Ok(V2Incorporation::Incorporated);
        }
        if self.resolved.is_terminally_resolved(&identity) {
            crate::resilience_metrics::record_command_deduplications(1);
            crate::resilience_metrics::record_command_resends(1);
            return Ok(V2Incorporation::PrunedDuplicate);
        }
        if let Err(error) = self
            .resolved
            .can_record(&identity, self.config.max_recorded_outcomes_per_session)
        {
            return Ok(V2Incorporation::Quarantine(error.to_string()));
        }

        let outcome = match authorize_game_command(
            self.engine.get_committed_state(),
            identity.user_id,
            command,
        ) {
            Ok(command) => match self.engine.process_command(command) {
                Ok(command) => CommandOutcome::Scheduled { command },
                Err(error) => CommandOutcome::Rejected {
                    reason: error.to_string(),
                },
            },
            Err(reason) => CommandOutcome::Rejected {
                reason: reason.to_string(),
            },
        };
        if matches!(outcome, CommandOutcome::Rejected { .. }) {
            crate::resilience_metrics::record_command_rejections(1);
        }
        self.resolved.record(
            &identity,
            outcome.clone(),
            self.config.max_recorded_outcomes_per_session,
        )?;
        if self.live {
            self.publish_outcome(stream_id, &identity, &outcome, false)
                .await?;
        }
        Ok(V2Incorporation::Incorporated)
    }

    fn incorporate_recorded_decision(
        &mut self,
        identity: &ClientCommandIdentityV2,
        decision: &CommandDecisionV1,
    ) -> Result<V2Incorporation> {
        decision.validate()?;
        let (recorded_identity, outcome) = decision.identity_and_outcome()?;
        if recorded_identity != identity {
            bail!("durable command decision identity changed during replay");
        }
        if decision.event.stream_seq <= self.next_event_stream_sequence {
            bail!(
                "durable command decision event watermark {} does not strictly advance recovered watermark {}",
                decision.event.stream_seq,
                self.next_event_stream_sequence
            );
        }

        if let Some(existing) = self.resolved.get(identity) {
            if existing != &outcome {
                bail!("durable command decision conflicts with checkpointed outcome");
            }
        } else if !self.resolved.is_terminally_resolved(identity) {
            self.resolved
                .can_record(identity, self.config.max_recorded_outcomes_per_session)?;
            if let CommandOutcome::Scheduled { command } = &outcome {
                self.engine.replay_scheduled_command(command.clone())?;
            }
            self.resolved.record(
                identity,
                outcome,
                self.config.max_recorded_outcomes_per_session,
            )?;
        }

        if self.engine.next_server_command_sequence() != decision.next_server_command_sequence {
            bail!(
                "durable command decision restored server counter {}, expected {}",
                self.engine.next_server_command_sequence(),
                decision.next_server_command_sequence
            );
        }
        self.next_event_stream_sequence = decision.event.stream_seq;
        Ok(V2Incorporation::Incorporated)
    }

    async fn publish_outcome(
        &mut self,
        stream_id: &str,
        identity: &ClientCommandIdentityV2,
        outcome: &CommandOutcome,
        deduplicated_replay: bool,
    ) -> Result<()> {
        let event = match outcome {
            CommandOutcome::Scheduled { command } => GameEvent::CommandScheduledV2 {
                command_id: identity.clone(),
                command_message: command.clone(),
                deduplicated_replay,
            },
            CommandOutcome::Rejected { reason } => GameEvent::CommandRejected {
                command_id: identity.clone(),
                reason: reason.clone(),
            },
        };
        let stream_seq = self
            .next_event_stream_sequence
            .checked_add(1)
            .context("command outcome stream sequence overflow")?;
        let state = self.engine.get_committed_state();
        let message = GameEventMessage {
            game_id: self.game_id,
            tick: state.tick,
            sequence: state.event_sequence,
            stream_seq,
            user_id: None,
            event,
        };
        let decision = CommandDecisionV1::new(
            stream_id.to_string(),
            self.engine.next_server_command_sequence(),
            message,
        );
        self.bus
            .publish_command_decision_fenced(&self.guard, &decision)
            .await?;
        self.next_event_stream_sequence = stream_seq;
        Ok(())
    }

    async fn publish_event(&mut self, event: GameEvent) -> Result<()> {
        // Once the engine is terminal, even an unrelated event stamped with
        // the terminal tick can make replicas derive Complete while catching
        // up. The fenced completion transaction bypasses this helper and is
        // the sole terminal publication path.
        if self.engine.get_committed_state().is_complete() {
            return Ok(());
        }
        self.next_event_stream_sequence += 1;
        let state = self.engine.get_committed_state();
        let message = GameEventMessage {
            game_id: self.game_id,
            tick: state.tick,
            sequence: state.event_sequence,
            stream_seq: self.next_event_stream_sequence,
            user_id: None,
            event,
        };
        self.bus.publish_event_fenced(&self.guard, &message).await?;
        Ok(())
    }

    fn envelope(&self) -> RecoveryEnvelopeV2 {
        RecoveryEnvelopeV2::new(
            self.game_id,
            self.guard.partition(),
            self.engine.get_committed_state().clone(),
            self.command_cursor.clone(),
            self.resolved.clone(),
            self.engine.next_server_command_sequence(),
            self.next_event_stream_sequence,
            chrono::Utc::now().timestamp_millis(),
            self.guard.encoded_token(),
        )
    }

    fn terminal_pending(&self) -> bool {
        self.engine.get_committed_state().is_complete() && !self.completion_committed
    }

    async fn checkpoint(&mut self) -> Result<()> {
        // The fenced completion transaction is the only operation allowed to
        // persist a terminal recovery state. Until it succeeds, the prior
        // non-terminal checkpoint plus unacked stream entries remain the crash
        // recovery source of truth. Refresh that checkpoint's TTL in place so
        // a prolonged materialization outage cannot age it out.
        let terminal_pending = self.engine.get_committed_state().is_complete();
        let mut retry_delay = Duration::from_millis(25);
        loop {
            let covered = self.pending_stream_ids.clone();
            let result = if terminal_pending {
                self.bus
                    .refresh_recovery_ttl_fenced(&self.guard, self.game_id, self.config.retention)
                    .await
                    .map(|_| 0)
            } else {
                self.bus
                    .checkpoint_and_ack_fenced(
                        &self.guard,
                        &self.envelope(),
                        &covered,
                        self.config.retention,
                    )
                    .await
            };
            match result {
                Ok(_) => {
                    if !terminal_pending {
                        crate::resilience_metrics::record_checkpoint_writes(1);
                        self.pending_stream_ids.clear();
                    }
                    self.last_checkpoint_success = Instant::now();
                    return Ok(());
                }
                Err(error) if !is_retryable_checkpoint_error(&error) => {
                    crate::resilience_metrics::record_checkpoint_failures(1);
                    return Err(error);
                }
                Err(error) => {
                    crate::resilience_metrics::record_checkpoint_failures(1);
                    let age = self.last_checkpoint_success.elapsed();
                    if age >= self.config.max_checkpoint_age {
                        return Err(error.context(format!(
                            "checkpoint age {:?} exceeded fail-closed budget {:?}",
                            age, self.config.max_checkpoint_age
                        )));
                    }
                    warn!(
                        game_id = self.game_id,
                        ?age,
                        ?retry_delay,
                        %error,
                        "checkpoint failed; retaining unacked work and retrying"
                    );
                    tokio::select! {
                        _ = self.fatal.cancelled() => {
                            bail!("checkpoint retry cancelled after partition authority loss");
                        }
                        _ = tokio::time::sleep(retry_delay) => {}
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_millis(500));
                }
            }
        }
    }

    async fn activate(&mut self) -> Result<()> {
        // Backlog is incorporated before this call. Catch-up mutations are
        // intentionally not emitted as deltas; one fresh snapshot reanchors all
        // consumers after the recovery checkpoint is durable.
        let now_ms = chrono::Utc::now().timestamp_millis();
        let _ = self.engine.run_until(now_ms)?;
        if self.terminal_pending() {
            // Do not checkpoint or publish the terminal state through the
            // ordinary recovery-snapshot path. A failed materialization stays
            // live and is retried by `advance_live`; a successful commit
            // publishes the one authoritative terminal snapshot atomically.
            self.live = true;
            self.commit_completion_until_handoff().await?;
            return Ok(());
        }
        self.checkpoint().await?;
        if self.start_event_pending {
            self.publish_event(GameEvent::StatusUpdated {
                status: GameStatus::Started {
                    server_id: self.server_id,
                },
            })
            .await?;
            self.start_event_pending = false;
        }
        self.publish_event(GameEvent::Snapshot {
            game_state: self.engine.get_committed_state().clone(),
        })
        .await?;
        self.live = true;
        Ok(())
    }

    async fn publish_fresh_snapshot(&mut self) -> Result<()> {
        if self.engine.get_committed_state().is_complete() {
            // The completion transaction publishes the terminal snapshot.
            // Never duplicate it through the ordinary snapshot stream or let
            // a partition-wide snapshot request wait on a database read.
            return Ok(());
        }
        self.checkpoint().await?;
        self.publish_event(GameEvent::Snapshot {
            game_state: self.engine.get_committed_state().clone(),
        })
        .await
    }

    async fn advance_live(&mut self) -> Result<()> {
        let events = self
            .engine
            .run_until(chrono::Utc::now().timestamp_millis())?;
        if self.terminal_pending() {
            // A replica fast-forwarding to any event from the terminal tick can
            // derive Complete even if the explicit status event is withheld.
            // Drop the whole transition batch and let the fenced completion
            // transaction publish one full terminal snapshot instead.
            self.commit_completion_until_handoff().await?;
            return Ok(());
        }
        for (_, _, event) in events {
            self.publish_event(event).await?;
        }
        Ok(())
    }

    async fn commit_completion(&mut self) -> Result<()> {
        let final_state = self.engine.get_committed_state().clone();
        if self.pending_completion.is_none() {
            let attempt = materialize_completion(
                self.db.as_ref(),
                self.game_id,
                self.guard.partition(),
                self.server_id,
                final_state.clone(),
                chrono::Utc::now().timestamp_millis(),
            );
            let Some(record) = materialize_completion_game_local(
                self.game_id,
                &mut self.completion_materialization_retry_at,
                attempt,
            )
            .await
            else {
                return Ok(());
            };
            record.validate()?;
            self.pending_completion = Some(record);
        }
        // Keep the exact immutable UUID/timestamp/payload for the entire
        // commit/effect/status sequence. An ambiguous Redis timeout or a later
        // effect failure must never cause a retry to materialize a conflicting
        // completion revision.
        let record = self
            .pending_completion
            .clone()
            .context("terminal game has no materialized completion record")?;
        let covered = self.pending_stream_ids.clone();
        self.bus
            .commit_completion_record_fenced(
                &self.guard,
                &self.envelope(),
                &covered,
                &record,
                self.config.retention,
            )
            .await?;
        self.pending_stream_ids.clear();
        // The immutable record and pending-effect index are now authoritative.
        // DynamoDB is deliberately decoupled from this actor: the partition
        // retry loop applies effects best-effort, and a successor resumes the
        // same record after a crash. A regional database outage must not hold
        // gameplay authority or terminate unrelated actors.
        self.completion_committed = true;
        Ok(())
    }

    /// Completion materialization may include a slow DynamoDB MMR read. Planned
    /// handoff cancels only this game-local attempt, never the shared partition
    /// or its barrier. The successor resumes from the prior non-terminal
    /// checkpoint and unacked command entries.
    async fn commit_completion_until_handoff(&mut self) -> Result<()> {
        let completion_cancel = self.completion_cancel.clone();
        tokio::select! {
            biased;
            _ = completion_cancel.cancelled() => {
                self.live = false;
                Ok(())
            }
            result = self.commit_completion() => result,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_game_executor_v2(
    server_id: u64,
    guard: PartitionLeaseGuard,
    lease_store: PartitionLeaseStore,
    bus: Arc<GameBus>,
    db: Arc<dyn Database>,
    config: RecoveryConfig,
    cancellation: CancellationToken,
) -> (PartitionExecutorV2Handle, JoinHandle<Result<()>>) {
    let (control, receiver) = mpsc::channel(4);
    let fatal = cancellation.child_token();
    // One shared signal stops both further batch dispatch and any game-local
    // completion read. Direct drain and assignment movement therefore enter
    // exactly the same cooperative handoff path.
    let handoff_cancel = fatal.child_token();
    let handle = PartitionExecutorV2Handle {
        control,
        handoff_cancel: handoff_cancel.clone(),
    };
    let task = tokio::spawn(run_game_executor_v2(
        server_id,
        guard,
        lease_store,
        bus,
        db,
        config,
        cancellation,
        fatal,
        handoff_cancel,
        receiver,
    ));
    (handle, task)
}

#[allow(clippy::too_many_arguments)]
async fn run_game_executor_v2(
    server_id: u64,
    guard: PartitionLeaseGuard,
    lease_store: PartitionLeaseStore,
    bus: Arc<GameBus>,
    db: Arc<dyn Database>,
    config: RecoveryConfig,
    cancellation: CancellationToken,
    fatal: CancellationToken,
    handoff_cancel: CancellationToken,
    mut control: mpsc::Receiver<ExecutorControl>,
) -> Result<()> {
    let partition = guard.partition();
    let (actor_failures, mut actor_failure_rx) = mpsc::unbounded_channel();
    // Authority monitoring starts before the first recovery read. Bootstrap
    // may include a large active-game set or PEL, and must never outlive the
    // three-second lease it was acquired under.
    let watchdog_stop = CancellationToken::new();
    let (watchdog, mut watchdog_rx) = spawn_lease_watchdog(
        lease_store.clone(),
        guard.clone(),
        watchdog_stop.clone(),
        handoff_cancel.clone(),
    );

    let bootstrap = async {
        // Anchor fan-out requests immediately on acquisition so a cold
        // replica's one-shot request cannot fall into bootstrap.
        let snapshot_requests = bus
            .subscribe_executor_snapshot_requests(partition)
            .await?
            .receiver;
        let envelopes = bus
            .load_partition_recovery_fenced(&guard, config.retention)
            .await?;
        if !envelopes.is_empty() {
            crate::resilience_metrics::record_recovered_games(envelopes.len() as u64);
        }
        let mut cursors: HashMap<u32, String> = envelopes
            .iter()
            .map(|envelope| (envelope.game_id, envelope.command_cursor.clone()))
            .collect();
        let mut actors = HashMap::new();
        for envelope in envelopes {
            insert_actor(
                &mut actors,
                server_id,
                envelope,
                bus.clone(),
                guard.clone(),
                db.clone(),
                config.clone(),
                fatal.clone(),
                handoff_cancel.clone(),
                actor_failures.clone(),
            );
        }

        let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
        let mut command_decisions = bus.load_command_decisions_fenced(&guard).await?;

        // Recover all PEL entries first, then every currently undelivered entry.
        loop {
            let batch = consumer.reclaim_next().await?;
            if !batch.deleted_pending_ids.is_empty() {
                bail!(
                    "pending command entries were trimmed before takeover: {:?}",
                    batch.deleted_pending_ids
                );
            }
            if !batch.deliveries.is_empty() {
                crate::resilience_metrics::record_recovery_replays(batch.deliveries.len() as u64);
            }
            let mut deliveries = batch.deliveries;
            attach_command_decisions(&mut deliveries, &mut command_decisions);
            dispatch_batch(
                deliveries,
                &mut actors,
                &mut cursors,
                server_id,
                &bus,
                &guard,
                db.clone(),
                config.clone(),
                fatal.clone(),
                handoff_cancel.clone(),
                actor_failures.clone(),
                false,
            )
            .await?;
            if batch.complete {
                break;
            }
        }
        loop {
            let mut deliveries = consumer.read_new_now().await?;
            if deliveries.is_empty() {
                break;
            }
            attach_command_decisions(&mut deliveries, &mut command_decisions);
            dispatch_batch(
                deliveries,
                &mut actors,
                &mut cursors,
                server_id,
                &bus,
                &guard,
                db.clone(),
                config.clone(),
                fatal.clone(),
                handoff_cancel.clone(),
                actor_failures.clone(),
                false,
            )
            .await?;
        }
        if !command_decisions.is_empty() {
            let mut orphaned: Vec<_> = command_decisions.into_keys().collect();
            orphaned.sort();
            bail!(
                "command decision journal contains entries without recoverable deliveries: {orphaned:?}"
            );
        }
        activate_all(&actors).await?;
        Result::<_>::Ok((snapshot_requests, consumer, actors, cursors))
    };
    tokio::pin!(bootstrap);
    let bootstrap_result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            fatal.cancel();
            Ok(None)
        }
        Some(error) = actor_failure_rx.recv() => {
            fatal.cancel();
            Err(error)
        }
        Some(ExecutorControl::Handoff { reply }) = control.recv() => {
            let result = release_bootstrap_authority(
                &lease_store,
                &guard,
                &fatal,
                &watchdog_stop,
            ).await;
            match result {
                Ok(()) => {
                    let _ = reply.send(Ok(()));
                    Ok(None)
                }
                Err(error) => {
                    // Preserve the typed Redis/timeout source for the
                    // partition supervisor. The drain caller still receives a
                    // useful error, while the old token safely falls back to
                    // expiry instead of escalating an expected handoff race
                    // into a task-fatal invariant failure.
                    let _ = reply.send(Err(anyhow::anyhow!("{error:#}")));
                    Err(error)
                }
            }
        }
        event = watchdog_rx.recv() => {
            match event {
                Some(LeaseWatchdogEvent::AssignmentMoved) => {
                    release_bootstrap_authority(
                        &lease_store,
                        &guard,
                        &fatal,
                        &watchdog_stop,
                    ).await.map(|()| None)
                }
                event => {
                    fatal.cancel();
                    Err(watchdog_event_error(partition, event))
                }
            }
        }
        result = &mut bootstrap => result.map(Some),
    };
    let Some((mut snapshot_requests, mut consumer, mut actors, mut cursors)) =
        (match bootstrap_result {
            Ok(state) => state,
            Err(error) => {
                // Bootstrap owns detached actor tasks. Cancel their fatal child
                // (and therefore any completion read) before dropping the map.
                fatal.cancel();
                watchdog_stop.cancel();
                let _ = tokio::time::timeout(Duration::from_secs(1), watchdog).await;
                return Err(error);
            }
        })
    else {
        watchdog_stop.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), watchdog).await;
        return Ok(());
    };

    let mut completion_retry = tokio::time::interval(COMPLETION_RETRY_INTERVAL);
    completion_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut completion_worker: Option<JoinHandle<Result<()>>> = None;

    let result: Result<()> = async {
        loop {
            // XREADGROUP mutates Redis before its multiplexed response reaches
            // this future. Keep the exact read future alive while normal timer
            // and snapshot branches run; dropping it would strand its assigned
            // entries in this consumer's PEL until a partition restart.
            let deliveries = {
                let read = consumer.read_new_blocking();
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            fatal.cancel();
                            return Ok(());
                        }
                        Some(error) = actor_failure_rx.recv() => {
                            fatal.cancel();
                            return Err(error);
                        }
                        _ = fatal.cancelled() => {
                            return Err(anyhow::anyhow!("partition {partition} executor failed closed"));
                        }
                        Some(ExecutorControl::Handoff { reply }) = control.recv() => {
                            watchdog_stop.cancel();
                            let result = cooperative_handoff(
                                &actors,
                                &lease_store,
                                &guard,
                                &fatal,
                                &handoff_cancel,
                            ).await;
                            let ok = result.is_ok();
                            let _ = reply.send(result);
                            if ok { return Ok(()); }
                            fatal.cancel();
                            return Err(anyhow::anyhow!("partition handoff failed"));
                        }
                        event = watchdog_rx.recv() => {
                            match event {
                                Some(LeaseWatchdogEvent::AssignmentMoved) => {
                                    cooperative_handoff(
                                        &actors,
                                        &lease_store,
                                        &guard,
                                        &fatal,
                                        &handoff_cancel,
                                    ).await?;
                                    return Ok(());
                                }
                                Some(LeaseWatchdogEvent::AuthorityLost) | None => {
                                    fatal.cancel();
                                    return Err(anyhow::anyhow!("partition {partition} lease authority was lost"));
                                }
                                Some(LeaseWatchdogEvent::Failed(error)) => {
                                    fatal.cancel();
                                    return Err(error.context("partition lease renewal failed closed"));
                                }
                            }
                        }
                        work = async {
                            tokio::select! {
                                _ = completion_retry.tick() => LiveExecutorWork::CompletionRetry,
                                request = snapshot_requests.recv() => {
                                    LiveExecutorWork::SnapshotRequest(request)
                                }
                                deliveries = &mut read => LiveExecutorWork::Deliveries(deliveries),
                            }
                        } => match work {
                            LiveExecutorWork::CompletionRetry => {
                                if completion_worker.as_ref().is_some_and(|task| task.is_finished()) {
                                    let task = completion_worker.take().expect("finished worker exists");
                                    match task.await {
                                        Ok(Ok(())) => {}
                                        Ok(Err(error)) => warn!(partition, %error, "pending completion retry failed"),
                                        Err(error) => warn!(partition, %error, "pending completion retry worker panicked"),
                                    }
                                }
                                if completion_worker.is_none() {
                                    let retry_bus = bus.clone();
                                    let retry_guard = guard.clone();
                                    let retry_db = db.clone();
                                    let retention = config.retention;
                                    completion_worker = Some(tokio::spawn(async move {
                                        drain_pending_completions(
                                            retry_bus.as_ref(),
                                            &retry_guard,
                                            retry_db.as_ref(),
                                            retention,
                                        ).await
                                    }));
                                }
                                if let Err(error) = bus.trim_executor_commands_fenced(&guard).await {
                                    warn!(partition, %error, "executor command trim failed");
                                }
                            }
                            LiveExecutorWork::SnapshotRequest(request) => {
                                let Some(request) = request else {
                                    return Err(anyhow::anyhow!("partition {partition} snapshot-request reader exited"));
                                };
                                if request.partition_id != partition {
                                    return Err(anyhow::anyhow!("snapshot request was routed to the wrong partition"));
                                }
                                publish_snapshots(&actors).await?;
                            }
                            LiveExecutorWork::Deliveries(deliveries) => break deliveries?,
                        }
                    }
                }
            };
            let dispatch_result = dispatch_batch(
                deliveries,
                &mut actors,
                &mut cursors,
                server_id,
                &bus,
                &guard,
                db.clone(),
                config.clone(),
                fatal.clone(),
                handoff_cancel.clone(),
                actor_failures.clone(),
                true,
            )
            .await;
            prefer_actor_failure(dispatch_result, &mut actor_failure_rx)?;
        }
    }
    .await;
    // No actor may outlive its partition executor after any control-loop exit.
    // This also cancels an in-flight completion read through the child token.
    fatal.cancel();
    if let Some(worker) = completion_worker {
        worker.abort();
    }
    watchdog_stop.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(1), watchdog).await;
    result
}

#[allow(clippy::too_many_arguments)]
fn insert_actor(
    actors: &mut HashMap<u32, GameActorSlot>,
    server_id: u64,
    envelope: RecoveryEnvelopeV2,
    bus: Arc<GameBus>,
    guard: PartitionLeaseGuard,
    db: Arc<dyn Database>,
    config: RecoveryConfig,
    fatal: CancellationToken,
    completion_cancel: CancellationToken,
    actor_failures: mpsc::UnboundedSender<anyhow::Error>,
) {
    let game_id = envelope.game_id;
    let (sender, receiver) = mpsc::channel(256);
    let mut actor = GameActor::from_envelope(
        server_id,
        envelope,
        bus,
        guard,
        db,
        config,
        receiver,
        fatal.clone(),
        completion_cancel.clone(),
    );
    let partition = actor.guard.partition();
    let terminally_completed = Arc::new(AtomicBool::new(false));
    let task_terminally_completed = terminally_completed.clone();
    let task = tokio::spawn(async move {
        if supervise_actor_run(actor.run(), &actor_failures, &fatal, partition, game_id).await
            && actor.completion_committed
        {
            // Publish terminal completion before dropping the actor's
            // receiver. A queued request may be dropped as the actor exits,
            // and its waiter must distinguish that benign race from failure.
            task_terminally_completed.store(true, Ordering::Release);
        }
    });
    actors.insert(
        game_id,
        GameActorSlot {
            sender,
            terminally_completed,
            _task: task,
        },
    );
}

async fn reject_or_quarantine_delivery(
    bus: &GameBus,
    guard: &PartitionLeaseGuard,
    stream_id: &str,
    command_id: Option<&ClientCommandIdentityV2>,
    reason: &str,
) -> Result<()> {
    let Some(command_id) = command_id else {
        return bus
            .quarantine_and_ack_fenced(guard, stream_id, &[], reason)
            .await;
    };
    crate::resilience_metrics::record_command_rejections(1);
    let rejection = GameEventMessage {
        game_id: command_id.game_id,
        // This outcome is deliberately outside the actor's ordered game-state
        // sequence. A zero stream sequence identifies an out-of-band outcome
        // that does not advance replica state.
        tick: 0,
        sequence: 0,
        stream_seq: 0,
        user_id: Some(command_id.user_id),
        event: GameEvent::CommandRejected {
            command_id: command_id.clone(),
            reason: reason.to_string(),
        },
    };
    bus.reject_and_ack_fenced(guard, stream_id, &rejection, reason)
        .await
        .map(|_| ())
}

fn attach_command_decisions(
    deliveries: &mut [CommandDelivery],
    decisions: &mut HashMap<String, CommandDecisionV1>,
) {
    for delivery in deliveries {
        delivery.decision = decisions.remove(&delivery.stream_id);
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_batch(
    deliveries: Vec<CommandDelivery>,
    actors: &mut HashMap<u32, GameActorSlot>,
    cursors: &mut HashMap<u32, String>,
    server_id: u64,
    bus: &Arc<GameBus>,
    guard: &PartitionLeaseGuard,
    db: Arc<dyn Database>,
    config: RecoveryConfig,
    fatal: CancellationToken,
    handoff_cancel: CancellationToken,
    actor_failures: mpsc::UnboundedSender<anyhow::Error>,
    activate_new_actors: bool,
) -> Result<()> {
    for delivery in deliveries {
        // XREADGROUP may already have returned a large batch when assignment
        // changes. Leave untouched entries in the old consumer's PEL so the
        // successor can claim them at zero idle time instead of extending the
        // ownership pause by draining the entire batch first.
        if handoff_cancel.is_cancelled() {
            return Ok(());
        }
        let stream_id = delivery.stream_id.clone();
        let game_id = match &delivery.payload {
            CommandDeliveryPayload::Poison { raw, reason } => {
                bus.quarantine_and_ack_fenced(guard, &stream_id, raw, reason)
                    .await?;
                continue;
            }
            CommandDeliveryPayload::Command(StreamEvent::GameCreated { game_id, .. })
            | CommandDeliveryPayload::Command(StreamEvent::GameCommandSubmittedV2 {
                game_id,
                ..
            })
            | CommandDeliveryPayload::Command(StreamEvent::StatusUpdated { game_id, .. }) => {
                *game_id
            }
        };
        if game_id % PARTITION_COUNT != guard.partition() {
            bus.quarantine_and_ack_fenced(
                guard,
                &stream_id,
                &[],
                "command game belongs to a different partition",
            )
            .await?;
            continue;
        }
        if let CommandDeliveryPayload::Command(StreamEvent::GameCommandSubmittedV2 {
            game_id,
            user_id,
            command_id,
            ..
        }) = &delivery.payload
        {
            let identity_error = if command_id.game_id != *game_id || command_id.user_id != *user_id
            {
                Some("v2 command identity does not match its authenticated stream envelope".into())
            } else {
                validate_client_command_identity(command_id)
                    .err()
                    .map(|error| error.to_string())
            };
            if let Some(reason) = identity_error {
                crate::resilience_metrics::record_command_rejections(1);
                bus.quarantine_and_ack_fenced(guard, &stream_id, &[], &reason)
                    .await?;
                continue;
            }
        }
        let replyable_command_id = match &delivery.payload {
            CommandDeliveryPayload::Command(StreamEvent::GameCommandSubmittedV2 {
                command_id,
                ..
            }) => Some(command_id.clone()),
            _ => None,
        };
        if let Some(cursor) = cursors.get(&game_id)
            && stream_id_leq(&stream_id, cursor)?
        {
            bus.xack_fenced(guard, &[stream_id]).await?;
            continue;
        }

        if let CommandDeliveryPayload::Command(StreamEvent::StatusUpdated {
            status: GameStatus::Complete { .. },
            ..
        }) = &delivery.payload
        {
            actors.remove(&game_id);
            bus.xack_fenced(guard, &[stream_id]).await?;
            continue;
        }

        let mut created_actor = false;
        if !actors.contains_key(&game_id) {
            let CommandDeliveryPayload::Command(StreamEvent::GameCreated { game_state, .. }) =
                &delivery.payload
            else {
                reject_or_quarantine_delivery(
                    bus,
                    guard,
                    &stream_id,
                    replyable_command_id.as_ref(),
                    "command targets an inactive game without GameCreated",
                )
                .await?;
                continue;
            };
            let envelope = RecoveryEnvelopeV2::new(
                game_id,
                guard.partition(),
                game_state.clone(),
                "0-0".to_string(),
                ResolvedCommandState::default(),
                0,
                0,
                chrono::Utc::now().timestamp_millis(),
                guard.encoded_token(),
            );
            cursors.insert(game_id, "0-0".into());
            insert_actor(
                actors,
                server_id,
                envelope,
                bus.clone(),
                guard.clone(),
                db.clone(),
                config.clone(),
                fatal.clone(),
                handoff_cancel.clone(),
                actor_failures.clone(),
            );
            created_actor = true;
        }

        let slot = actors.get(&game_id).context("game actor disappeared")?;
        let Some(disposition) = slot.deliver(delivery).await? else {
            // Only a durably completed actor permits terminal rejection. An
            // abnormal closure returns above and leaves the command pending so
            // partition restart can recover it.
            reject_or_quarantine_delivery(
                bus,
                guard,
                &stream_id,
                replyable_command_id.as_ref(),
                "command targets a game whose authoritative actor has completed",
            )
            .await?;
            continue;
        };
        match disposition {
            DeliveryDisposition::Incorporated => {
                cursors.insert(game_id, stream_id);
            }
            DeliveryDisposition::Quarantine { reason } => {
                reject_or_quarantine_delivery(
                    bus,
                    guard,
                    &stream_id,
                    replyable_command_id.as_ref(),
                    &reason,
                )
                .await?;
            }
        }
        if created_actor && activate_new_actors {
            let slot = actors.get(&game_id).context("new game actor disappeared")?;
            let (reply, receive) = oneshot::channel();
            slot.sender
                .send(GameActorMessage::Activate { reply })
                .await
                .context("new game actor stopped before activation")?;
            receive
                .await
                .context("new game actor dropped activation reply")??;
        }
    }
    Ok(())
}

async fn activate_all(actors: &HashMap<u32, GameActorSlot>) -> Result<()> {
    let mut replies = Vec::with_capacity(actors.len());
    for slot in actors.values() {
        let (reply, receive) = oneshot::channel();
        slot.sender
            .send(GameActorMessage::Activate { reply })
            .await
            .context("game actor stopped before recovery activation")?;
        replies.push(receive);
    }
    for reply in replies {
        reply
            .await
            .context("game actor dropped activation reply")??;
    }
    Ok(())
}

async fn await_actor_replies(
    replies: Vec<(oneshot::Receiver<Result<()>>, Arc<AtomicBool>)>,
    dropped_context: &'static str,
) -> Result<()> {
    let mut actor_error = None;
    let mut dropped_error = None;
    for (reply, terminally_completed) in replies {
        match reply.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if actor_error.is_none() {
                    actor_error = Some(error);
                }
            }
            Err(_) if terminally_completed.load(Ordering::Acquire) => {}
            Err(error) => {
                if dropped_error.is_none() {
                    dropped_error = Some(anyhow::Error::new(error).context(dropped_context));
                }
            }
        }
    }
    match actor_error.or(dropped_error) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn publish_snapshots(actors: &HashMap<u32, GameActorSlot>) -> Result<()> {
    publish_snapshots_with_timeout(actors, SNAPSHOT_FANOUT_TIMEOUT).await
}

async fn publish_snapshots_with_timeout(
    actors: &HashMap<u32, GameActorSlot>,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, publish_snapshots_unbounded(actors))
        .await
        .context("partition snapshot fan-out exceeded its deadline")?
}

async fn publish_snapshots_unbounded(actors: &HashMap<u32, GameActorSlot>) -> Result<()> {
    let mut replies = Vec::with_capacity(actors.len());
    for slot in actors.values() {
        if slot._task.is_finished() {
            if slot.has_terminal_completion() {
                continue;
            }
            bail!("game actor stopped before snapshot request");
        }
        let (reply, receive) = oneshot::channel();
        if slot
            .sender
            .send(GameActorMessage::Snapshot { reply })
            .await
            .is_err()
        {
            if slot.has_terminal_completion() {
                continue;
            }
            bail!("game actor stopped before snapshot request");
        }
        replies.push((receive, slot.terminally_completed.clone()));
    }
    await_actor_replies(replies, "game actor dropped snapshot reply").await
}

async fn barrier_actors(actors: &HashMap<u32, GameActorSlot>) -> Result<()> {
    let mut replies = Vec::with_capacity(actors.len());
    for slot in actors.values() {
        let (reply, receive) = oneshot::channel();
        if slot
            .sender
            .send(GameActorMessage::Barrier { reply })
            .await
            .is_err()
        {
            if slot.has_terminal_completion() {
                continue;
            }
            bail!("game actor stopped before handoff barrier");
        }
        replies.push((receive, slot.terminally_completed.clone()));
    }
    await_actor_replies(replies, "game actor dropped handoff barrier").await
}

async fn cooperative_handoff(
    actors: &HashMap<u32, GameActorSlot>,
    lease_store: &PartitionLeaseStore,
    guard: &PartitionLeaseGuard,
    fatal: &CancellationToken,
    handoff_cancel: &CancellationToken,
) -> Result<()> {
    // Preempt game-local completion materialization before enqueueing barriers.
    // This leaves the durable checkpoint and PEL entry for the successor
    // instead of letting a slow DynamoDB read hold partition transfer.
    handoff_cancel.cancel();

    let keepalive_stop = CancellationToken::new();
    let keepalive_task_stop = keepalive_stop.clone();
    let keepalive_store = lease_store.clone();
    let keepalive_guard = guard.clone();
    let keepalive = tokio::spawn(async move {
        let mut interval = tokio::time::interval(LEASE_RENEW_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = keepalive_task_stop.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    if !keepalive_store.renew_for_handoff(&keepalive_guard).await? {
                        bail!("partition lease was lost during cooperative barrier");
                    }
                }
            }
        }
    });
    let barrier_result =
        match tokio::time::timeout(HANDOFF_BARRIER_TIMEOUT, barrier_actors(actors)).await {
            Ok(result) => result,
            Err(_) => {
                fatal.cancel();
                Err(anyhow::anyhow!(
                    "partition handoff barrier exceeded its single deadline"
                ))
            }
        };
    keepalive_stop.cancel();
    let keepalive_result = keepalive
        .await
        .context("partition handoff keepalive panicked")?;
    barrier_result?;
    keepalive_result?;
    if !lease_store.release(guard).await? {
        bail!("partition lease was lost before cooperative release");
    }
    Ok(())
}

async fn attempt_all_pending_completions<F, Fut>(
    partition: u32,
    game_ids: Vec<u32>,
    mut attempt: F,
) -> Result<()>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut first_failure = None;
    let mut failure_count = 0usize;
    for game_id in game_ids {
        if let Err(error) = attempt(game_id).await {
            failure_count += 1;
            warn!(
                partition,
                game_id,
                %error,
                "pending completion remains retryable; continuing with later records"
            );
            if first_failure.is_none() {
                first_failure = Some(error);
            }
        }
    }
    match first_failure {
        Some(error) => Err(error.context(format!(
            "{failure_count} pending completion record(s) failed this drain"
        ))),
        None => Ok(()),
    }
}

async fn drain_pending_completions(
    bus: &GameBus,
    guard: &PartitionLeaseGuard,
    db: &dyn Database,
    cleanup_grace: Duration,
) -> Result<()> {
    let game_ids = bus
        .list_pending_completion_ids(guard.namespace(), guard.partition())
        .await?;
    attempt_all_pending_completions(guard.partition(), game_ids, |game_id| async move {
        // The immutable record and pending index were established in the
        // original fenced completion transaction. Recovery snapshots have
        // a shorter TTL and are not an effect-retry dependency.
        let record = bus
            .load_pending_completion(guard.namespace(), guard.partition(), game_id)
            .await?;
        bus.cleanup_matchmaking_for_completion(&record).await?;
        for effect in &record.effects {
            if db.apply_completion_effect(&record, effect).await?
                == EffectApplyResult::AlreadyApplied
            {
                crate::resilience_metrics::record_duplicate_completion_effect_prevented(1);
            }
            bus.mark_completion_effect_done_fenced(guard, &record, effect.id(), cleanup_grace)
                .await?;
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_membership::{BootIdentity, ClusterNamespace};
    use crate::db::ServerRegistration;
    use crate::db::models::{CustomLobby, Game, GamePlayer, HighScoreEntry, RankingEntry, User};
    use crate::redis_keys::RedisKeys;
    use common::{CommandId, Direction, GameCommand, GameState, GameType, Position, QueueMode};
    use redis::AsyncCommands;

    #[test]
    fn lease_watchdog_retries_one_transient_probe_and_keeps_an_expiry_margin() {
        let acquired_at = Instant::now();
        let mut budget = LeaseWatchdogBudget::new(
            acquired_at,
            Duration::from_secs(3),
            Duration::from_millis(750),
        );
        let first_deadline = acquired_at + Duration::from_millis(2_250);
        assert_eq!(budget.fail_closed_at(), first_deadline);
        assert!(budget.can_retry_at(acquired_at + Duration::from_millis(900)));
        assert!(!budget.can_retry_at(first_deadline));

        // A successful retry establishes a fresh full budget; the transient
        // attempt itself must not extend authority.
        let retry_confirmed_at = acquired_at + Duration::from_millis(1_050);
        budget.confirm_at(retry_confirmed_at);
        assert_eq!(
            budget.fail_closed_at(),
            retry_confirmed_at + Duration::from_millis(2_250)
        );
        assert_eq!(
            retry_confirmed_at + Duration::from_secs(3) - budget.fail_closed_at(),
            Duration::from_millis(750),
            "watchdog must stop one bounded Redis operation before possible expiry"
        );
    }

    #[test]
    fn autonomous_actor_redis_failure_is_forwarded_with_its_source_chain() {
        let (failures, mut receiver) = mpsc::unbounded_channel();
        let fatal = CancellationToken::new();
        let redis_error = redis::RedisError::from((
            redis::ErrorKind::IoError,
            "injected autonomous actor Redis failure",
        ));

        report_autonomous_actor_failure(&failures, &fatal, 4, 24, anyhow::Error::new(redis_error));

        let forwarded = receiver
            .try_recv()
            .expect("actor failure should reach the partition executor");
        assert!(fatal.is_cancelled());
        assert!(forwarded.to_string().contains("game 24 actor"));
        assert!(
            forwarded
                .chain()
                .any(|cause| cause.downcast_ref::<redis::RedisError>().is_some()),
            "context must retain the Redis source used by local-restart classification"
        );
    }

    #[tokio::test]
    async fn autonomous_actor_timeout_is_forwarded_with_its_source_chain() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("pending operation should time out");
        let forwarded = autonomous_actor_failure(7, 27, anyhow::Error::new(elapsed));
        assert!(
            forwarded.chain().any(|cause| cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()),
            "context must retain the timeout source used by local-restart classification"
        );
    }

    #[tokio::test]
    async fn aborted_blocked_handoff_does_not_set_a_flag_without_a_request() -> Result<()> {
        let (control, _control_receiver) = mpsc::channel(1);
        let (occupied_reply, _occupied_receive) = oneshot::channel();
        control
            .send(ExecutorControl::Handoff {
                reply: occupied_reply,
            })
            .await?;
        let handoff_cancel = CancellationToken::new();
        let handle = PartitionExecutorV2Handle {
            control,
            handoff_cancel: handoff_cancel.clone(),
        };

        let blocked = tokio::spawn(async move { handle.handoff().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !handoff_cancel.is_cancelled(),
            "cancellation must not become sticky until the typed request is queued"
        );
        blocked.abort();
        assert!(
            blocked
                .await
                .expect_err("blocked handoff should abort")
                .is_cancelled()
        );
        assert!(!handoff_cancel.is_cancelled());
        Ok(())
    }

    #[test]
    fn autonomous_actor_invariant_failure_is_not_disguised_as_coordination_failure() {
        let forwarded = autonomous_actor_failure(
            3,
            23,
            anyhow::anyhow!("invalid game event sequence invariant"),
        );
        assert!(!forwarded.chain().any(|cause| {
            cause.downcast_ref::<redis::RedisError>().is_some()
                || cause
                    .downcast_ref::<tokio::time::error::Elapsed>()
                    .is_some()
        }));
    }

    #[tokio::test]
    async fn actor_panic_is_forwarded_and_cancels_partition() {
        let (failures, mut receiver) = mpsc::unbounded_channel();
        let fatal = CancellationToken::new();

        let completed_normally = supervise_actor_run(
            async {
                panic!("injected game actor panic");
                #[allow(unreachable_code)]
                Result::<()>::Ok(())
            },
            &failures,
            &fatal,
            9,
            29,
        )
        .await;

        assert!(!completed_normally);
        assert!(fatal.is_cancelled());
        let failure = receiver
            .try_recv()
            .expect("actor panic should reach the partition executor");
        assert!(failure.to_string().contains("game 29 actor"));
        assert!(
            failure
                .chain()
                .any(|cause| cause.to_string() == "game actor panicked")
        );
    }

    fn synthetic_delivery(sequence: u64) -> CommandDelivery {
        CommandDelivery {
            stream_id: format!("{sequence}-0"),
            payload: CommandDeliveryPayload::Poison {
                raw: Vec::new(),
                reason: "synthetic actor delivery".to_string(),
            },
            decision: None,
        }
    }

    async fn sender_closed_slot(terminal: bool) -> GameActorSlot {
        let (sender, receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task_terminally_completed = terminally_completed.clone();
        let (ready, closed) = oneshot::channel();
        let task = tokio::spawn(async move {
            if terminal {
                task_terminally_completed.store(true, Ordering::Release);
            }
            drop(receiver);
            let _ = ready.send(());
            std::future::pending::<()>().await;
        });
        closed.await.expect("test receiver should close");
        GameActorSlot {
            sender,
            terminally_completed,
            _task: task,
        }
    }

    fn reply_dropping_slot(terminal: bool) -> GameActorSlot {
        let (sender, mut receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task_terminally_completed = terminally_completed.clone();
        let task = tokio::spawn(async move {
            let Some(GameActorMessage::Delivery { reply, .. }) = receiver.recv().await else {
                panic!("delivery was not sent to test actor");
            };
            if terminal {
                task_terminally_completed.store(true, Ordering::Release);
            }
            drop(reply);
            std::future::pending::<()>().await;
        });
        GameActorSlot {
            sender,
            terminally_completed,
            _task: task,
        }
    }

    async fn abort_test_slot(slot: GameActorSlot) {
        slot._task.abort();
        let _ = slot._task.await;
    }

    #[tokio::test]
    async fn terminal_completion_racing_snapshot_reply_is_benign() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task_terminally_completed = terminally_completed.clone();
        let task = tokio::spawn(async move {
            let Some(GameActorMessage::Snapshot { reply }) = receiver.recv().await else {
                panic!("snapshot request was not delivered");
            };
            // This is the exact actor-exit ordering: publish durable terminal
            // completion first, then drop the receiver and its queued reply.
            task_terminally_completed.store(true, Ordering::Release);
            drop(reply);
        });
        let mut actors = HashMap::from([(
            17,
            GameActorSlot {
                sender,
                terminally_completed,
                _task: task,
            },
        )]);

        publish_snapshots(&actors).await?;

        actors.remove(&17).expect("test actor exists")._task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn abnormal_actor_exit_racing_snapshot_reply_fails_closed() {
        let (sender, mut receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(async move {
            let Some(GameActorMessage::Snapshot { reply }) = receiver.recv().await else {
                panic!("snapshot request was not delivered");
            };
            drop(reply);
        });
        let mut actors = HashMap::from([(
            18,
            GameActorSlot {
                sender,
                terminally_completed,
                _task: task,
            },
        )]);

        let error = publish_snapshots(&actors)
            .await
            .expect_err("an unclassified actor exit must fail the partition");
        assert!(error.to_string().contains("dropped snapshot reply"));
        actors
            .remove(&18)
            .expect("test actor exists")
            ._task
            .await
            .expect("test actor should stop normally");
    }

    #[tokio::test]
    async fn explicit_actor_error_wins_over_secondary_dropped_reply() {
        let (dropped_sender, dropped_reply) = oneshot::channel();
        drop(dropped_sender);
        let (failed_sender, failed_reply) = oneshot::channel();
        failed_sender
            .send(Err(anyhow::anyhow!("primary snapshot failure")))
            .expect("test receiver remains open");
        let replies = vec![
            (dropped_reply, Arc::new(AtomicBool::new(false))),
            (failed_reply, Arc::new(AtomicBool::new(false))),
        ];

        let error = await_actor_replies(replies, "secondary dropped reply")
            .await
            .expect_err("actor failure must fail the operation");

        assert_eq!(error.to_string(), "primary snapshot failure");
    }

    #[test]
    fn queued_actor_failure_wins_over_dropped_delivery_reply() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let source =
            redis::RedisError::from((redis::ErrorKind::IoError, "primary actor Redis failure"));
        sender
            .send(autonomous_actor_failure(4, 14, anyhow::Error::new(source)))
            .expect("test receiver remains open");

        let error = prefer_actor_failure::<()>(
            Err(anyhow::anyhow!("game actor dropped delivery reply")),
            &mut receiver,
        )
        .expect_err("actor failure must fail the dispatch");

        assert!(
            error
                .chain()
                .any(|cause| cause.downcast_ref::<redis::RedisError>().is_some()),
            "the primary typed Redis source must survive the reply-closure race"
        );
    }

    #[tokio::test]
    async fn delivery_send_closure_is_benign_only_after_terminal_commit() -> Result<()> {
        let terminal = sender_closed_slot(true).await;
        assert!(terminal.deliver(synthetic_delivery(1)).await?.is_none());
        abort_test_slot(terminal).await;

        let abnormal = sender_closed_slot(false).await;
        let error = match abnormal.deliver(synthetic_delivery(2)).await {
            Err(error) => error,
            Ok(_) => panic!("abnormal send closure must fail the partition"),
        };
        assert!(
            error
                .to_string()
                .contains("stopped before command delivery")
        );
        abort_test_slot(abnormal).await;
        Ok(())
    }

    #[tokio::test]
    async fn delivery_reply_closure_is_benign_only_after_terminal_commit() -> Result<()> {
        let terminal = reply_dropping_slot(true);
        assert!(terminal.deliver(synthetic_delivery(3)).await?.is_none());
        abort_test_slot(terminal).await;

        let abnormal = reply_dropping_slot(false);
        let error = match abnormal.deliver(synthetic_delivery(4)).await {
            Err(error) => error,
            Ok(_) => panic!("abnormal reply closure must fail the partition"),
        };
        assert!(error.to_string().contains("dropped delivery reply"));
        abort_test_slot(abnormal).await;
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_fanout_has_one_typed_deadline() {
        let (pending_sender, mut pending_receiver) = mpsc::channel(1);
        let pending_task = tokio::spawn(async move {
            let Some(GameActorMessage::Snapshot { reply }) = pending_receiver.recv().await else {
                panic!("snapshot request was not delivered");
            };
            let _reply = reply;
            std::future::pending::<()>().await;
        });
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let mut actors = HashMap::from([(
            21,
            GameActorSlot {
                sender: pending_sender,
                terminally_completed,
                _task: pending_task,
            },
        )]);

        let error = publish_snapshots_with_timeout(&actors, Duration::from_millis(25))
            .await
            .expect_err("one fan-out deadline must bound reply draining");
        assert!(error.chain().any(|cause| {
            cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
        }));

        for (_, slot) in actors.drain() {
            abort_test_slot(slot).await;
        }
    }

    #[tokio::test]
    async fn terminal_completion_racing_handoff_barrier_is_benign() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task_terminally_completed = terminally_completed.clone();
        let task = tokio::spawn(async move {
            let Some(GameActorMessage::Barrier { reply }) = receiver.recv().await else {
                panic!("handoff barrier was not delivered");
            };
            task_terminally_completed.store(true, Ordering::Release);
            drop(reply);
        });
        let mut actors = HashMap::from([(
            19,
            GameActorSlot {
                sender,
                terminally_completed,
                _task: task,
            },
        )]);

        barrier_actors(&actors).await?;

        actors.remove(&19).expect("test actor exists")._task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn abnormal_actor_exit_racing_handoff_barrier_fails_closed() {
        let (sender, mut receiver) = mpsc::channel(1);
        let terminally_completed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(async move {
            let Some(GameActorMessage::Barrier { reply }) = receiver.recv().await else {
                panic!("handoff barrier was not delivered");
            };
            drop(reply);
        });
        let mut actors = HashMap::from([(
            20,
            GameActorSlot {
                sender,
                terminally_completed,
                _task: task,
            },
        )]);

        let error = barrier_actors(&actors)
            .await
            .expect_err("an unclassified actor exit must fail handoff");
        assert!(error.to_string().contains("dropped handoff barrier"));
        actors
            .remove(&20)
            .expect("test actor exists")
            ._task
            .await
            .expect("test actor should stop normally");
    }

    #[derive(Default)]
    struct UnusedDatabase {
        mmr_failures_remaining: Option<std::sync::atomic::AtomicUsize>,
        mmr_read_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    }

    impl UnusedDatabase {
        fn fail_mmr_reads(count: usize) -> Self {
            Self {
                mmr_failures_remaining: Some(std::sync::atomic::AtomicUsize::new(count)),
                mmr_read_gate: None,
            }
        }

        fn block_mmr_reads(
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        ) -> Self {
            Self {
                mmr_failures_remaining: Some(std::sync::atomic::AtomicUsize::new(0)),
                mmr_read_gate: Some((entered, release)),
            }
        }
    }

    macro_rules! unused_database {
        ($(unused_database_method!($name:ident($($argument:ident: $argument_type:ty),* $(,)?) -> $output:ty);)*) => {
            #[async_trait::async_trait]
            impl Database for UnusedDatabase {
                async fn get_user_mmrs(
                    &self,
                    user_ids: &[i32],
                ) -> Result<std::collections::HashMap<i32, (i32, i32)>> {
                    if let Some((entered, release)) = &self.mmr_read_gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    let Some(failures) = &self.mmr_failures_remaining else {
                        return Err(anyhow::anyhow!(
                            "injected persistent MMR read failure for executor recovery test"
                        ));
                    };
                    if failures
                        .fetch_update(
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                            |remaining| remaining.checked_sub(1),
                        )
                        .is_ok()
                    {
                        return Err(anyhow::anyhow!(
                            "injected transient MMR read failure for executor recovery test"
                        ));
                    }
                    Ok(user_ids
                        .iter()
                        .copied()
                        .map(|user_id| (user_id, (1_000, 1_000)))
                        .collect())
                }

                $(
                    async fn $name(&self, $($argument: $argument_type),*) -> Result<$output> {
                        $(let _ = $argument;)*
                        panic!(concat!(stringify!($name), " is not used by executor recovery tests"))
                    }
                )*
            }
        };
    }

    unused_database! {
        unused_database_method!(register_server(
            grpc_address: &str,
            region: &str,
            origin: &str,
            ws_url: &str
        ) -> i32);
        unused_database_method!(update_server_heartbeat(
            server_id: i32,
            registration: &ServerRegistration
        ) -> ());
        unused_database_method!(update_server_status(server_id: i32, status: &str) -> ());
        unused_database_method!(get_server_for_load_balancing(region: &str) -> i32);
        unused_database_method!(get_active_servers(region: &str) -> Vec<(i32, String)>);
        unused_database_method!(get_region_ws_url(region: &str) -> Option<String>);
        unused_database_method!(create_user(username: &str, password_hash: &str, mmr: i32) -> User);
        unused_database_method!(create_guest_user(
            nickname: &str,
            guest_token: &str,
            mmr: i32
        ) -> User);
        unused_database_method!(get_user_by_id(user_id: i32) -> Option<User>);
        unused_database_method!(get_user_by_username(username: &str) -> Option<User>);
        unused_database_method!(update_user_mmr(user_id: i32, mmr: i32) -> ());
        unused_database_method!(update_guest_username(user_id: i32, username: &str) -> ());
        unused_database_method!(add_user_xp(user_id: i32, xp_to_add: i32) -> i32);
        unused_database_method!(update_user_mmr_by_mode(
            user_id: i32,
            mmr_delta: i32,
            queue_mode: &QueueMode
        ) -> i32);
        unused_database_method!(upsert_ranking(
            user_id: i32,
            username: &str,
            mmr: i32,
            queue_mode: &QueueMode,
            game_type: &GameType,
            region: &str,
            season: crate::season::Season,
            won: bool
        ) -> ());
        unused_database_method!(get_leaderboard(
            queue_mode: &QueueMode,
            game_type: Option<&GameType>,
            region: Option<&str>,
            season: crate::season::Season,
            limit: usize
        ) -> Vec<RankingEntry>);
        unused_database_method!(get_user_ranking(
            user_id: i32,
            queue_mode: &QueueMode,
            game_type: &GameType,
            region: &str,
            season: crate::season::Season
        ) -> Option<RankingEntry>);
        unused_database_method!(insert_high_score(
            game_id: &str,
            user_id: i32,
            username: &str,
            score: i32,
            game_type: &GameType,
            region: &str,
            season: crate::season::Season
        ) -> ());
        unused_database_method!(get_high_scores(
            game_type: &GameType,
            region: Option<&str>,
            season: crate::season::Season,
            limit: usize
        ) -> Vec<HighScoreEntry>);
        unused_database_method!(allocate_game_id() -> i32);
        unused_database_method!(create_game(
            server_id: i32,
            game_type: &serde_json::Value,
            game_mode: &str,
            is_private: bool,
            game_code: Option<&str>
        ) -> i32);
        unused_database_method!(get_game_by_id(game_id: i32) -> Option<Game>);
        unused_database_method!(get_game_by_code(game_code: &str) -> Option<Game>);
        unused_database_method!(update_game_status(game_id: i32, status: &str) -> ());
        unused_database_method!(upsert_completed_game(
            game_id: i32,
            server_id: i32,
            game_state: &GameState
        ) -> ());
        unused_database_method!(apply_completion_effect(
            completion: &CompletionRecordV1,
            effect: &crate::completion::CompletionEffect
        ) -> EffectApplyResult);
        unused_database_method!(add_player_to_game(
            game_id: i32,
            user_id: i32,
            team_id: i32
        ) -> ());
        unused_database_method!(get_game_players(game_id: i32) -> Vec<GamePlayer>);
        unused_database_method!(get_player_count(game_id: i32) -> i64);
        unused_database_method!(create_custom_lobby(
            game_code: &str,
            host_user_id: i32,
            settings: &serde_json::Value
        ) -> i32);
        unused_database_method!(update_custom_lobby_game_id(lobby_id: i32, game_id: i32) -> ());
        unused_database_method!(get_custom_lobby_host(game_id: i32) -> Option<i32>);
        unused_database_method!(get_custom_lobby_by_code(game_code: &str) -> Option<CustomLobby>);
        unused_database_method!(add_spectator_to_game(game_id: i32, user_id: i32) -> ());
    }

    // Command/event stream keys are intentionally regional rather than test-
    // namespaced. Serialize these focused Redis tests so one harness cannot
    // delete another harness's partition stream between assertions.
    static CRASH_BOUNDARY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct CrashBoundaryHarness {
        _test_lock: tokio::sync::MutexGuard<'static, ()>,
        token: CancellationToken,
        bus: Arc<GameBus>,
        raw: redis::aio::MultiplexedConnection,
        leases: PartitionLeaseStore,
        namespace: ClusterNamespace,
        owner: BootIdentity,
        guard: PartitionLeaseGuard,
        partition: u32,
        game_id: u32,
        command_id: ClientCommandIdentityV2,
        command: StreamEvent,
    }

    impl CrashBoundaryHarness {
        async fn new(label: &str) -> Result<Self> {
            let test_lock = CRASH_BOUNDARY_TEST_LOCK.lock().await;
            let salt = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let partition = (salt % PARTITION_COUNT as u128) as u32;
            let base_game_id = 100_000_000 + (salt % 50_000_000) as u32;
            let game_id = base_game_id
                + (partition + PARTITION_COUNT - base_game_id % PARTITION_COUNT) % PARTITION_COUNT;
            let namespace = ClusterNamespace::new(format!("boundary-{label}-{salt}"))?;
            let owner = BootIdentity::new();
            let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
            let mut raw = client.get_multiplexed_async_connection().await?;
            let _: () = raw
                .del(&[
                    RedisKeys::stream_commands(partition),
                    RedisKeys::stream_events(partition),
                ])
                .await?;
            let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
            let manager =
                crate::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
            let token = CancellationToken::new();
            let bus = Arc::new(GameBus::new(manager.clone(), client, token.clone()));
            let mut owners = serde_json::Map::new();
            owners.insert(
                partition.to_string(),
                serde_json::Value::String(owner.to_string()),
            );
            let _: () = raw
                .set(
                    namespace.partition_assignment(partition),
                    serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
                )
                .await?;
            let leases = PartitionLeaseStore::new(
                manager,
                namespace.clone(),
                Duration::from_secs(30),
                Duration::from_millis(750),
            )?;
            let guard = leases
                .try_acquire(partition, &owner)
                .await?
                .context("test partition lease was not acquired")?;
            bus.ensure_executor_command_group(&namespace, partition)
                .await?;

            let now = chrono::Utc::now().timestamp_millis();
            let mut initial_state = GameState::new(
                40,
                40,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                Some(7),
                now,
            );
            initial_state.status = GameStatus::Started { server_id: 1 };
            let snake_id = initial_state
                .add_player(77, Some("player-77".into()))?
                .snake_id;
            let baseline = RecoveryEnvelopeV2::new(
                game_id,
                partition,
                initial_state.clone(),
                "0-0".into(),
                ResolvedCommandState::default(),
                0,
                0,
                now,
                guard.encoded_token(),
            );
            bus.checkpoint_and_ack_fenced(&guard, &baseline, &[], Duration::from_secs(60))
                .await?;

            let command_id = ClientCommandIdentityV2 {
                game_id,
                user_id: 77,
                client_game_session_id: format!("session-{salt}"),
                sequence: 1,
            };
            let command = StreamEvent::GameCommandSubmittedV2 {
                game_id,
                user_id: command_id.user_id,
                command_id: command_id.clone(),
                command: GameCommandMessage {
                    command_id_client: CommandId {
                        tick: 1_000,
                        user_id: command_id.user_id,
                        sequence_number: 1,
                    },
                    command_id_server: None,
                    command: GameCommand::Turn {
                        snake_id,
                        direction: Direction::Up,
                    },
                },
            };
            Ok(Self {
                _test_lock: test_lock,
                token,
                bus,
                raw,
                leases,
                namespace,
                owner,
                guard,
                partition,
                game_id,
                command_id,
                command,
            })
        }

        async fn append_command(&mut self) -> Result<String> {
            Ok(self
                .raw
                .xadd(
                    RedisKeys::stream_commands(self.partition),
                    "*",
                    &[("data", serde_json::to_vec(&self.command)?)],
                )
                .await?)
        }

        async fn takeover(&self) -> Result<PartitionLeaseGuard> {
            if !self.leases.release(&self.guard).await? {
                bail!("test owner could not release its original token");
            }
            self.leases
                .try_acquire(self.partition, &self.owner)
                .await?
                .context("successor did not acquire a fresh token")
        }

        async fn recovery(&self) -> Result<RecoveryEnvelopeV2> {
            self.bus
                .get_recovery(&self.namespace, self.game_id)
                .await?
                .context("test game has no recovery checkpoint")
        }

        fn actor(&self, envelope: RecoveryEnvelopeV2, guard: PartitionLeaseGuard) -> GameActor {
            self.actor_with_database(envelope, guard, Arc::new(UnusedDatabase::default()))
        }

        fn actor_with_database(
            &self,
            envelope: RecoveryEnvelopeV2,
            guard: PartitionLeaseGuard,
            db: Arc<dyn Database>,
        ) -> GameActor {
            let (_sender, receiver) = mpsc::channel(1);
            let fatal = self.token.child_token();
            GameActor::from_envelope(
                2,
                envelope,
                self.bus.clone(),
                guard,
                db,
                RecoveryConfig::default(),
                receiver,
                fatal.clone(),
                fatal.child_token(),
            )
        }

        async fn cleanup(mut self, live_guard: &PartitionLeaseGuard) -> Result<()> {
            self.token.cancel();
            let _: () = self
                .raw
                .del(&[
                    self.namespace.partition_assignment(self.partition),
                    live_guard.lease_key(),
                    self.namespace.recovery(self.game_id),
                    self.namespace.recovery_failure(self.game_id),
                    self.namespace.active_games(self.partition),
                    self.namespace.command_decisions(self.partition),
                    self.namespace.completion(self.game_id),
                    self.namespace.pending_completions(self.partition),
                    self.namespace.completion_effects_done(self.game_id),
                    self.namespace.completion_terminal_notified(self.game_id),
                    RedisKeys::game_snapshot(self.game_id),
                    RedisKeys::stream_commands(self.partition),
                    RedisKeys::stream_events(self.partition),
                ])
                .await?;
            Ok(())
        }
    }

    fn assert_one_scheduled_result(actor: &GameActor, command_id: &ClientCommandIdentityV2) {
        assert_eq!(actor.engine.next_server_command_sequence(), 1);
        assert!(matches!(
            actor.resolved.get(command_id),
            Some(CommandOutcome::Scheduled { .. })
        ));
    }

    async fn read_game_events(
        redis: &mut redis::aio::MultiplexedConnection,
        partition: u32,
    ) -> Result<Vec<GameEventMessage>> {
        let entries: redis::streams::StreamRangeReply = redis
            .xrange_all(RedisKeys::stream_events(partition))
            .await?;
        entries
            .ids
            .into_iter()
            .map(|entry| {
                let payload: Vec<u8> = entry
                    .get("data")
                    .with_context(|| format!("event stream entry {} has no payload", entry.id))?;
                Ok(serde_json::from_slice(&payload)?)
            })
            .collect()
    }

    async fn read_executor_commands(
        redis: &mut redis::aio::MultiplexedConnection,
        partition: u32,
    ) -> Result<Vec<StreamEvent>> {
        let entries: redis::streams::StreamRangeReply = redis
            .xrange_all(RedisKeys::stream_commands(partition))
            .await?;
        entries
            .ids
            .into_iter()
            .map(|entry| {
                let payload: Vec<u8> = entry
                    .get("data")
                    .with_context(|| format!("command stream entry {} has no payload", entry.id))?;
                Ok(serde_json::from_slice(&payload)?)
            })
            .collect()
    }

    async fn wait_for_game_snapshot(
        subscription: &mut crate::game_bus::PartitionSubscription,
        game_id: u32,
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let event = subscription
                    .recv_event()
                    .await
                    .context("partition event reader stopped before snapshot")?;
                if event.game_id == game_id && matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Result::<()>::Ok(());
                }
            }
        })
        .await
        .context("timed out waiting for game snapshot")??;
        Ok(())
    }

    #[tokio::test]
    async fn command_crash_boundaries_recover_one_logical_result() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            // Kill after XADD but before group delivery: the successor reads
            // the still-new entry, schedules it once, and checkpoints its ID.
            let mut after_xadd = CrashBoundaryHarness::new("after-xadd").await?;
            let xadd_id = after_xadd.append_command().await?;
            let successor_guard = after_xadd.takeover().await?;
            let mut consumer = after_xadd
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].stream_id, xadd_id);
            let mut successor =
                after_xadd.actor(after_xadd.recovery().await?, successor_guard.clone());
            assert!(matches!(
                successor.incorporate(deliveries.remove(0)).await?,
                DeliveryDisposition::Incorporated
            ));
            successor.checkpoint().await?;
            assert_one_scheduled_result(&successor, &after_xadd.command_id);
            let recovered = after_xadd.recovery().await?;
            assert_eq!(recovered.command_cursor, xadd_id);
            assert_eq!(recovered.next_server_command_sequence, 1);
            assert!(matches!(
                recovered
                    .resolved_client_commands
                    .get(&after_xadd.command_id),
                Some(CommandOutcome::Scheduled { .. })
            ));
            assert_eq!(
                after_xadd
                    .bus
                    .xack_fenced(&successor_guard, std::slice::from_ref(&xadd_id))
                    .await?,
                0
            );
            after_xadd.cleanup(&successor_guard).await?;

            // Kill after delivery into the PEL but before schedule: takeover
            // reclaims the exact stream ID rather than inventing new work.
            let mut before_schedule = CrashBoundaryHarness::new("before-schedule").await?;
            let pending_id = before_schedule.append_command().await?;
            let mut original_consumer = before_schedule
                .bus
                .subscribe_executor_commands(before_schedule.guard.clone())
                .await?;
            let original_delivery = original_consumer.read_new_now().await?;
            assert_eq!(original_delivery.len(), 1);
            assert_eq!(original_delivery[0].stream_id, pending_id);
            drop(original_consumer);
            let successor_guard = before_schedule.takeover().await?;
            let mut consumer = before_schedule
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut successor =
                before_schedule.actor(before_schedule.recovery().await?, successor_guard.clone());
            assert!(matches!(
                successor
                    .incorporate(reclaimed.deliveries.remove(0))
                    .await?,
                DeliveryDisposition::Incorporated
            ));
            successor.checkpoint().await?;
            assert_one_scheduled_result(&successor, &before_schedule.command_id);
            before_schedule.cleanup(&successor_guard).await?;

            // Kill after scheduling but before checkpoint: only volatile state
            // is lost. Replaying from the prior envelope produces exactly the
            // same command queue, result, and server sequence.
            let mut before_checkpoint = CrashBoundaryHarness::new("before-checkpoint").await?;
            let pending_id = before_checkpoint.append_command().await?;
            let mut original_consumer = before_checkpoint
                .bus
                .subscribe_executor_commands(before_checkpoint.guard.clone())
                .await?;
            let mut deliveries = original_consumer.read_new_now().await?;
            let mut original = before_checkpoint.actor(
                before_checkpoint.recovery().await?,
                before_checkpoint.guard.clone(),
            );
            assert!(matches!(
                original.incorporate(deliveries.remove(0)).await?,
                DeliveryDisposition::Incorporated
            ));
            let volatile_state = serde_json::to_value(original.engine.get_committed_state())?;
            assert_one_scheduled_result(&original, &before_checkpoint.command_id);
            drop(original);
            drop(original_consumer);
            let successor_guard = before_checkpoint.takeover().await?;
            let mut successor_consumer = before_checkpoint
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut successor = before_checkpoint
                .actor(before_checkpoint.recovery().await?, successor_guard.clone());
            successor
                .incorporate(reclaimed.deliveries.remove(0))
                .await?;
            assert_eq!(
                serde_json::to_value(successor.engine.get_committed_state())?,
                volatile_state
            );
            assert_one_scheduled_result(&successor, &before_checkpoint.command_id);
            successor.checkpoint().await?;
            before_checkpoint.cleanup(&successor_guard).await?;

            // Kill after checkpoint but before CommandScheduledV2: the ACKed
            // entry is not replayed, while the exact outcome remains durable
            // for the reconnect recovery bridge.
            let mut before_publication = CrashBoundaryHarness::new("before-publication").await?;
            let checkpointed_id = before_publication.append_command().await?;
            let mut original_consumer = before_publication
                .bus
                .subscribe_executor_commands(before_publication.guard.clone())
                .await?;
            let mut deliveries = original_consumer.read_new_now().await?;
            let mut original = before_publication.actor(
                before_publication.recovery().await?,
                before_publication.guard.clone(),
            );
            original.incorporate(deliveries.remove(0)).await?;
            original.checkpoint().await?;
            let checkpointed_state = serde_json::to_value(original.engine.get_committed_state())?;
            drop(original);
            drop(original_consumer);
            let successor_guard = before_publication.takeover().await?;
            let checkpoint = before_publication.recovery().await?;
            assert_eq!(checkpoint.command_cursor, checkpointed_id);
            assert!(matches!(
                checkpoint
                    .resolved_client_commands
                    .get(&before_publication.command_id),
                Some(CommandOutcome::Scheduled { .. })
            ));
            let mut successor_consumer = before_publication
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            assert!(
                successor_consumer
                    .reclaim_next()
                    .await?
                    .deliveries
                    .is_empty()
            );
            assert!(successor_consumer.read_new_now().await?.is_empty());
            let successor = before_publication.actor(checkpoint, successor_guard.clone());
            assert_eq!(
                serde_json::to_value(successor.engine.get_committed_state())?,
                checkpointed_state
            );
            assert_one_scheduled_result(&successor, &before_publication.command_id);
            before_publication.cleanup(&successor_guard).await?;

            // Kill after visible CommandScheduledV2 but before XACK: the
            // successor replays silently, publishes a snapshot reanchor, and
            // does not emit a second incremental schedule.
            let mut before_xack = CrashBoundaryHarness::new("before-xack").await?;
            let mut events = before_xack
                .bus
                .subscribe_to_partition(before_xack.partition)
                .await?;
            let pending_id = before_xack.append_command().await?;
            let mut original_consumer = before_xack
                .bus
                .subscribe_executor_commands(before_xack.guard.clone())
                .await?;
            let mut deliveries = original_consumer.read_new_now().await?;
            let mut original =
                before_xack.actor(before_xack.recovery().await?, before_xack.guard.clone());
            original.live = true;
            original.incorporate(deliveries.remove(0)).await?;
            let visible = tokio::time::timeout(Duration::from_secs(2), events.recv_event())
                .await?
                .context("scheduled outcome was not published")?;
            assert!(matches!(
                visible.event,
                GameEvent::CommandScheduledV2 { command_id, .. }
                    if command_id == before_xack.command_id
            ));
            let volatile_state = serde_json::to_value(original.engine.get_committed_state())?;
            drop(original);
            drop(original_consumer);
            let successor_guard = before_xack.takeover().await?;
            let mut successor_consumer = before_xack
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut successor =
                before_xack.actor(before_xack.recovery().await?, successor_guard.clone());
            successor
                .incorporate(reclaimed.deliveries.remove(0))
                .await?;
            assert_eq!(
                serde_json::to_value(successor.engine.get_committed_state())?,
                volatile_state
            );
            successor.publish_fresh_snapshot().await?;
            let reanchor = tokio::time::timeout(Duration::from_secs(2), events.recv_event())
                .await?
                .context("recovery snapshot was not published")?;
            assert!(matches!(reanchor.event, GameEvent::Snapshot { .. }));
            assert!(
                tokio::time::timeout(Duration::from_millis(150), events.recv_event())
                    .await
                    .is_err(),
                "replay emitted a duplicate incremental command outcome"
            );
            assert_one_scheduled_result(&successor, &before_xack.command_id);
            before_xack.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn crash_replay_reuses_the_exact_write_ahead_server_schedule() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut harness = CrashBoundaryHarness::new("write-ahead-schedule").await?;
            let checkpoint = harness.recovery().await?;
            let checkpoint_tick = checkpoint.game_state.tick;
            if let StreamEvent::GameCommandSubmittedV2 { command, .. } = &mut harness.command {
                command.command_id_client.tick = 1;
            }

            let first_target_ms = checkpoint.game_state.start_ms + 2_000;
            let final_target_ms = checkpoint.game_state.start_ms + 2_500;
            let mut original = harness.actor(checkpoint.clone(), harness.guard.clone());
            original.live = true;
            original.engine.run_until(first_target_ms)?;
            let original_decision_tick = original.engine.get_committed_state().tick;
            assert!(original_decision_tick > checkpoint_tick);

            let mut events = harness
                .bus
                .subscribe_to_partition(harness.partition)
                .await?;
            let pending_id = harness.append_command().await?;
            let mut original_consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let mut deliveries = original_consumer.read_new_now().await?;
            original.incorporate(deliveries.remove(0)).await?;
            let visible = tokio::time::timeout(Duration::from_secs(2), events.recv_event())
                .await?
                .context("write-ahead scheduled outcome was not published")?;
            let exact_schedule = match &visible.event {
                GameEvent::CommandScheduledV2 {
                    command_id,
                    command_message,
                    deduplicated_replay: false,
                } if command_id == &harness.command_id => command_message.clone(),
                event => bail!("unexpected write-ahead outcome event: {event:?}"),
            };
            let server_id = exact_schedule
                .command_id_server
                .as_ref()
                .context("scheduled outcome has no server command ID")?;
            assert_eq!(server_id.tick, original_decision_tick);
            assert_eq!(original.engine.next_server_command_sequence(), 1);
            assert_eq!(
                harness
                    .raw
                    .hlen::<_, usize>(harness.namespace.command_decisions(harness.partition))
                    .await?,
                1
            );

            original.engine.run_until(final_target_ms)?;
            let expected_final_state = serde_json::to_value(original.engine.get_committed_state())?;
            drop(original);
            drop(original_consumer);

            let successor_guard = harness.takeover().await?;
            let mut successor_consumer = harness
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut decisions = harness
                .bus
                .load_command_decisions_fenced(&successor_guard)
                .await?;
            attach_command_decisions(&mut reclaimed.deliveries, &mut decisions);
            assert!(decisions.is_empty());
            assert!(reclaimed.deliveries[0].decision.is_some());

            let mut successor = harness.actor(checkpoint, successor_guard.clone());
            successor
                .incorporate(reclaimed.deliveries.remove(0))
                .await?;
            assert_eq!(successor.engine.next_server_command_sequence(), 1);
            assert_eq!(successor.next_event_stream_sequence, visible.stream_seq);
            assert!(matches!(
                successor.resolved.get(&harness.command_id),
                Some(CommandOutcome::Scheduled { command }) if command == &exact_schedule
            ));
            successor.engine.run_until(final_target_ms)?;
            assert_eq!(
                serde_json::to_value(successor.engine.get_committed_state())?,
                expected_final_state,
                "successor diverged after replaying the exact server schedule"
            );

            successor.publish_fresh_snapshot().await?;
            let reanchor = tokio::time::timeout(Duration::from_secs(2), events.recv_event())
                .await?
                .context("successor recovery snapshot was not published")?;
            assert!(matches!(reanchor.event, GameEvent::Snapshot { .. }));
            assert!(reanchor.stream_seq > visible.stream_seq);
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());
            assert_eq!(
                harness
                    .raw
                    .hlen::<_, usize>(harness.namespace.command_decisions(harness.partition))
                    .await?,
                0
            );
            let scheduled_count = read_game_events(&mut harness.raw, harness.partition)
                .await?
                .into_iter()
                .filter(|message| {
                    matches!(
                        &message.event,
                        GameEvent::CommandScheduledV2 { command_id, .. }
                            if command_id == &harness.command_id
                    )
                })
                .count();
            assert_eq!(scheduled_count, 1);

            harness.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn recorded_decision_must_strictly_advance_recovered_event_watermark() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let harness = CrashBoundaryHarness::new("journal-event-watermark").await?;
            let mut envelope = harness.recovery().await?;
            envelope.next_event_stream_sequence = 7;
            let state_tick = envelope.game_state.tick;
            let state_sequence = envelope.game_state.event_sequence;
            let mut actor = harness.actor(envelope, harness.guard.clone());
            let decision = CommandDecisionV1::new(
                "1-0".into(),
                0,
                GameEventMessage {
                    game_id: harness.game_id,
                    tick: state_tick,
                    sequence: state_sequence,
                    stream_seq: 7,
                    user_id: None,
                    event: GameEvent::CommandRejected {
                        command_id: harness.command_id.clone(),
                        reason: "recorded rejection".into(),
                    },
                },
            );

            let error = actor
                .incorporate_recorded_decision(&harness.command_id, &decision)
                .err()
                .context("equal journal watermark must fail closed")?;
            assert!(error.to_string().contains("does not strictly advance"));
            assert!(actor.resolved.get(&harness.command_id).is_none());
            assert_eq!(actor.next_event_stream_sequence, 7);

            let guard = harness.guard.clone();
            harness.cleanup(&guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_completion_is_visible_only_after_its_atomic_commit() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("terminal-publication").await?;
            let now = chrono::Utc::now().timestamp_millis();
            let mut state = GameState::new(
                10,
                10,
                GameType::Solo,
                QueueMode::Quickmatch,
                Some(11),
                now - 2_000,
            );
            state.status = GameStatus::Started { server_id: 1 };
            let snake_id = state.add_player(77, Some("player-77".into()))?.snake_id;
            state.arena.snakes[snake_id as usize].body =
                vec![Position { x: 0, y: 2 }, Position { x: 1, y: 2 }];
            state.arena.snakes[snake_id as usize].direction = Direction::Left;

            let envelope = RecoveryEnvelopeV2::new(
                harness.game_id,
                harness.partition,
                state,
                "0-0".into(),
                ResolvedCommandState::default(),
                0,
                0,
                now,
                harness.guard.encoded_token(),
            );
            let mut actor = harness.actor(envelope, harness.guard.clone());

            // A wrong-type pending index deterministically rejects the fenced
            // completion/checkpoint transaction before it writes anything.
            let _: () = harness
                .raw
                .set(
                    harness.namespace.pending_completions(harness.partition),
                    "injected-wrong-type",
                )
                .await?;
            let error = actor
                .advance_live()
                .await
                .expect_err("completion commit should reject the wrong-type index");
            assert!(error.to_string().contains("wrong type"));
            assert!(actor.engine.get_committed_state().is_complete());
            assert!(!actor.completion_committed);

            let before_commit = read_game_events(&mut harness.raw, harness.partition).await?;
            assert!(
                before_commit.is_empty(),
                "a failed completion commit exposed its terminal-transition event batch"
            );
            assert!(
                read_executor_commands(&mut harness.raw, harness.partition)
                    .await?
                    .is_empty(),
                "a failed completion commit exposed its terminal command marker"
            );

            let removed: usize = harness
                .raw
                .del(harness.namespace.pending_completions(harness.partition))
                .await?;
            assert_eq!(removed, 1);
            actor.advance_live().await?;
            assert!(actor.completion_committed);

            let after_commit = read_game_events(&mut harness.raw, harness.partition).await?;
            let terminal_events: Vec<_> = after_commit
                .iter()
                .filter(|message| match &message.event {
                    GameEvent::StatusUpdated {
                        status: GameStatus::Complete { .. },
                    } => true,
                    GameEvent::Snapshot { game_state } => game_state.is_complete(),
                    _ => false,
                })
                .collect();
            assert_eq!(
                terminal_events.len(),
                1,
                "successful completion must publish one durable terminal outcome"
            );
            assert!(matches!(
                &terminal_events[0].event,
                GameEvent::Snapshot { game_state } if game_state.is_complete()
            ));
            assert_eq!(
                terminal_events[0].stream_seq,
                actor.next_event_stream_sequence + 1
            );

            let terminal_commands =
                read_executor_commands(&mut harness.raw, harness.partition).await?;
            assert_eq!(terminal_commands.len(), 1);
            assert!(matches!(
                &terminal_commands[0],
                StreamEvent::StatusUpdated {
                    game_id,
                    status: GameStatus::Complete { .. }
                } if *game_id == harness.game_id
            ));

            harness.cleanup(&actor.guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn recovery_activation_does_not_publish_or_checkpoint_terminal_state_before_commit()
    -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("terminal-recovery-activation").await?;
            let now = chrono::Utc::now().timestamp_millis();
            let mut state = GameState::new(
                10,
                10,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                Some(11),
                now - 2_000,
            );
            state.status = GameStatus::Started { server_id: 1 };
            let snake_id = state.add_player(77, Some("player-77".into()))?.snake_id;
            state.arena.snakes[snake_id as usize].body =
                vec![Position { x: 0, y: 2 }, Position { x: 1, y: 2 }];
            state.arena.snakes[snake_id as usize].direction = Direction::Left;

            let envelope = RecoveryEnvelopeV2::new(
                harness.game_id,
                harness.partition,
                state,
                "0-0".into(),
                ResolvedCommandState::default(),
                0,
                0,
                now,
                harness.guard.encoded_token(),
            );
            harness
                .bus
                .checkpoint_and_ack_fenced(&harness.guard, &envelope, &[], Duration::from_secs(60))
                .await?;
            let mut actor = harness.actor_with_database(
                envelope,
                harness.guard.clone(),
                Arc::new(UnusedDatabase::fail_mmr_reads(1)),
            );
            let pending_id = harness.append_command().await?;
            let mut consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let mut deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 1);
            actor.incorporate(deliveries.remove(0)).await?;

            // Catch-up crosses into Complete, then the first required MMR read
            // fails. Activation remains healthy and live so only this game is
            // retried; it must expose no terminal state before that succeeds.
            actor.activate().await?;
            assert!(actor.engine.get_committed_state().is_complete());
            assert!(actor.terminal_pending());
            assert!(actor.live);
            assert!(actor.completion_materialization_retry_at.is_some());
            actor.checkpoint().await?;
            actor.publish_fresh_snapshot().await?;
            assert_eq!(actor.pending_stream_ids, vec![pending_id.clone()]);
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert_eq!(pending.ids.len(), 1);
            assert_eq!(pending.ids[0].id, pending_id);

            let persisted = harness.recovery().await?;
            assert!(
                !persisted.game_state.is_complete(),
                "ordinary activation checkpoint exposed Complete before the atomic commit"
            );
            assert!(
                !harness
                    .bus
                    .get_stored_snapshot(harness.game_id)
                    .await?
                    .context("activation baseline snapshot disappeared")?
                    .is_complete(),
                "stored snapshot exposed Complete before the atomic commit"
            );
            assert!(
                read_game_events(&mut harness.raw, harness.partition)
                    .await?
                    .iter()
                    .all(|message| match &message.event {
                        GameEvent::StatusUpdated {
                            status: GameStatus::Complete { .. },
                        } => false,
                        GameEvent::Snapshot { game_state } => !game_state.is_complete(),
                        _ => true,
                    }),
                "failed recovery activation published a terminal event"
            );

            actor.completion_materialization_retry_at = Some(
                Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .context("one millisecond is representable")?,
            );
            actor.advance_live().await?;
            assert!(actor.completion_committed);
            assert!(actor.pending_stream_ids.is_empty());
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());
            assert!(harness.recovery().await?.game_state.is_complete());
            let completion: Option<Vec<u8>> = harness
                .raw
                .get(harness.namespace.completion(harness.game_id))
                .await?;
            assert!(completion.is_some());
            assert!(
                !harness
                    .raw
                    .sismember::<_, _, bool>(
                        harness.namespace.active_games(harness.partition),
                        harness.game_id,
                    )
                    .await?
            );
            let terminal_events: Vec<_> = read_game_events(&mut harness.raw, harness.partition)
                .await?
                .into_iter()
                .filter(|message| {
                    matches!(
                        &message.event,
                        GameEvent::Snapshot { game_state } if game_state.is_complete()
                    )
                })
                .collect();
            assert_eq!(terminal_events.len(), 1);

            harness.cleanup(&actor.guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn handoff_barrier_leaves_live_work_for_exact_successor_replay() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("live-handoff-replay").await?;
            let baseline = harness.recovery().await?;
            let (sender, receiver) = mpsc::channel(8);
            let fatal = harness.token.child_token();
            let recovery_config = RecoveryConfig {
                // Keep the test focused on the handoff barrier. A slow CI
                // runner must not let the ordinary periodic checkpoint race
                // the assertion that Barrier itself performs no checkpoint.
                checkpoint_interval: Duration::from_secs(60),
                ..RecoveryConfig::default()
            };
            let mut original = GameActor::from_envelope(
                2,
                baseline,
                harness.bus.clone(),
                harness.guard.clone(),
                Arc::new(UnusedDatabase::default()),
                recovery_config,
                receiver,
                fatal.clone(),
                fatal.child_token(),
            );
            let original_task = tokio::spawn(async move { original.run().await });

            let (activate_reply, activate_result) = oneshot::channel();
            sender
                .send(GameActorMessage::Activate {
                    reply: activate_reply,
                })
                .await?;
            activate_result.await??;

            let pending_id = harness.append_command().await?;
            let mut consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let mut deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].stream_id, pending_id);
            let (delivery_reply, delivery_result) = oneshot::channel();
            sender
                .send(GameActorMessage::Delivery {
                    delivery: deliveries.remove(0),
                    reply: delivery_reply,
                })
                .await?;
            assert!(matches!(
                delivery_result.await??,
                DeliveryDisposition::Incorporated
            ));

            let (barrier_reply, barrier_result) = oneshot::channel();
            sender
                .send(GameActorMessage::Barrier {
                    reply: barrier_reply,
                })
                .await?;
            barrier_result.await??;
            original_task.await??;

            assert_eq!(
                harness.recovery().await?.command_cursor,
                "0-0",
                "handoff must leave post-checkpoint work pending for authoritative recovery"
            );
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert_eq!(pending.ids.len(), 1);
            assert_eq!(pending.ids[0].id, pending_id);
            assert_eq!(
                harness
                    .raw
                    .hlen::<_, usize>(harness.namespace.command_decisions(harness.partition))
                    .await?,
                1
            );

            let successor_guard = harness.takeover().await?;
            let mut recovered = harness
                .bus
                .load_partition_recovery_fenced(&successor_guard, Duration::from_secs(60))
                .await?;
            assert_eq!(recovered.len(), 1);
            let mut successor_consumer = harness
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut decisions = harness
                .bus
                .load_command_decisions_fenced(&successor_guard)
                .await?;
            attach_command_decisions(&mut reclaimed.deliveries, &mut decisions);
            assert!(decisions.is_empty());

            let mut successor = harness.actor(recovered.remove(0), successor_guard.clone());
            assert!(matches!(
                successor
                    .incorporate(reclaimed.deliveries.remove(0))
                    .await?,
                DeliveryDisposition::Incorporated
            ));
            assert_one_scheduled_result(&successor, &harness.command_id);
            successor.activate().await?;

            assert_eq!(harness.recovery().await?.command_cursor, pending_id);
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());
            assert_eq!(
                harness
                    .raw
                    .hlen::<_, usize>(harness.namespace.command_decisions(harness.partition))
                    .await?,
                0
            );
            let scheduled = read_game_events(&mut harness.raw, harness.partition)
                .await?
                .into_iter()
                .filter(|message| {
                    matches!(
                        &message.event,
                        GameEvent::CommandScheduledV2 { command_id, .. }
                            if command_id == &harness.command_id
                    )
                })
                .count();
            assert_eq!(
                scheduled, 1,
                "successor replay duplicated a visible outcome"
            );

            harness.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn assignment_move_preempts_blocked_completion_and_successor_finishes() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut harness = CrashBoundaryHarness::new("blocked-completion-handoff").await?;
            let now = chrono::Utc::now().timestamp_millis();
            let mut state = GameState::new(
                10,
                10,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                Some(11),
                now - 2_000,
            );
            state.status = GameStatus::Started { server_id: 1 };
            let snake_id = state.add_player(77, Some("player-77".into()))?.snake_id;
            state.arena.snakes[snake_id as usize].body =
                vec![Position { x: 0, y: 2 }, Position { x: 1, y: 2 }];
            state.arena.snakes[snake_id as usize].direction = Direction::Left;

            let envelope = RecoveryEnvelopeV2::new(
                harness.game_id,
                harness.partition,
                state,
                "0-0".into(),
                ResolvedCommandState::default(),
                0,
                0,
                now,
                harness.guard.encoded_token(),
            );
            harness
                .bus
                .checkpoint_and_ack_fenced(&harness.guard, &envelope, &[], Duration::from_secs(60))
                .await?;

            let pending_id = harness.append_command().await?;
            let mut consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].stream_id, pending_id);
            drop(deliveries);
            drop(consumer);

            let mmr_read_entered = Arc::new(tokio::sync::Notify::new());
            let never_release_mmr_read = Arc::new(tokio::sync::Notify::new());
            let partition_cancellation = CancellationToken::new();
            let (_executor, executor_task) = spawn_game_executor_v2(
                2,
                harness.guard.clone(),
                harness.leases.clone(),
                harness.bus.clone(),
                Arc::new(UnusedDatabase::block_mmr_reads(
                    mmr_read_entered.clone(),
                    never_release_mmr_read,
                )),
                RecoveryConfig::default(),
                partition_cancellation,
            );
            tokio::time::timeout(Duration::from_secs(1), mmr_read_entered.notified())
                .await
                .context("completion never entered the blocked MMR read")?;

            let successor_owner = BootIdentity::new();
            let mut owners = serde_json::Map::new();
            owners.insert(
                harness.partition.to_string(),
                serde_json::Value::String(successor_owner.to_string()),
            );
            let _: () = harness
                .raw
                .set(
                    harness.namespace.partition_assignment(harness.partition),
                    serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
                )
                .await?;

            tokio::time::timeout(Duration::from_secs(3), executor_task)
                .await
                .context("assignment-driven handoff did not preempt blocked completion")???;

            let persisted = harness.recovery().await?;
            assert!(
                !persisted.game_state.is_complete(),
                "handoff exposed the in-memory terminal state"
            );
            assert!(
                !harness
                    .raw
                    .exists::<_, bool>(harness.namespace.completion(harness.game_id))
                    .await?,
                "canceled materialization created a completion record"
            );
            assert!(
                read_game_events(&mut harness.raw, harness.partition)
                    .await?
                    .iter()
                    .all(|message| match &message.event {
                        GameEvent::StatusUpdated {
                            status: GameStatus::Complete { .. },
                        } => false,
                        GameEvent::Snapshot { game_state } => !game_state.is_complete(),
                        _ => true,
                    }),
                "canceled materialization published a terminal event"
            );
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert_eq!(pending.ids.len(), 1);
            assert_eq!(pending.ids[0].id, pending_id);

            let successor_guard = harness
                .leases
                .try_acquire(harness.partition, &successor_owner)
                .await?
                .context("successor did not acquire the released lease")?;
            let mut recovered = harness
                .bus
                .load_partition_recovery_fenced(&successor_guard, Duration::from_secs(60))
                .await?;
            assert_eq!(recovered.len(), 1);
            assert!(!recovered[0].game_state.is_complete());

            let mut successor_consumer = harness
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let mut reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);
            let mut decisions = harness
                .bus
                .load_command_decisions_fenced(&successor_guard)
                .await?;
            attach_command_decisions(&mut reclaimed.deliveries, &mut decisions);

            let mut successor = harness.actor_with_database(
                recovered.remove(0),
                successor_guard.clone(),
                Arc::new(UnusedDatabase::fail_mmr_reads(0)),
            );
            assert!(matches!(
                successor
                    .incorporate(reclaimed.deliveries.remove(0))
                    .await?,
                DeliveryDisposition::Incorporated
            ));
            successor.activate().await?;
            assert!(successor.completion_committed);
            assert!(harness.recovery().await?.game_state.is_complete());
            assert!(
                !harness
                    .raw
                    .sismember::<_, _, bool>(
                        harness.namespace.active_games(harness.partition),
                        harness.game_id,
                    )
                    .await?
            );
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());

            let completion: Vec<u8> = harness
                .raw
                .get(harness.namespace.completion(harness.game_id))
                .await?;
            let completion: CompletionRecordV1 = serde_json::from_slice(&completion)?;
            completion.validate()?;
            let terminal_event_count = read_game_events(&mut harness.raw, harness.partition)
                .await?
                .into_iter()
                .filter(|message| match &message.event {
                    GameEvent::StatusUpdated {
                        status: GameStatus::Complete { .. },
                    } => true,
                    GameEvent::Snapshot { game_state } => game_state.is_complete(),
                    _ => false,
                })
                .count();
            assert_eq!(terminal_event_count, 1);

            harness.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn assignment_move_during_blocked_bootstrap_releases_for_successor() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut harness = CrashBoundaryHarness::new("bootstrap-assignment-move").await?;
            let (read_assigned, _never_release_read_reply) =
                harness.bus.gate_next_nonempty_executor_read_reply();
            let pending_id = harness.append_command().await?;
            let executor_cancel = CancellationToken::new();
            let (_executor, executor_task) = spawn_game_executor_v2(
                2,
                harness.guard.clone(),
                harness.leases.clone(),
                harness.bus.clone(),
                Arc::new(UnusedDatabase::default()),
                RecoveryConfig::default(),
                executor_cancel,
            );
            tokio::time::timeout(Duration::from_secs(2), read_assigned.notified())
                .await
                .context("executor did not block inside bootstrap command recovery")?;

            let successor_owner = BootIdentity::new();
            let mut owners = serde_json::Map::new();
            owners.insert(
                harness.partition.to_string(),
                serde_json::Value::String(successor_owner.to_string()),
            );
            let _: () = harness
                .raw
                .set(
                    harness.namespace.partition_assignment(harness.partition),
                    serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
                )
                .await?;

            // The harness lease lasts 30 seconds. Completion here proves that
            // assignment movement compare-deletes the exact bootstrap token
            // instead of waiting for expiry.
            let executor_result = tokio::time::timeout(Duration::from_secs(3), executor_task)
                .await
                .context("assignment move waited for the bootstrap lease TTL")?
                .context("bootstrap executor task panicked")?;
            executor_result?;
            let successor_guard = harness
                .leases
                .try_acquire(harness.partition, &successor_owner)
                .await?
                .context("successor could not acquire the released bootstrap lease")?;

            let mut successor_consumer = harness
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);

            harness.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn direct_handoff_during_blocked_bootstrap_releases_for_successor() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut harness = CrashBoundaryHarness::new("bootstrap-direct-handoff").await?;
            let (read_assigned, _never_release_read_reply) =
                harness.bus.gate_next_nonempty_executor_read_reply();
            let pending_id = harness.append_command().await?;
            let executor_cancel = CancellationToken::new();
            let (executor, executor_task) = spawn_game_executor_v2(
                2,
                harness.guard.clone(),
                harness.leases.clone(),
                harness.bus.clone(),
                Arc::new(UnusedDatabase::default()),
                RecoveryConfig::default(),
                executor_cancel,
            );
            tokio::time::timeout(Duration::from_secs(2), read_assigned.notified())
                .await
                .context("executor did not block inside bootstrap command recovery")?;

            tokio::time::timeout(Duration::from_secs(2), executor.handoff())
                .await
                .context("direct handoff was not observed during blocked bootstrap")??;
            let executor_result = tokio::time::timeout(Duration::from_secs(1), executor_task)
                .await
                .context("bootstrap executor did not stop after direct handoff")?
                .context("bootstrap executor task panicked")?;
            executor_result?;
            assert!(
                !harness
                    .raw
                    .exists::<_, bool>(harness.guard.lease_key())
                    .await?,
                "direct bootstrap handoff left the exact lease until TTL expiry"
            );

            let successor_owner = BootIdentity::new();
            let mut owners = serde_json::Map::new();
            owners.insert(
                harness.partition.to_string(),
                serde_json::Value::String(successor_owner.to_string()),
            );
            let _: () = harness
                .raw
                .set(
                    harness.namespace.partition_assignment(harness.partition),
                    serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
                )
                .await?;
            let successor_guard = harness
                .leases
                .try_acquire(harness.partition, &successor_owner)
                .await?
                .context("successor could not acquire after direct bootstrap handoff")?;

            let mut successor_consumer = harness
                .bus
                .subscribe_executor_commands(successor_guard.clone())
                .await?;
            let reclaimed = successor_consumer.reclaim_next().await?;
            assert_eq!(reclaimed.deliveries.len(), 1);
            assert_eq!(reclaimed.deliveries[0].stream_id, pending_id);

            harness.cleanup(&successor_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn materialization_backoff_blocks_ordinary_terminal_checkpoints_and_snapshots()
    -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("terminal-materialization-backoff").await?;
            let baseline = harness.recovery().await?;
            assert!(!baseline.game_state.is_complete());

            let now = chrono::Utc::now().timestamp_millis();
            let mut terminal_state = GameState::new(
                10,
                10,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                Some(11),
                now,
            );
            terminal_state.status = GameStatus::Started { server_id: 1 };
            let snake_id = terminal_state
                .add_player(77, Some("player-77".into()))?
                .snake_id;
            terminal_state.status = GameStatus::Complete {
                winning_snake_id: Some(snake_id),
            };
            let terminal_envelope = RecoveryEnvelopeV2::new(
                harness.game_id,
                harness.partition,
                terminal_state,
                baseline.command_cursor.clone(),
                ResolvedCommandState::default(),
                0,
                baseline.next_event_stream_sequence,
                now,
                harness.guard.encoded_token(),
            );
            let mut actor = harness.actor(terminal_envelope, harness.guard.clone());
            actor.live = true;

            // Competitive completion requires an MMR read. The test database
            // returns a transient failure, so the game remains terminal-pending.
            actor.advance_live().await?;
            assert!(actor.terminal_pending());
            assert!(actor.completion_materialization_retry_at.is_some());

            // Exercise both paths that previously leaked terminal state while
            // the one-second materialization retry backoff was active.
            actor.checkpoint().await?;
            actor.publish_fresh_snapshot().await?;
            actor.checkpoint().await?;

            let persisted = harness.recovery().await?;
            assert!(
                !persisted.game_state.is_complete(),
                "periodic checkpoint replaced the last non-terminal recovery state"
            );
            assert!(
                !harness
                    .bus
                    .get_stored_snapshot(harness.game_id)
                    .await?
                    .context("baseline stored snapshot disappeared")?
                    .is_complete(),
                "snapshot request persisted terminal state during materialization backoff"
            );
            assert!(
                read_game_events(&mut harness.raw, harness.partition)
                    .await?
                    .iter()
                    .all(|message| match &message.event {
                        GameEvent::StatusUpdated {
                            status: GameStatus::Complete { .. },
                        } => false,
                        GameEvent::Snapshot { game_state } => !game_state.is_complete(),
                        _ => true,
                    }),
                "snapshot request published Complete before materialization succeeded"
            );
            let completion: Option<Vec<u8>> = harness
                .raw
                .get(harness.namespace.completion(harness.game_id))
                .await?;
            assert!(completion.is_none());

            harness.cleanup(&actor.guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn completed_game_exact_and_pruned_duplicates_are_acked_without_publication() -> Result<()>
    {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("terminal-resolved-duplicates").await?;
            let mut terminal_state = harness.recovery().await?.game_state;
            terminal_state.status = GameStatus::Complete {
                winning_snake_id: None,
            };
            let session_id = format!("terminal-session-{}", harness.game_id);
            let pruned_identity = ClientCommandIdentityV2 {
                game_id: harness.game_id,
                user_id: 77,
                client_game_session_id: session_id.clone(),
                sequence: 1,
            };
            let exact_identity = ClientCommandIdentityV2 {
                sequence: 2,
                ..pruned_identity.clone()
            };
            let exact_outcome = CommandOutcome::Rejected {
                reason: "already resolved".to_string(),
            };
            let resolved = ResolvedCommandState {
                sessions: std::collections::BTreeMap::from([(
                    ResolvedCommandState::session_key(&pruned_identity),
                    crate::recovery::SessionCommandOutcomes {
                        contiguous_through: 2,
                        outcomes: std::collections::BTreeMap::from([(
                            exact_identity.sequence,
                            exact_outcome,
                        )]),
                    },
                )]),
            };
            let envelope = RecoveryEnvelopeV2::new(
                harness.game_id,
                harness.partition,
                terminal_state,
                "0-0".into(),
                resolved,
                0,
                0,
                chrono::Utc::now().timestamp_millis(),
                harness.guard.encoded_token(),
            );
            let mut actor = harness.actor(envelope, harness.guard.clone());
            actor.live = true;
            actor.completion_committed = true;

            let command = match &harness.command {
                StreamEvent::GameCommandSubmittedV2 { command, .. } => command.clone(),
                _ => unreachable!("harness command is always V2"),
            };
            for identity in [&pruned_identity, &exact_identity] {
                let event = StreamEvent::GameCommandSubmittedV2 {
                    game_id: harness.game_id,
                    user_id: identity.user_id,
                    command_id: identity.clone(),
                    command: command.clone(),
                };
                let _: String = harness
                    .raw
                    .xadd(
                        RedisKeys::stream_commands(harness.partition),
                        "*",
                        &[("data", serde_json::to_vec(&event)?)],
                    )
                    .await?;
            }
            let mut consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 2);
            for delivery in deliveries {
                assert!(matches!(
                    actor.incorporate(delivery).await?,
                    DeliveryDisposition::Incorporated
                ));
            }

            assert!(actor.pending_stream_ids.is_empty());
            assert_eq!(actor.command_cursor, "0-0");
            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());
            assert!(
                read_game_events(&mut harness.raw, harness.partition)
                    .await?
                    .is_empty(),
                "resolved terminal duplicates must not publish at the terminal tick"
            );

            harness.cleanup(&actor.guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_failure_budget_keeps_work_pending_until_success_or_fail_closed()
    -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut nine_seconds = CrashBoundaryHarness::new("checkpoint-nine-seconds").await?;
            let stream_id = nine_seconds.append_command().await?;
            let mut consumer = nine_seconds
                .bus
                .subscribe_executor_commands(nine_seconds.guard.clone())
                .await?;
            let mut deliveries = consumer.read_new_now().await?;
            let mut actor =
                nine_seconds.actor(nine_seconds.recovery().await?, nine_seconds.guard.clone());
            actor.incorporate(deliveries.remove(0)).await?;
            actor.last_checkpoint_success = Instant::now()
                .checked_sub(Duration::from_secs(9))
                .context("nine-second checkpoint age is representable")?;
            nine_seconds.bus.fail_next_checkpoints(1);
            actor.checkpoint().await?;
            assert!(actor.pending_stream_ids.is_empty());
            assert_eq!(nine_seconds.recovery().await?.command_cursor, stream_id);
            assert_eq!(
                nine_seconds
                    .bus
                    .xack_fenced(&nine_seconds.guard, std::slice::from_ref(&stream_id))
                    .await?,
                0
            );
            nine_seconds.cleanup(&actor.guard).await?;

            let mut eleven_seconds = CrashBoundaryHarness::new("checkpoint-eleven-seconds").await?;
            let stream_id = eleven_seconds.append_command().await?;
            let mut consumer = eleven_seconds
                .bus
                .subscribe_executor_commands(eleven_seconds.guard.clone())
                .await?;
            let mut deliveries = consumer.read_new_now().await?;
            let mut actor = eleven_seconds.actor(
                eleven_seconds.recovery().await?,
                eleven_seconds.guard.clone(),
            );
            actor.incorporate(deliveries.remove(0)).await?;
            actor.last_checkpoint_success = Instant::now()
                .checked_sub(Duration::from_secs(11))
                .context("eleven-second checkpoint age is representable")?;
            eleven_seconds.bus.fail_next_checkpoints(1);
            let error = actor
                .checkpoint()
                .await
                .expect_err("checkpoint age beyond ten seconds must fail closed");
            assert!(error.to_string().contains("exceeded fail-closed budget"));
            assert_eq!(actor.pending_stream_ids, vec![stream_id.clone()]);
            assert_eq!(eleven_seconds.recovery().await?.command_cursor, "0-0");
            let pending: redis::streams::StreamPendingCountReply = eleven_seconds
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(eleven_seconds.partition),
                    eleven_seconds
                        .namespace
                        .command_group(eleven_seconds.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert_eq!(pending.ids.len(), 1);
            assert_eq!(pending.ids[0].id, stream_id);
            eleven_seconds.cleanup(&actor.guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn game_created_dispatch_checkpoints_and_indexes_before_ack() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("game-created-dispatch").await?;
            let initial_state = harness.recovery().await?.game_state;
            let _: () = harness
                .raw
                .del(&[
                    harness.namespace.recovery(harness.game_id),
                    harness.namespace.active_games(harness.partition),
                    RedisKeys::game_snapshot(harness.game_id),
                ])
                .await?;
            let game_created = StreamEvent::GameCreated {
                game_id: harness.game_id,
                game_state: initial_state,
            };
            let stream_id: String = harness
                .raw
                .xadd(
                    RedisKeys::stream_commands(harness.partition),
                    "*",
                    &[("data", serde_json::to_vec(&game_created)?)],
                )
                .await?;
            let mut consumer = harness
                .bus
                .subscribe_executor_commands(harness.guard.clone())
                .await?;
            let deliveries = consumer.read_new_now().await?;
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].stream_id, stream_id);

            let fatal = CancellationToken::new();
            let handoff_cancel = fatal.child_token();
            let (actor_failures, mut failure_events) = mpsc::unbounded_channel();
            let mut actors = HashMap::new();
            let mut cursors = HashMap::new();
            dispatch_batch(
                deliveries,
                &mut actors,
                &mut cursors,
                2,
                &harness.bus,
                &harness.guard,
                Arc::new(UnusedDatabase::default()),
                RecoveryConfig::default(),
                fatal.clone(),
                handoff_cancel,
                actor_failures,
                true,
            )
            .await?;

            let checkpoint = harness.recovery().await?;
            assert_eq!(checkpoint.command_cursor, stream_id);
            assert!(
                harness
                    .raw
                    .sismember::<_, _, bool>(
                        harness.namespace.active_games(harness.partition),
                        harness.game_id,
                    )
                    .await?
            );
            assert_eq!(
                harness
                    .bus
                    .xack_fenced(&harness.guard, std::slice::from_ref(&stream_id))
                    .await?,
                0
            );

            fatal.cancel();
            for (_, slot) in actors {
                tokio::time::timeout(Duration::from_secs(1), slot._task).await??;
            }
            assert!(matches!(
                failure_events.try_recv(),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
            ));
            let live_guard = harness.guard.clone();
            harness.cleanup(&live_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn live_executor_preserves_assigned_read_across_snapshot_work() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut harness = CrashBoundaryHarness::new("preserve-live-read").await?;

            // Keep the actor alive while the test deliberately pauses command
            // delivery after Redis has assigned it to the consumer group.
            let mut baseline = harness.recovery().await?;
            baseline.game_state.start_ms = chrono::Utc::now().timestamp_millis() + 60_000;
            baseline.checkpointed_at_ms = chrono::Utc::now().timestamp_millis();
            harness
                .bus
                .checkpoint_and_ack_fenced(&harness.guard, &baseline, &[], Duration::from_secs(60))
                .await?;

            let mut events = harness
                .bus
                .subscribe_to_partition(harness.partition)
                .await?;
            let executor_cancel = CancellationToken::new();
            let config = RecoveryConfig {
                checkpoint_interval: Duration::from_millis(50),
                ..RecoveryConfig::default()
            };
            let (_handle, task) = spawn_game_executor_v2(
                2,
                harness.guard.clone(),
                harness.leases.clone(),
                harness.bus.clone(),
                Arc::new(UnusedDatabase::default()),
                config,
                executor_cancel.clone(),
            );
            wait_for_game_snapshot(&mut events, harness.game_id).await?;

            let (read_assigned, release_read_reply) =
                harness.bus.gate_next_nonempty_executor_read_reply();
            let stream_id = harness.append_command().await?;
            tokio::time::timeout(Duration::from_secs(2), read_assigned.notified())
                .await
                .context("executor did not assign the command-group entry")?;

            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert_eq!(pending.ids.len(), 1);
            assert_eq!(pending.ids[0].id, stream_id);

            // A snapshot request is ordinary live-loop work. It must not drop
            // the already-assigned read and leave that entry in the PEL.
            harness
                .bus
                .request_partition_snapshots(harness.partition)
                .await?;
            wait_for_game_snapshot(&mut events, harness.game_id).await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            release_read_reply.notify_one();

            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if harness.recovery().await?.command_cursor == stream_id {
                        return Result::<()>::Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("assigned command was stranded after snapshot work")??;

            let pending: redis::streams::StreamPendingCountReply = harness
                .raw
                .xpending_count(
                    RedisKeys::stream_commands(harness.partition),
                    harness.namespace.command_group(harness.partition),
                    "-",
                    "+",
                    10,
                )
                .await?;
            assert!(pending.ids.is_empty());

            executor_cancel.cancel();
            let executor_result = tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .context("executor did not stop after test cancellation")?
                .context("executor task panicked")?;
            executor_result?;

            let live_guard = harness.guard.clone();
            harness.cleanup(&live_guard).await?;
            Result::<()>::Ok(())
        })
        .await??;
        Ok(())
    }

    #[test]
    fn recovered_actor_preserves_persisted_checkpoint_age_budget() {
        let budget = Duration::from_secs(10);
        assert_eq!(
            persisted_checkpoint_age(1_000, 10_000, budget),
            Duration::from_secs(9)
        );
        assert_eq!(persisted_checkpoint_age(1_000, 12_000, budget), budget);
        assert_eq!(
            persisted_checkpoint_age(12_000, 10_000, budget),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn transient_materialization_failure_is_game_local_and_retried() {
        let mut retry_at = None;
        let mut first_attempt_polled = false;
        let record = materialize_completion_game_local(17, &mut retry_at, async {
            first_attempt_polled = true;
            Err::<CompletionRecordV1, _>(anyhow::anyhow!("transient DynamoDB read failure"))
        })
        .await;
        assert!(record.is_none());
        assert!(first_attempt_polled);
        assert!(retry_at.is_some_and(|deadline| deadline > Instant::now()));

        let mut premature_retry_polled = false;
        let record = materialize_completion_game_local(17, &mut retry_at, async {
            premature_retry_polled = true;
            Err::<CompletionRecordV1, _>(anyhow::anyhow!("must remain throttled"))
        })
        .await;
        assert!(record.is_none());
        assert!(!premature_retry_polled);

        retry_at = Some(
            Instant::now()
                .checked_sub(Duration::from_millis(1))
                .expect("one millisecond is representable"),
        );
        let mut retry_polled = false;
        let record = materialize_completion_game_local(17, &mut retry_at, async {
            retry_polled = true;
            Err::<CompletionRecordV1, _>(anyhow::anyhow!("second transient failure"))
        })
        .await;
        assert!(record.is_none());
        assert!(retry_polled);
    }

    #[tokio::test]
    async fn pending_completion_failure_does_not_starve_later_ids() {
        let mut attempted = Vec::new();
        let result = attempt_all_pending_completions(7, vec![11, 12, 13], |game_id| {
            attempted.push(game_id);
            std::future::ready(if game_id == 11 {
                Err(anyhow::anyhow!("permanent malformed completion"))
            } else {
                Ok(())
            })
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempted, vec![11, 12, 13]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_watchdog_renews_through_saturated_bootstrap() -> Result<()> {
        use crate::cluster_membership::{BootIdentity, ClusterNamespace};
        use redis::AsyncCommands;

        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let partition = 900_000 + (salt % 50_000) as u32;
        let namespace = ClusterNamespace::new(format!("watchdog-{salt}"))?;
        let boot_id = BootIdentity::new();
        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let mut raw = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            crate::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let _: () = raw
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let leases = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(1),
            Duration::from_millis(200),
        )?;
        let guard = leases
            .try_acquire(partition, &boot_id)
            .await?
            .context("test lease was not acquired")?;
        let stop = CancellationToken::new();
        let (watchdog, mut events) = spawn_lease_watchdog(
            leases.clone(),
            guard.clone(),
            stop.clone(),
            CancellationToken::new(),
        );

        // Keep a recovery-like task continuously runnable for more than twice
        // the lease TTL. The watchdog has to make progress independently; an
        // inline renewal selected behind this work would expire deterministically.
        let deadline = Instant::now() + Duration::from_millis(2_200);
        let mut work = 0_u64;
        while Instant::now() < deadline {
            for value in 0..10_000_u64 {
                work = std::hint::black_box(work.wrapping_add(value));
            }
            tokio::task::yield_now().await;
        }
        assert_ne!(work, 0);
        assert!(leases.validate(&guard).await?);
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        stop.cancel();
        watchdog.await?;
        let _: () = raw
            .del(&[namespace.partition_assignment(partition), guard.lease_key()])
            .await?;
        Ok(())
    }

    #[test]
    fn v2_command_identity_is_present_on_the_stream_event() {
        let event = StreamEvent::GameCommandSubmittedV2 {
            game_id: 7,
            user_id: 9,
            command_id: ClientCommandIdentityV2 {
                game_id: 7,
                user_id: 9,
                client_game_session_id: "session".into(),
                sequence: 1,
            },
            command: GameCommandMessage {
                command_id_client: common::CommandId {
                    tick: 0,
                    user_id: 9,
                    sequence_number: 1,
                },
                command_id_server: None,
                command: common::GameCommand::Turn {
                    snake_id: 1,
                    direction: common::Direction::Up,
                },
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("client_game_session_id"));
    }
}
