//! Process-level executor failover checks against a dedicated local Valkey DB.
//!
//! This deliberately uses real child processes. The parent kills or suspends
//! the incumbent after Redis has put a command in that process's consumer PEL,
//! then proves that a successor reclaims, checkpoints, and ACKs the command.
//! The pause/resume case additionally proves that the resumed incumbent cannot
//! overwrite the successor's checkpoint with its stale lease token.

#![cfg(unix)]

use anyhow::{Context, Result, bail, ensure};
use common::{GameEvent, GameEventMessage, GameState, GameStatus, GameType, QueueMode};
use redis::AsyncCommands;
use redis::streams::StreamPendingReply;
use serde::{Deserialize, Serialize};
use server::cluster_membership::{
    BootIdentity, ClusterNamespace, DEFAULT_MEMBERSHIP_TTL, TaskLifecycle, TaskMembership,
};
use server::game_bus::{CommandDelivery, CommandDeliveryPayload, GameBus};
use server::game_executor::{PARTITION_COUNT, StreamEvent};
use server::partition_assignment::{
    AssignmentDocument, AssignmentStore, AssignmentWrite, balanced_minimal_movement,
};
use server::partition_lease::{
    CoordinatorLeaseStore, DEFAULT_COORDINATION_OPERATION_TIMEOUT, DEFAULT_PARTITION_LEASE_TTL,
    LeaseToken, PartitionLeaseGuard, PartitionLeaseStore,
};
use server::recovery::{RecoveryEnvelopeV2, ResolvedCommandState};
use server::redis_keys::RedisKeys;
use std::env;
use std::io::{self, Write};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const DEFAULT_CHAOS_REDIS_URL: &str = "redis://127.0.0.1:6379/14?protocol=resp3";
const CHAOS_REDIS_URL_ENV: &str = "SNAKETRON_PROCESS_CHAOS_REDIS_URL";
const WORKER_ROLE_ENV: &str = "SNAKETRON_PROCESS_CHAOS_WORKER_ROLE";
const WORKER_SCENARIO_ENV: &str = "SNAKETRON_PROCESS_CHAOS_WORKER_SCENARIO";
const WORKER_NAMESPACE_ENV: &str = "SNAKETRON_PROCESS_CHAOS_NAMESPACE";
const WORKER_BOOT_ID_ENV: &str = "SNAKETRON_PROCESS_CHAOS_BOOT_ID";
const WORKER_PARTITION_ENV: &str = "SNAKETRON_PROCESS_CHAOS_PARTITION";
const WORKER_GAME_ID_ENV: &str = "SNAKETRON_PROCESS_CHAOS_GAME_ID";
const EVENT_PREFIX: &str = "SNAKETRON_PROCESS_CHAOS_EVENT ";
const LOCK_KEY: &str = "snaketron:test:executor-process-chaos:db14-lock";
const LEASE_TTL: Duration = DEFAULT_PARTITION_LEASE_TTL;
const OPERATION_TIMEOUT: Duration = DEFAULT_COORDINATION_OPERATION_TIMEOUT;
const RENEW_INTERVAL: Duration = Duration::from_millis(200);
const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);
const RECOVERY_RETENTION: Duration = Duration::from_secs(60);
const RECOVERABLE_REPLACEMENT_DELAY: Duration = Duration::from_secs(30);
const EXPIRED_REPLACEMENT_DELAY: Duration = Duration::from_secs(31);
const PARTITION: u32 = 7;

#[derive(Debug, Clone, Copy)]
enum Fault {
    Sigkill,
    PauseResume,
}

impl Fault {
    fn label(self) -> &'static str {
        match self {
            Self::Sigkill => "sigkill",
            Self::PauseResume => "pause-resume",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerRole {
    Incumbent,
    Successor,
}

impl WorkerRole {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "incumbent" => Ok(Self::Incumbent),
            "successor" => Ok(Self::Successor),
            other => bail!("unknown process-chaos worker role {other:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Incumbent => "incumbent",
            Self::Successor => "successor",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerScenario {
    PendingCommand,
    RetainedGame,
}

impl WorkerScenario {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending-command" => Ok(Self::PendingCommand),
            "retained-game" => Ok(Self::RetainedGame),
            other => bail!("unknown process-chaos worker scenario {other:?}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::PendingCommand => "pending-command",
            Self::RetainedGame => "retained-game",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerEvent {
    event: String,
    boot_id: String,
    lease_token: Option<String>,
    coordinator_token: Option<String>,
    assignment_version: Option<u64>,
    stream_id: Option<String>,
}

struct WorkerProcess {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    transcript: Vec<String>,
}

impl WorkerProcess {
    async fn spawn(
        role: WorkerRole,
        scenario: WorkerScenario,
        redis_url: &str,
        namespace: &ClusterNamespace,
        boot_id: &BootIdentity,
        game_id: u32,
    ) -> Result<Self> {
        let executable =
            env::current_exe().context("failed to locate process-chaos test binary")?;
        let mut child = Command::new(executable)
            .args([
                "--exact",
                "process_chaos_worker_entrypoint",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(WORKER_ROLE_ENV, role.as_str())
            .env(WORKER_SCENARIO_ENV, scenario.as_str())
            .env(CHAOS_REDIS_URL_ENV, redis_url)
            .env(WORKER_NAMESPACE_ENV, namespace.region())
            .env(WORKER_BOOT_ID_ENV, boot_id.as_str())
            .env(WORKER_PARTITION_ENV, PARTITION.to_string())
            .env(WORKER_GAME_ID_ENV, game_id.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn {} worker", role.as_str()))?;
        let stdout = child.stdout.take().context("worker stdout was not piped")?;
        Ok(Self {
            child,
            lines: BufReader::new(stdout).lines(),
            transcript: Vec::new(),
        })
    }

    fn id(&self) -> Result<u32> {
        self.child.id().context("worker process has no PID")
    }

    async fn wait_for_event(&mut self, expected: &str) -> Result<WorkerEvent> {
        let event = timeout(PROCESS_TIMEOUT, async {
            while let Some(line) = self.lines.next_line().await? {
                self.transcript.push(line.clone());
                let Some(offset) = line.find(EVENT_PREFIX) else {
                    continue;
                };
                let payload = &line[offset + EVENT_PREFIX.len()..];
                let event: WorkerEvent = serde_json::from_str(payload)
                    .with_context(|| format!("worker emitted malformed event: {payload}"))?;
                if event.event == expected {
                    return Ok(event);
                }
            }
            bail!(
                "worker exited before {expected}; transcript: {:?}",
                self.transcript
            )
        })
        .await
        .with_context(|| {
            format!(
                "worker did not emit {expected} within {:?}; transcript: {:?}",
                PROCESS_TIMEOUT, self.transcript
            )
        })??;
        Ok(event)
    }

    async fn signal(&self, signal: &str) -> Result<()> {
        let pid = self.id()?;
        let status = Command::new("/bin/kill")
            .args([format!("-{signal}"), pid.to_string()])
            .status()
            .await
            .with_context(|| format!("failed to send SIG{signal} to worker {pid}"))?;
        ensure!(status.success(), "SIG{signal} failed for worker {pid}");
        Ok(())
    }

    async fn wait_for_exit(&mut self) -> Result<std::process::ExitStatus> {
        timeout(PROCESS_TIMEOUT, self.child.wait())
            .await
            .context("worker did not exit before the process timeout")?
            .context("failed to wait for worker")
    }

    async fn kill_and_wait(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_none() {
            self.signal("KILL").await?;
        }
        let _ = self.wait_for_exit().await?;
        Ok(())
    }
}

async fn connection_manager(redis_url: &str) -> Result<redis::aio::ConnectionManager> {
    let client = redis::Client::open(redis_url).context("invalid process-chaos Redis URL")?;
    redis::aio::ConnectionManager::new(client)
        .await
        .context("failed to connect to process-chaos Valkey DB")
}

fn validate_dedicated_redis_url(redis_url: &str) -> Result<()> {
    let parsed = url::Url::parse(redis_url).context("invalid process-chaos Redis URL")?;
    ensure!(
        matches!(parsed.scheme(), "redis" | "rediss"),
        "process-chaos Redis URL must use redis:// or rediss://"
    );
    ensure!(
        matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")),
        "process-chaos tests refuse non-loopback Redis hosts"
    );
    ensure!(
        parsed.path() == "/14",
        "process-chaos tests require dedicated Redis database 14"
    );
    Ok(())
}

async fn game_bus(redis_url: &str) -> Result<Arc<GameBus>> {
    let client = redis::Client::open(redis_url).context("invalid process-chaos Redis URL")?;
    let redis = redis::aio::ConnectionManager::new(client.clone()).await?;
    Ok(Arc::new(GameBus::new(
        redis.clone(),
        redis,
        client,
        CancellationToken::new(),
    )))
}

fn worker_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing {name} for process-chaos worker"))
}

fn delivery_state(delivery: CommandDelivery, expected_game_id: u32) -> Result<(String, GameState)> {
    match delivery.payload {
        CommandDeliveryPayload::Command(StreamEvent::GameCreated {
            game_id,
            game_state,
        }) if game_id == expected_game_id => Ok((delivery.stream_id, game_state)),
        other => {
            bail!("worker received an unexpected command for game {expected_game_id}: {other:?}")
        }
    }
}

fn envelope(
    game_id: u32,
    partition: u32,
    game_state: GameState,
    stream_id: String,
    guard: &PartitionLeaseGuard,
) -> RecoveryEnvelopeV2 {
    RecoveryEnvelopeV2::new(
        game_id,
        partition,
        game_state,
        stream_id,
        ResolvedCommandState::default(),
        0,
        0,
        chrono::Utc::now().timestamp_millis(),
        guard.encoded_token(),
    )
}

fn emit_worker_event(event: &str, guard: &PartitionLeaseGuard, stream_id: Option<String>) {
    let payload = WorkerEvent {
        event: event.to_owned(),
        boot_id: guard.token().boot_id.to_string(),
        lease_token: Some(guard.encoded_token()),
        coordinator_token: None,
        assignment_version: None,
        stream_id,
    };
    println!(
        "{EVENT_PREFIX}{}",
        serde_json::to_string(&payload).expect("worker event is serializable")
    );
    io::stdout().flush().expect("worker stdout flush succeeds");
}

fn emit_assignment_event(
    boot_id: &BootIdentity,
    coordinator_token: &LeaseToken,
    assignment: &AssignmentDocument,
) {
    let payload = WorkerEvent {
        event: "ASSIGNED".to_owned(),
        boot_id: boot_id.to_string(),
        lease_token: None,
        coordinator_token: Some(coordinator_token.encode()),
        assignment_version: Some(assignment.version),
        stream_id: None,
    };
    println!(
        "{EVENT_PREFIX}{}",
        serde_json::to_string(&payload).expect("worker event is serializable")
    );
    io::stdout().flush().expect("worker stdout flush succeeds");
}

async fn acquire_lease(
    store: &PartitionLeaseStore,
    partition: u32,
    boot_id: &BootIdentity,
) -> Result<PartitionLeaseGuard> {
    let deadline = tokio::time::Instant::now() + PROCESS_TIMEOUT;
    loop {
        if let Some(guard) = store.try_acquire(partition, boot_id).await? {
            return Ok(guard);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "worker could not acquire partition {partition} before timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn acquire_lease_while_coordinating(
    store: &PartitionLeaseStore,
    coordinator: &CoordinatorLeaseStore,
    coordinator_token: &LeaseToken,
    partition: u32,
    boot_id: &BootIdentity,
) -> Result<PartitionLeaseGuard> {
    let deadline = tokio::time::Instant::now() + PROCESS_TIMEOUT;
    let mut renew = tokio::time::interval(RENEW_INTERVAL);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            acquired = store.try_acquire(partition, boot_id) => {
                if let Some(guard) = acquired? {
                    return Ok(guard);
                }
            }
            _ = renew.tick() => {
                ensure!(
                    coordinator.renew(coordinator_token).await?,
                    "successor lost its coordinator term before partition takeover"
                );
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "coordinating worker could not acquire partition {partition} before timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn publish_successor_assignment(
    redis: redis::aio::ConnectionManager,
    namespace: &ClusterNamespace,
    boot_id: &BootIdentity,
) -> Result<(CoordinatorLeaseStore, LeaseToken, AssignmentDocument)> {
    let coordinator = CoordinatorLeaseStore::new(
        redis.clone(),
        namespace.clone(),
        LEASE_TTL,
        OPERATION_TIMEOUT,
    )?;
    let coordinator_token = coordinator
        .try_acquire(boot_id)
        .await?
        .context("successor did not acquire the assignment coordinator lease")?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let member = TaskMembership::new(
        boot_id.clone(),
        2,
        Some("process-chaos-successor".to_owned()),
        Some("process-chaos-v2".to_owned()),
        TaskLifecycle::Active,
        now_ms,
        DEFAULT_MEMBERSHIP_TTL,
    );
    let assignment_store = AssignmentStore::new(redis, namespace.clone());
    let (write, assignment) = assignment_store
        .reconcile(
            &coordinator_token.encode(),
            PARTITION_COUNT,
            &[member],
            now_ms,
        )
        .await?;
    ensure!(
        write == AssignmentWrite::Written,
        "successor failed to publish its assignment: {write:?}"
    );
    assignment.validate(PARTITION_COUNT)?;
    ensure!(
        assignment.desired_owner(PARTITION) == Some(boot_id),
        "successor assignment did not grant its target partition"
    );
    Ok((coordinator, coordinator_token, assignment))
}

async fn read_incumbent_command(
    consumer: &mut server::game_bus::ExecutorCommandConsumer,
    store: &PartitionLeaseStore,
    guard: &PartitionLeaseGuard,
) -> Result<CommandDelivery> {
    let deadline = tokio::time::Instant::now() + PROCESS_TIMEOUT;
    let mut renew = tokio::time::interval(RENEW_INTERVAL);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            deliveries = consumer.read_new_now() => {
                if let Some(delivery) = deliveries?.into_iter().next() {
                    return Ok(delivery);
                }
                // Production's blocking-style executor reader performs this
                // wait locally. Mirror it here instead of flooding one shared
                // dispatcher with an unbounded empty-read loop.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            _ = renew.tick() => {
                ensure!(store.renew(guard).await?, "incumbent lost its lease before fault injection");
            }
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "incumbent did not receive the command before timeout"
        );
    }
}

async fn reclaim_successor_command(
    consumer: &mut server::game_bus::ExecutorCommandConsumer,
) -> Result<CommandDelivery> {
    let deadline = tokio::time::Instant::now() + PROCESS_TIMEOUT;
    loop {
        let batch = consumer.reclaim_next().await?;
        ensure!(
            batch.deleted_pending_ids.is_empty(),
            "pending command was trimmed before process takeover: {:?}",
            batch.deleted_pending_ids
        );
        if let Some(delivery) = batch.deliveries.into_iter().next() {
            return Ok(delivery);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "successor did not reclaim the pending command before timeout"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn run_worker() -> Result<()> {
    let role = WorkerRole::parse(&worker_env(WORKER_ROLE_ENV)?)?;
    let scenario = WorkerScenario::parse(&worker_env(WORKER_SCENARIO_ENV)?)?;
    let redis_url = worker_env(CHAOS_REDIS_URL_ENV)?;
    validate_dedicated_redis_url(&redis_url)?;
    let namespace = ClusterNamespace::new(worker_env(WORKER_NAMESPACE_ENV)?)?;
    let boot_id = BootIdentity::parse(worker_env(WORKER_BOOT_ID_ENV)?)?;
    let partition: u32 = worker_env(WORKER_PARTITION_ENV)?.parse()?;
    let game_id: u32 = worker_env(WORKER_GAME_ID_ENV)?.parse()?;
    ensure!(game_id % PARTITION_COUNT == partition);

    let redis = connection_manager(&redis_url).await?;
    let store = PartitionLeaseStore::new(redis, namespace.clone(), LEASE_TTL, OPERATION_TIMEOUT)?;
    let bus = game_bus(&redis_url).await?;
    bus.ensure_executor_command_group(&namespace, partition)
        .await?;
    let assignment_authority =
        if role == WorkerRole::Successor && scenario == WorkerScenario::PendingCommand {
            let authority = publish_successor_assignment(
                connection_manager(&redis_url).await?,
                &namespace,
                &boot_id,
            )
            .await?;
            emit_assignment_event(&boot_id, &authority.1, &authority.2);
            Some(authority)
        } else {
            None
        };
    let guard = if let Some((coordinator, coordinator_token, _)) = &assignment_authority {
        acquire_lease_while_coordinating(
            &store,
            coordinator,
            coordinator_token,
            partition,
            &boot_id,
        )
        .await?
    } else {
        acquire_lease(&store, partition, &boot_id).await?
    };
    let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
    emit_worker_event("READY", &guard, None);

    match (role, scenario) {
        (WorkerRole::Incumbent, WorkerScenario::PendingCommand) => {
            let delivery = read_incumbent_command(&mut consumer, &store, &guard).await?;
            let (stream_id, state) = delivery_state(delivery, game_id)?;
            let stale_envelope = envelope(game_id, partition, state, stream_id.clone(), &guard);
            emit_worker_event("CLAIMED", &guard, Some(stream_id.clone()));

            let deadline = tokio::time::Instant::now() + PROCESS_TIMEOUT;
            let mut renew = tokio::time::interval(RENEW_INTERVAL);
            loop {
                renew.tick().await;
                match store.renew(&guard).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => break,
                }
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "incumbent never observed ownership transfer"
                );
            }
            let stale_write = bus
                .checkpoint_and_ack_fenced(
                    &guard,
                    &stale_envelope,
                    std::slice::from_ref(&stream_id),
                    RECOVERY_RETENTION,
                )
                .await;
            let stale_error = stale_write
                .expect_err("resumed stale incumbent unexpectedly checkpointed and ACKed");
            ensure!(
                stale_error
                    .to_string()
                    .contains("stale partition lease rejected checkpoint/ACK"),
                "stale checkpoint failed for the wrong reason: {stale_error:#}"
            );
            emit_worker_event("FENCED", &guard, Some(stream_id));
        }
        (WorkerRole::Successor, WorkerScenario::PendingCommand) => {
            let delivery = reclaim_successor_command(&mut consumer).await?;
            let (stream_id, state) = delivery_state(delivery, game_id)?;
            let recovered = envelope(game_id, partition, state.clone(), stream_id.clone(), &guard);
            let acked = bus
                .checkpoint_and_ack_fenced(
                    &guard,
                    &recovered,
                    std::slice::from_ref(&stream_id),
                    RECOVERY_RETENTION,
                )
                .await?;
            ensure!(acked == 1, "successor checkpoint ACKed {acked} commands");
            bus.publish_event_fenced(
                &guard,
                &GameEventMessage {
                    game_id,
                    tick: state.tick,
                    sequence: state.event_sequence,
                    stream_seq: 1,
                    user_id: None,
                    event: GameEvent::Snapshot { game_state: state },
                },
            )
            .await?;
            emit_worker_event("RECOVERED", &guard, Some(stream_id));

            let mut renew = tokio::time::interval(RENEW_INTERVAL);
            loop {
                renew.tick().await;
                ensure!(
                    store.renew(&guard).await?,
                    "successor unexpectedly lost its lease"
                );
                if let Some((coordinator, coordinator_token, _)) = &assignment_authority {
                    ensure!(
                        coordinator.renew(coordinator_token).await?,
                        "successor unexpectedly lost its coordinator term"
                    );
                }
            }
        }
        (WorkerRole::Incumbent, WorkerScenario::RetainedGame) => {
            let delivery = read_incumbent_command(&mut consumer, &store, &guard).await?;
            let (stream_id, state) = delivery_state(delivery, game_id)?;
            let checkpoint = envelope(game_id, partition, state, stream_id.clone(), &guard);
            let acked = bus
                .checkpoint_and_ack_fenced(
                    &guard,
                    &checkpoint,
                    std::slice::from_ref(&stream_id),
                    RECOVERY_RETENTION,
                )
                .await?;
            ensure!(acked == 1, "retention incumbent ACKed {acked} commands");
            emit_worker_event("CHECKPOINTED", &guard, Some(stream_id));

            let mut renew = tokio::time::interval(RENEW_INTERVAL);
            loop {
                renew.tick().await;
                ensure!(
                    store.renew(&guard).await?,
                    "retention incumbent unexpectedly lost its lease"
                );
            }
        }
        (WorkerRole::Successor, WorkerScenario::RetainedGame) => {
            let mut recovered = bus
                .load_partition_recovery_fenced(&guard, RECOVERY_RETENTION)
                .await?
                .into_iter()
                .find(|candidate| candidate.game_id == game_id)
                .context("retention successor did not load the checkpointed game")?;
            let stream_id = recovered.command_cursor.clone();
            recovered.source_lease_token = guard.encoded_token();
            recovered.checkpointed_at_ms = chrono::Utc::now().timestamp_millis();
            bus.checkpoint_and_ack_fenced(&guard, &recovered, &[], RECOVERY_RETENTION)
                .await?;
            bus.publish_event_fenced(
                &guard,
                &GameEventMessage {
                    game_id,
                    tick: recovered.game_state.tick,
                    sequence: recovered.game_state.event_sequence,
                    stream_seq: recovered.next_event_stream_sequence + 1,
                    user_id: None,
                    event: GameEvent::Snapshot {
                        game_state: recovered.game_state,
                    },
                },
            )
            .await?;
            emit_worker_event("RECOVERED", &guard, Some(stream_id));

            let mut renew = tokio::time::interval(RENEW_INTERVAL);
            loop {
                renew.tick().await;
                ensure!(
                    store.renew(&guard).await?,
                    "retention successor unexpectedly lost its lease"
                );
            }
        }
    }
    Ok(())
}

/// `current_exe()` starts this exact test as the child executor process.
#[test]
fn process_chaos_worker_entrypoint() {
    if env::var_os(WORKER_ROLE_ENV).is_none() {
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("worker Tokio runtime builds")
        .block_on(run_worker())
        .expect("process-chaos worker succeeds");
}

async fn write_assignment(
    redis: &mut redis::aio::ConnectionManager,
    namespace: &ClusterNamespace,
    owner: &BootIdentity,
) -> Result<()> {
    let assignment = balanced_minimal_movement(
        PARTITION_COUNT,
        None,
        [owner.clone()],
        namespace.region(),
        1,
        chrono::Utc::now().timestamp_millis(),
    );
    assignment.validate(PARTITION_COUNT)?;
    let payload = serde_json::to_vec(&assignment)?;
    redis
        .set::<_, _, ()>(namespace.assignment(), payload.clone())
        .await
        .context("failed to write process-chaos canonical assignment")?;
    for partition in 0..PARTITION_COUNT {
        redis
            .set::<_, _, ()>(namespace.partition_assignment(partition), payload.clone())
            .await
            .context("failed to write process-chaos partition assignment")?;
    }
    Ok(())
}

async fn cleanup_scenario(
    redis: &mut redis::aio::ConnectionManager,
    namespace: &ClusterNamespace,
    game_id: u32,
) -> Result<()> {
    for partition in 0..PARTITION_COUNT {
        redis
            .del::<_, ()>(namespace.partition_assignment(partition))
            .await
            .context("failed to remove process-chaos partition assignment")?;
    }
    redis
        .del::<_, ()>(&[
            namespace.assignment(),
            namespace.assignment_lease(),
            namespace.partition_lease(PARTITION),
            namespace.recovery(game_id),
            namespace.recovery_failure(game_id),
            namespace.active_games(PARTITION),
            namespace.command_quarantine(PARTITION),
            RedisKeys::game_snapshot(game_id),
            RedisKeys::stream_commands(PARTITION),
            RedisKeys::stream_events(PARTITION),
        ])
        .await
        .context("failed to remove exact process-chaos keys")
}

async fn assert_successor_state(
    redis: &mut redis::aio::ConnectionManager,
    bus: &GameBus,
    namespace: &ClusterNamespace,
    game_id: u32,
    recovered_event: &WorkerEvent,
    expected_stream_id: &str,
) -> Result<()> {
    let recovered_lease_token = recovered_event
        .lease_token
        .as_deref()
        .context("successor recovery event omitted its partition lease token")?;
    ensure!(
        recovered_event.stream_id.as_deref() == Some(expected_stream_id),
        "successor recovered a different stream entry"
    );
    let recovered = bus
        .get_recovery(namespace, game_id)
        .await?
        .context("successor did not persist a recovery envelope")?;
    ensure!(recovered.command_cursor == expected_stream_id);
    ensure!(recovered.source_lease_token == recovered_lease_token);
    ensure!(
        redis
            .sismember::<_, _, bool>(namespace.active_games(PARTITION), game_id)
            .await?,
        "successor did not restore the active-game index"
    );
    let live_token: Option<String> = redis.get(namespace.partition_lease(PARTITION)).await?;
    ensure!(live_token.as_deref() == Some(recovered_lease_token));

    let pending: StreamPendingReply = redis
        .xpending(
            RedisKeys::stream_commands(PARTITION),
            namespace.command_group(PARTITION),
        )
        .await?;
    let pending_count = match pending {
        StreamPendingReply::Empty => 0,
        StreamPendingReply::Data(summary) => summary.count,
    };
    ensure!(
        pending_count == 0,
        "successor left {pending_count} commands pending"
    );
    let events: redis::streams::StreamRangeReply = redis
        .xrange_all(RedisKeys::stream_events(PARTITION))
        .await?;
    ensure!(
        events.ids.len() == 1,
        "successor published {} authoritative outputs instead of one",
        events.ids.len()
    );
    let payload: String = redis::from_redis_value(
        events.ids[0]
            .map
            .get("data")
            .context("successor output omitted its event payload")?,
    )?;
    let output: GameEventMessage = serde_json::from_str(&payload)?;
    ensure!(output.game_id == game_id);
    let GameEvent::Snapshot { game_state } = output.event else {
        bail!("successor output was not a recovery snapshot");
    };
    ensure!(
        serde_json::to_value(game_state)? == serde_json::to_value(&recovered.game_state)?,
        "successor output did not contain the exact recovered authoritative state"
    );
    Ok(())
}

async fn assert_successor_assignment(
    redis: &mut redis::aio::ConnectionManager,
    namespace: &ClusterNamespace,
    successor_id: &BootIdentity,
    assigned_event: &WorkerEvent,
) -> Result<()> {
    ensure!(assigned_event.boot_id == successor_id.to_string());
    ensure!(
        assigned_event.assignment_version == Some(2),
        "successor published unexpected assignment version {:?}",
        assigned_event.assignment_version
    );
    let coordinator_token = assigned_event
        .coordinator_token
        .as_deref()
        .context("successor assignment event omitted its coordinator token")?;
    let live_coordinator_token: Option<String> = redis.get(namespace.assignment_lease()).await?;
    ensure!(
        live_coordinator_token.as_deref() == Some(coordinator_token),
        "successor coordinator token was not authoritative"
    );

    let assignment = AssignmentStore::new(redis.clone(), namespace.clone())
        .load()
        .await?
        .context("successor did not persist an assignment")?;
    assignment.validate(PARTITION_COUNT)?;
    ensure!(assignment.version == 2);
    ensure!(assignment.owners.len() == PARTITION_COUNT as usize);
    ensure!(assignment.eligible_members.as_slice() == std::slice::from_ref(successor_id));
    ensure!(
        assignment.desired_owner(PARTITION) == Some(successor_id),
        "successor was not the desired owner of the recovered partition"
    );
    Ok(())
}

fn unique_game_id() -> u32 {
    let raw = Uuid::new_v4().as_u128() as u32 % 100_000_000;
    raw * PARTITION_COUNT + PARTITION
}

async fn run_fault_scenario(redis_url: &str, fault: Fault) -> Result<()> {
    let namespace = ClusterNamespace::new(format!(
        "process-chaos-{}-{}",
        fault.label(),
        Uuid::new_v4()
    ))?;
    let game_id = unique_game_id();
    let incumbent_id = BootIdentity::new();
    let successor_id = BootIdentity::new();
    let mut redis = connection_manager(redis_url).await?;
    cleanup_scenario(&mut redis, &namespace, game_id).await?;

    let result = async {
        write_assignment(&mut redis, &namespace, &incumbent_id).await?;
        let initial_assignment = AssignmentStore::new(redis.clone(), namespace.clone())
            .load()
            .await?
            .context("incumbent assignment was not persisted")?;
        initial_assignment.validate(PARTITION_COUNT)?;
        ensure!(initial_assignment.version == 1);
        ensure!(
            initial_assignment.desired_owner(PARTITION) == Some(&incumbent_id),
            "initial assignment did not select the incumbent"
        );
        let mut incumbent = WorkerProcess::spawn(
            WorkerRole::Incumbent,
            WorkerScenario::PendingCommand,
            redis_url,
            &namespace,
            &incumbent_id,
            game_id,
        )
        .await?;
        let ready = incumbent.wait_for_event("READY").await?;
        ensure!(ready.boot_id == incumbent_id.to_string());

        let bus = game_bus(redis_url).await?;
        let state = GameState::new(
            12,
            12,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(42),
            chrono::Utc::now().timestamp_millis(),
        );
        bus.publish_command(
            PARTITION,
            &StreamEvent::GameCreated {
                game_id,
                game_state: state,
            },
        )
        .await?;
        let claimed = incumbent.wait_for_event("CLAIMED").await?;
        let stream_id = claimed
            .stream_id
            .clone()
            .context("incumbent claim omitted the stream ID")?;

        match fault {
            Fault::Sigkill => {
                incumbent.signal("KILL").await?;
                let status = incumbent.wait_for_exit().await?;
                ensure!(!status.success(), "SIGKILLed incumbent exited successfully");
            }
            Fault::PauseResume => incumbent.signal("STOP").await?,
        }
        // Measure from the completed process fault. Time spent scheduling the
        // harness's /bin/kill child is not executor outage time: until the
        // signal syscall runs, the incumbent is still alive and renewing.
        let fault_started = Instant::now();

        let mut successor = WorkerProcess::spawn(
            WorkerRole::Successor,
            WorkerScenario::PendingCommand,
            redis_url,
            &namespace,
            &successor_id,
            game_id,
        )
        .await?;
        let assigned = successor.wait_for_event("ASSIGNED").await?;
        assert_successor_assignment(&mut redis, &namespace, &successor_id, &assigned).await?;
        let recovered = successor.wait_for_event("RECOVERED").await?;
        ensure!(recovered.boot_id == successor_id.to_string());
        ensure!(
            fault_started.elapsed() < Duration::from_secs(5),
            "{} takeover exceeded five seconds: {:?}",
            fault.label(),
            fault_started.elapsed()
        );
        assert_successor_state(
            &mut redis,
            bus.as_ref(),
            &namespace,
            game_id,
            &recovered,
            &stream_id,
        )
        .await?;
        assert_successor_assignment(&mut redis, &namespace, &successor_id, &assigned).await?;

        if matches!(fault, Fault::PauseResume) {
            incumbent.signal("CONT").await?;
            let fenced = incumbent.wait_for_event("FENCED").await?;
            ensure!(fenced.lease_token == claimed.lease_token);
            let status = incumbent.wait_for_exit().await?;
            ensure!(
                status.success(),
                "resumed incumbent worker failed: {status}"
            );

            // The stale attempt must not replace the successor's durable state.
            assert_successor_state(
                &mut redis,
                bus.as_ref(),
                &namespace,
                game_id,
                &recovered,
                &stream_id,
            )
            .await?;
        }

        successor.kill_and_wait().await?;
        eprintln!(
            "process-chaos {} takeover recovered and ACKed command {} in {:?}",
            fault.label(),
            stream_id,
            fault_started.elapsed()
        );
        Result::<()>::Ok(())
    }
    .await;

    let cleanup = cleanup_scenario(&mut redis, &namespace, game_id).await;
    result.and(cleanup)
}

async fn checkpoint_then_sigkill(
    redis_url: &str,
    redis: &mut redis::aio::ConnectionManager,
    namespace: &ClusterNamespace,
    boot_id: &BootIdentity,
    game_id: u32,
    state: GameState,
) -> Result<String> {
    write_assignment(redis, namespace, boot_id).await?;
    let mut incumbent = WorkerProcess::spawn(
        WorkerRole::Incumbent,
        WorkerScenario::RetainedGame,
        redis_url,
        namespace,
        boot_id,
        game_id,
    )
    .await?;
    incumbent.wait_for_event("READY").await?;
    game_bus(redis_url)
        .await?
        .publish_command(
            PARTITION,
            &StreamEvent::GameCreated {
                game_id,
                game_state: state,
            },
        )
        .await?;
    let checkpointed = incumbent.wait_for_event("CHECKPOINTED").await?;
    let stream_id = checkpointed
        .stream_id
        .context("retention incumbent omitted its checkpointed command ID")?;
    incumbent.signal("KILL").await?;
    let status = incumbent.wait_for_exit().await?;
    ensure!(
        !status.success(),
        "SIGKILLed retention incumbent exited successfully"
    );
    Ok(stream_id)
}

/// Exercise the acceptance matrix's configured 60-second recovery retention at
/// its two documented sole-task replacement boundaries. Both incumbents checkpoint a live game and
/// then receive real SIGKILL. One replacement process starts after 30 seconds;
/// the other namespace has no replacement at all until its checkpoint is more
/// than 61 seconds old.
async fn run_recovery_retention_scenario(redis_url: &str) -> Result<()> {
    let recover_namespace =
        ClusterNamespace::new(format!("process-chaos-retain-{}", Uuid::new_v4()))?;
    let expire_namespace =
        ClusterNamespace::new(format!("process-chaos-expire-{}", Uuid::new_v4()))?;
    let recovered_game_id = unique_game_id();
    let expired_game_id = recovered_game_id + PARTITION_COUNT;
    let recover_incumbent_id = BootIdentity::new();
    let expire_incumbent_id = BootIdentity::new();
    let recover_successor_id = BootIdentity::new();
    let expire_successor_id = BootIdentity::new();
    let mut redis = connection_manager(redis_url).await?;
    cleanup_scenario(&mut redis, &recover_namespace, recovered_game_id).await?;
    cleanup_scenario(&mut redis, &expire_namespace, expired_game_id).await?;

    let result = async {
        let mut recovered_state = GameState::new(
            12,
            12,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(42),
            chrono::Utc::now().timestamp_millis(),
        );
        recovered_state.status = GameStatus::Started { server_id: 7 };
        let mut expired_state = GameState::new(
            14,
            14,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(43),
            chrono::Utc::now().timestamp_millis(),
        );
        expired_state.status = GameStatus::Started { server_id: 8 };

        let recovered_stream_id = checkpoint_then_sigkill(
            redis_url,
            &mut redis,
            &recover_namespace,
            &recover_incumbent_id,
            recovered_game_id,
            recovered_state.clone(),
        )
        .await?;
        // Command streams are regional rather than cluster-namespace scoped.
        // The first command is already durably checkpointed and ACKed, so
        // remove its isolated harness stream before creating the independent
        // 61-second namespace and consumer group.
        redis
            .del::<_, ()>(RedisKeys::stream_commands(PARTITION))
            .await?;
        checkpoint_then_sigkill(
            redis_url,
            &mut redis,
            &expire_namespace,
            &expire_incumbent_id,
            expired_game_id,
            expired_state,
        )
        .await?;
        let bus = game_bus(redis_url).await?;

        // Neither SIGKILLed process released or renewed its incumbent token.
        tokio::time::sleep(RECOVERABLE_REPLACEMENT_DELAY).await;
        ensure!(
            bus.get_recovery(&recover_namespace, recovered_game_id)
                .await?
                .is_some(),
            "60-second recovery data expired during the documented 30-second gap"
        );
        ensure!(
            bus.get_recovery(&expire_namespace, expired_game_id)
                .await?
                .is_some(),
            "control recovery data expired before its 60-second TTL"
        );

        write_assignment(&mut redis, &recover_namespace, &recover_successor_id).await?;
        let mut recover_successor = WorkerProcess::spawn(
            WorkerRole::Successor,
            WorkerScenario::RetainedGame,
            redis_url,
            &recover_namespace,
            &recover_successor_id,
            recovered_game_id,
        )
        .await?;
        let recovered_event = recover_successor.wait_for_event("RECOVERED").await?;
        assert_successor_state(
            &mut redis,
            bus.as_ref(),
            &recover_namespace,
            recovered_game_id,
            &recovered_event,
            &recovered_stream_id,
        )
        .await?;
        let recovered = bus
            .get_recovery(&recover_namespace, recovered_game_id)
            .await?
            .context("30-second replacement omitted its refreshed checkpoint")?;
        ensure!(
            serde_json::to_value(&recovered.game_state)? == serde_json::to_value(&recovered_state)?,
            "30-second replacement changed the authoritative game state"
        );

        tokio::time::sleep(EXPIRED_REPLACEMENT_DELAY).await;
        ensure!(
            bus.get_recovery(&recover_namespace, recovered_game_id)
                .await?
                .is_some(),
            "the 30-second replacement did not keep its recovered game durable"
        );
        write_assignment(&mut redis, &expire_namespace, &expire_successor_id).await?;
        let expire_successor_store = PartitionLeaseStore::new(
            connection_manager(redis_url).await?,
            expire_namespace.clone(),
            LEASE_TTL,
            OPERATION_TIMEOUT,
        )?;
        let expire_successor_guard = expire_successor_store
            .try_acquire(PARTITION, &expire_successor_id)
            .await?
            .context("61-second replacement did not acquire the expired incumbent lease")?;
        let after_expiry = bus
            .load_partition_recovery_fenced(&expire_successor_guard, RECOVERY_RETENTION)
            .await?;
        ensure!(
            after_expiry.is_empty(),
            "61-second replacement fabricated recoveries: {:?}",
            after_expiry
                .iter()
                .map(|candidate| candidate.game_id)
                .collect::<Vec<_>>()
        );
        let failure = bus
            .get_recovery_failure(&expire_namespace, expired_game_id)
            .await?
            .context("61-second replacement did not create an unrecoverable marker")?;
        ensure!(failure.game_id == expired_game_id && failure.partition_id == PARTITION);
        ensure!(
            bus.get_recovery(&expire_namespace, expired_game_id)
                .await?
                .is_none(),
            "expired game unexpectedly retained fabricated recovery state"
        );
        recover_successor.kill_and_wait().await?;
        Result::<()>::Ok(())
    }
    .await;

    let first_cleanup = cleanup_scenario(&mut redis, &recover_namespace, recovered_game_id).await;
    let second_cleanup = cleanup_scenario(&mut redis, &expire_namespace, expired_game_id).await;
    result.and(first_cleanup).and(second_cleanup)
}

async fn acquire_test_lock(redis: &mut redis::aio::ConnectionManager, token: &str) -> Result<()> {
    let acquired: Option<String> = redis::cmd("SET")
        .arg(LOCK_KEY)
        .arg(token)
        .arg("NX")
        .arg("PX")
        .arg(300_000)
        .query_async(redis)
        .await?;
    ensure!(
        acquired.as_deref() == Some("OK"),
        "another process-chaos test owns dedicated Valkey DB 14"
    );
    Ok(())
}

async fn release_test_lock(redis: &mut redis::aio::ConnectionManager, token: &str) -> Result<()> {
    let removed: i32 = redis::Script::new(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
    )
    .key(LOCK_KEY)
    .arg(token)
    .invoke_async(redis)
    .await?;
    ensure!(removed == 1, "process-chaos test lock expired unexpectedly");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_faults_recover_pending_commands_and_fence_stale_owner() -> Result<()> {
    if env::var_os(WORKER_ROLE_ENV).is_some() {
        return Ok(());
    }
    let redis_url =
        env::var(CHAOS_REDIS_URL_ENV).unwrap_or_else(|_| DEFAULT_CHAOS_REDIS_URL.to_owned());
    validate_dedicated_redis_url(&redis_url)?;
    let mut redis = connection_manager(&redis_url).await?;
    let lock_token = Uuid::new_v4().to_string();
    acquire_test_lock(&mut redis, &lock_token).await?;

    let result = async {
        let key_count: usize = redis::cmd("DBSIZE").query_async(&mut redis).await?;
        ensure!(
            key_count == 1,
            "dedicated Valkey DB 14 contains {key_count} keys besides the harness lock; refusing to delete shared data"
        );
        run_fault_scenario(&redis_url, Fault::Sigkill).await?;
        run_fault_scenario(&redis_url, Fault::PauseResume).await?;
        run_recovery_retention_scenario(&redis_url).await?;
        Result::<()>::Ok(())
    }
    .await;
    let unlock = release_test_lock(&mut redis, &lock_token).await;
    result.and(unlock)
}
