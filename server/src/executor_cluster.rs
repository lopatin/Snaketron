//! Runtime wiring for membership, assignment, and fenced partition executors.

use crate::cluster_membership::{
    BootIdentity, ClusterNamespace, MembershipStore, TaskLifecycle as ClusterTaskLifecycle,
    TaskMembership,
};
use crate::db::Database;
use crate::game_bus::GameBus;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor_v2::{PartitionExecutorV2Handle, spawn_game_executor_v2};
use crate::lifecycle::TaskLifecycle as LocalTaskLifecycle;
use crate::partition_assignment::AssignmentStore;
use crate::partition_lease::{
    CoordinatorLeaseStore, DEFAULT_COORDINATION_OPERATION_TIMEOUT, DEFAULT_PARTITION_LEASE_TTL,
    LeaseToken, PartitionLeaseStore,
};
use crate::recovery::RecoveryConfig;
use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const STATE_WARMING: u8 = 0;
const STATE_ACTIVE: u8 = 1;
const STATE_DRAINING: u8 = 2;
// Bound the healthy release-to-acquire polling gap well below the one-second
// command continuity budget. This remains coarse relative to game ticks and
// adds only five small coordination passes per task per second.
const CONTROL_TICK: Duration = Duration::from_millis(200);

async fn run_membership_heartbeat(
    store: MembershipStore,
    boot_id: BootIdentity,
    state: Arc<AtomicU8>,
    metadata: Arc<TaskMetadata>,
    lifecycle: LocalTaskLifecycle,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut interval =
        tokio::time::interval(crate::cluster_membership::DEFAULT_MEMBERSHIP_HEARTBEAT);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            _ = interval.tick() => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let current_state = state.load(Ordering::Acquire);
                let member_lifecycle = if current_state == STATE_DRAINING {
                    ClusterTaskLifecycle::Draining
                } else if lifecycle.is_assignment_eligible() {
                    ClusterTaskLifecycle::Active
                } else {
                    // Stop local reconciliation/acquisition before publishing
                    // the demotion. A healthy Redis makes the membership
                    // document follow on this same heartbeat; an unavailable
                    // Redis cannot grant this task a fenced lease anyway.
                    state.store(STATE_WARMING, Ordering::Release);
                    ClusterTaskLifecycle::Warming
                };
                let member = TaskMembership::new(
                    boot_id.clone(),
                    metadata.server_id,
                    metadata.ecs_task_id.clone(),
                    metadata.task_definition.clone(),
                    member_lifecycle,
                    now_ms,
                    store.ttl(),
                );
                let operation = store.heartbeat(&member);
                tokio::pin!(operation);
                let first_result = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Ok(()),
                    result = &mut operation => Some(result),
                    _ = tokio::time::sleep(DEFAULT_COORDINATION_OPERATION_TIMEOUT) => None,
                };
                let result = match first_result {
                    Some(result) => result,
                    None => {
                        // Mark readiness false at the bounded deadline, but do
                        // not repeatedly cancel ConnectionManager recovery.
                        // The same in-flight operation is allowed to reconnect
                        // and completes after Valkey returns.
                        lifecycle.mark_membership_ready(false);
                        if state.load(Ordering::Acquire) != STATE_DRAINING {
                            state.store(STATE_WARMING, Ordering::Release);
                        }
                        warn!("Membership heartbeat timed out; awaiting local reconnect");
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => return Ok(()),
                            result = &mut operation => result,
                        }
                    }
                };
                match result {
                    Ok(()) => {
                        if state.load(Ordering::Acquire) != STATE_DRAINING {
                            state.store(
                                if member_lifecycle == ClusterTaskLifecycle::Active {
                                    STATE_ACTIVE
                                } else {
                                    STATE_WARMING
                                },
                                Ordering::Release,
                            );
                        }
                        lifecycle.mark_membership_ready(
                            member_lifecycle == ClusterTaskLifecycle::Active
                                && state.load(Ordering::Acquire) != STATE_DRAINING,
                        );
                    }
                    Err(error) => {
                        lifecycle.mark_membership_ready(false);
                        if state.load(Ordering::Acquire) != STATE_DRAINING {
                            state.store(STATE_WARMING, Ordering::Release);
                        }
                        warn!(%error, "Membership heartbeat failed; retrying locally");
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ExecutorClusterHandle {
    state: Arc<AtomicU8>,
    boot_id: BootIdentity,
    membership: MembershipStore,
    coordinator: CoordinatorLeaseStore,
    coordinator_token: Arc<Mutex<Option<LeaseToken>>>,
    metadata: Arc<TaskMetadata>,
    executors: Arc<Mutex<HashMap<u32, PartitionExecutorV2Handle>>>,
    cancellation: CancellationToken,
}

impl ExecutorClusterHandle {
    pub fn boot_id(&self) -> &BootIdentity {
        &self.boot_id
    }

    /// Marks the end of process bootstrap. Assignment eligibility itself is
    /// published by the heartbeat only after every local readiness predicate
    /// converges, and is demoted again whenever one drops.
    pub async fn activate(&self) -> Result<()> {
        // TaskLifecycle::activate is called immediately before this method;
        // the next heartbeat observes it. Keeping one steady-state writer
        // avoids an activation heartbeat racing a readiness demotion.
        Ok(())
    }

    /// Announces DRAINING immediately, then handoffs all currently owned
    /// partitions in parallel under one caller-supplied deadline.
    pub async fn drain(&self, deadline: tokio::time::Instant) -> Result<()> {
        self.state.store(STATE_DRAINING, Ordering::Release);
        if let Err(error) = self.publish_membership().await {
            warn!(%error, "Could not publish draining membership; TTL expiry remains authoritative");
        }
        // Serialize with any in-flight reconciliation. Once this lock is
        // acquired, the DRAINING state prevents another assignment pass; the
        // exact-token compare-delete then lets a survivor coordinate
        // immediately instead of waiting for the three-second TTL.
        if let Some(token) = self.coordinator_token.lock().await.take()
            && let Err(error) = self.coordinator.release(&token).await
        {
            warn!(%error, "Could not release coordinator lease; TTL expiry remains authoritative");
        }
        let handles: Vec<_> = self.executors.lock().await.values().cloned().collect();
        let handoff = async {
            let mut tasks = Vec::with_capacity(handles.len());
            for handle in handles {
                tasks.push(tokio::spawn(async move { handle.handoff().await }));
            }
            for task in tasks {
                task.await.context("partition handoff task panicked")??;
            }
            Result::<()>::Ok(())
        };
        let handoff_result = tokio::time::timeout_at(deadline, handoff)
            .await
            .context("executor cluster drain exceeded global deadline")
            .and_then(|result| result);
        // Cleanup is unconditional. A concurrent assignment watcher may have
        // completed the same cooperative handoff first, or one partition may
        // have fallen back to crash recovery. Neither case may leave this
        // task advertised or its cluster worker running.
        if let Err(error) = self.membership.remove(&self.boot_id).await {
            warn!(%error, "Could not remove draining membership; TTL expiry will remove it");
        }
        self.cancellation.cancel();
        handoff_result
    }

    async fn publish_membership(&self) -> Result<()> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let lifecycle = match self.state.load(Ordering::Acquire) {
            STATE_ACTIVE => ClusterTaskLifecycle::Active,
            STATE_DRAINING => ClusterTaskLifecycle::Draining,
            _ => ClusterTaskLifecycle::Warming,
        };
        self.membership
            .heartbeat(&TaskMembership::new(
                self.boot_id.clone(),
                self.metadata.server_id,
                self.metadata.ecs_task_id.clone(),
                self.metadata.task_definition.clone(),
                lifecycle,
                now_ms,
                self.membership.ttl(),
            ))
            .await
    }
}

struct TaskMetadata {
    server_id: u64,
    ecs_task_id: Option<String>,
    task_definition: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn start_executor_cluster(
    server_id: u64,
    boot_id: BootIdentity,
    namespace: ClusterNamespace,
    redis: RedisConnection,
    bus: Arc<GameBus>,
    db: Arc<dyn Database>,
    local_lifecycle: LocalTaskLifecycle,
    ecs_task_id: Option<String>,
    task_definition: Option<String>,
    cancellation: CancellationToken,
) -> Result<(ExecutorClusterHandle, JoinHandle<Result<()>>)> {
    let membership = MembershipStore::new(
        redis.clone(),
        namespace.clone(),
        crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
    )?;
    let assignment = AssignmentStore::new(redis.clone(), namespace.clone());
    let coordinator = CoordinatorLeaseStore::new(
        redis.clone(),
        namespace.clone(),
        DEFAULT_PARTITION_LEASE_TTL,
        DEFAULT_COORDINATION_OPERATION_TIMEOUT,
    )?;
    let leases = PartitionLeaseStore::new(
        redis,
        namespace.clone(),
        DEFAULT_PARTITION_LEASE_TTL,
        DEFAULT_COORDINATION_OPERATION_TIMEOUT,
    )?;
    let state = Arc::new(AtomicU8::new(STATE_WARMING));
    let executors = Arc::new(Mutex::new(HashMap::new()));
    let coordinator_token = Arc::new(Mutex::new(None));
    let metadata = Arc::new(TaskMetadata {
        server_id,
        ecs_task_id,
        task_definition,
    });
    let handle = ExecutorClusterHandle {
        state: state.clone(),
        boot_id: boot_id.clone(),
        membership: membership.clone(),
        coordinator: coordinator.clone(),
        coordinator_token: coordinator_token.clone(),
        metadata: metadata.clone(),
        executors: executors.clone(),
        cancellation: cancellation.clone(),
    };
    let task = tokio::spawn(async move {
        run_executor_cluster(
            server_id,
            boot_id,
            namespace,
            membership,
            assignment,
            coordinator,
            leases,
            bus,
            db,
            local_lifecycle,
            metadata,
            state,
            executors,
            coordinator_token,
            cancellation,
        )
        .await
    });
    Ok((handle, task))
}

#[allow(clippy::too_many_arguments)]
async fn run_executor_cluster(
    server_id: u64,
    boot_id: BootIdentity,
    namespace: ClusterNamespace,
    membership: MembershipStore,
    assignment: AssignmentStore,
    coordinator: CoordinatorLeaseStore,
    leases: PartitionLeaseStore,
    bus: Arc<GameBus>,
    db: Arc<dyn Database>,
    local_lifecycle: LocalTaskLifecycle,
    metadata: Arc<TaskMetadata>,
    state: Arc<AtomicU8>,
    executors: Arc<Mutex<HashMap<u32, PartitionExecutorV2Handle>>>,
    coordinator_token: Arc<Mutex<Option<LeaseToken>>>,
    cancellation: CancellationToken,
) -> Result<()> {
    local_lifecycle.mark_membership_ready(false);

    let heartbeat_cancel = cancellation.child_token();
    let heartbeat_store = membership.clone();
    let heartbeat_boot = boot_id.clone();
    let heartbeat_state = state.clone();
    let heartbeat_metadata = metadata.clone();
    let heartbeat_lifecycle = local_lifecycle.clone();
    let mut heartbeat = tokio::spawn(run_membership_heartbeat(
        heartbeat_store,
        heartbeat_boot,
        heartbeat_state,
        heartbeat_metadata,
        heartbeat_lifecycle,
        heartbeat_cancel,
    ));

    let mut partition_tasks: HashMap<u32, JoinHandle<Result<()>>> = HashMap::new();
    let mut tick = tokio::time::interval(CONTROL_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut command_groups_ready = false;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            result = &mut heartbeat => {
                if cancellation.is_cancelled() {
                    break;
                }
                local_lifecycle.mark_membership_ready(false);
                return Err(match result {
                    Ok(Ok(())) => anyhow::anyhow!("membership heartbeat worker exited unexpectedly"),
                    Ok(Err(error)) => error.context("membership heartbeat worker failed"),
                    Err(error) => anyhow::Error::new(error).context("membership heartbeat worker panicked"),
                });
            }
            _ = tick.tick() => {
                let control_result: Result<()> = async {
                    if state.load(Ordering::Acquire) == STATE_ACTIVE
                        && !local_lifecycle.is_assignment_eligible()
                    {
                        state.store(STATE_WARMING, Ordering::Release);
                    }
                    // Consumer groups are required runtime state. Establish
                    // all of them before readiness, retrying the complete set
                    // after a partial bootstrap failure. Once established,
                    // do not spend every control tick reissuing ten XGROUP
                    // commands from every task.
                    if !command_groups_ready {
                        for partition in 0..PARTITION_COUNT {
                            bus.ensure_executor_command_group(&namespace, partition).await?;
                        }
                        command_groups_ready = true;
                    }
                    // Observe the assignment document even while WARMING.
                    // This is the assignment-watcher readiness proof and
                    // breaks the otherwise circular dependency where ACTIVE
                    // was required before the watcher could become ready.
                    let mut document = assignment.load().await?;

                    if state.load(Ordering::Acquire) == STATE_ACTIVE {
                        let mut coordinator_token = coordinator_token.lock().await;
                        // Drain may have started while this tick waited for
                        // the shared token. Recheck under the same lock that
                        // drain uses before acquiring a fresh term or writing
                        // an assignment.
                        if state.load(Ordering::Acquire) == STATE_ACTIVE {
                            reconcile_assignment(
                                &boot_id,
                                &membership,
                                &assignment,
                                &coordinator,
                                &mut coordinator_token,
                            )
                            .await?;
                            // Reconciliation may have replaced the document.
                            document = assignment.load().await?;
                        }
                    }

                    if state.load(Ordering::Acquire) == STATE_ACTIVE {
                        for partition in 0..PARTITION_COUNT {
                            if partition_tasks.contains_key(&partition) { continue; }
                            let Some(document) = document.as_ref() else { continue; };
                            if document.desired_owner(partition) != Some(&boot_id) { continue; }
                            let Some(guard) = leases.try_acquire(partition, &boot_id).await? else { continue; };
                            // Drain can begin while the bounded acquire is in
                            // flight. Recheck before publishing a handle so no
                            // new authority can escape drain's handle snapshot.
                            let mut executor_handles = executors.lock().await;
                            if state.load(Ordering::Acquire) != STATE_ACTIVE
                                || !local_lifecycle.is_assignment_eligible()
                            {
                                state.store(STATE_WARMING, Ordering::Release);
                                drop(executor_handles);
                                let _ = leases.release(&guard).await;
                                continue;
                            }
                            let (executor, task) = spawn_game_executor_v2(
                                server_id,
                                guard,
                                leases.clone(),
                                bus.clone(),
                                db.clone(),
                                RecoveryConfig::from_env()?,
                                cancellation.child_token(),
                            );
                            executor_handles.insert(partition, executor);
                            drop(executor_handles);
                            partition_tasks.insert(partition, task);
                            info!(partition, %boot_id, "started v2 partition executor");
                        }
                    }

                    let finished: Vec<u32> = partition_tasks
                        .iter()
                        .filter(|(_, task)| task.is_finished())
                        .map(|(partition, _)| *partition)
                        .collect();
                    for partition in finished {
                        let task = partition_tasks.remove(&partition).expect("task exists");
                        executors.lock().await.remove(&partition);
                        match task.await.context("v2 partition executor panicked")? {
                            Ok(()) => {}
                            Err(error) if is_coordination_unavailable(&error)
                                || is_normal_authority_exit(&error) => {
                                warn!(partition, %error, "Partition executor stopped fail-closed; retrying locally");
                            }
                            Err(error) => return Err(error.context(format!(
                                "partition {partition} executor invariant failure"
                            ))),
                        }
                    }
                    Ok(())
                }
                .await;
                match control_result {
                    Ok(()) => local_lifecycle.mark_assignment_ready(true),
                    Err(error) if is_coordination_unavailable(&error) => {
                        local_lifecycle.mark_assignment_ready(false);
                        warn!(%error, "Executor coordination unavailable; retrying locally");
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }

    let remaining: Vec<_> = executors
        .lock()
        .await
        .drain()
        .map(|(_, handle)| handle)
        .collect();
    for handle in remaining {
        let _ = handle.handoff().await;
    }
    if let Err(error) = membership.remove(&boot_id).await {
        warn!(%error, "Membership removal failed during shutdown; TTL expiry will clean it up");
    }
    heartbeat.abort();
    Ok(())
}

async fn reconcile_assignment(
    boot_id: &BootIdentity,
    membership: &MembershipStore,
    assignment: &AssignmentStore,
    coordinator: &CoordinatorLeaseStore,
    token: &mut Option<LeaseToken>,
) -> Result<()> {
    if let Some(current) = token
        && !coordinator.renew(current).await?
    {
        *token = None;
    }
    if token.is_none() {
        *token = coordinator.try_acquire(boot_id).await?;
    }
    let Some(token) = token else {
        return Ok(());
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let members = membership.list_live(now_ms).await?;
    assignment
        .reconcile(&token.encode(), PARTITION_COUNT, &members, now_ms)
        .await?;
    Ok(())
}

fn is_coordination_unavailable(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<redis::RedisError>().is_some()
            || cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
    })
}

fn is_normal_authority_exit(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("lease authority was lost")
        || message.contains("partition lease was lost")
        || message.contains("stale partition lease rejected")
        || message.contains("partition handoff")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_assignment::{
        ASSIGNMENT_SCHEMA_VERSION, AssignmentDocument, AssignmentWrite,
    };
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use tokio::io;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;

    #[test]
    fn cooperative_handoff_lease_loss_is_a_normal_authority_exit() {
        for message in [
            "partition lease was lost during cooperative barrier",
            "partition lease was lost before cooperative release",
        ] {
            assert!(is_normal_authority_exit(&anyhow::anyhow!(message)));
        }
    }

    #[test]
    fn autonomous_actor_redis_failure_restarts_only_its_partition() {
        let source = redis::RedisError::from((
            redis::ErrorKind::IoError,
            "injected autonomous actor Redis failure",
        ));
        let failure =
            crate::game_executor_v2::autonomous_actor_failure(2, 12, anyhow::Error::new(source));
        assert!(is_coordination_unavailable(&failure));
    }

    #[tokio::test]
    async fn autonomous_actor_timeout_restarts_only_its_partition() {
        let source = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("pending operation should time out");
        let failure =
            crate::game_executor_v2::autonomous_actor_failure(6, 16, anyhow::Error::new(source));
        assert!(is_coordination_unavailable(&failure));
    }

    #[test]
    fn autonomous_actor_invariant_failure_remains_task_fatal() {
        let failure = crate::game_executor_v2::autonomous_actor_failure(
            8,
            18,
            anyhow::anyhow!("invalid recovery sequence invariant"),
        );
        assert!(!is_coordination_unavailable(&failure));
        assert!(!is_normal_authority_exit(&failure));
    }

    struct RedisFaultProxy {
        address: SocketAddr,
        available: watch::Sender<bool>,
        cancellation: CancellationToken,
        task: JoinHandle<()>,
    }

    impl RedisFaultProxy {
        async fn start(upstream: SocketAddr) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let (available, availability) = watch::channel(true);
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = tokio::spawn(async move {
                loop {
                    let (mut inbound, _) = tokio::select! {
                        _ = task_cancellation.cancelled() => return,
                        accepted = listener.accept() => match accepted {
                            Ok(accepted) => accepted,
                            Err(_) => return,
                        },
                    };
                    let mut connection_availability = availability.clone();
                    tokio::spawn(async move {
                        if !*connection_availability.borrow() {
                            return;
                        }
                        let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                            return;
                        };
                        let outage = async {
                            loop {
                                if connection_availability.changed().await.is_err()
                                    || !*connection_availability.borrow()
                                {
                                    return;
                                }
                            }
                        };
                        tokio::select! {
                            _ = io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                            _ = outage => {}
                        }
                    });
                }
            });
            Ok(Self {
                address,
                available,
                cancellation,
                task,
            })
        }

        fn set_available(&self, available: bool) {
            self.available.send_replace(available);
        }

        async fn stop(self) {
            self.cancellation.cancel();
            let _ = self.task.await;
        }
    }

    async fn wait_until(deadline: Duration, mut condition: impl FnMut() -> bool) -> Result<()> {
        tokio::time::timeout(deadline, async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("condition did not become true before deadline")?;
        Ok(())
    }

    #[tokio::test]
    async fn owner_change_emits_assignment_moved_without_waiting_for_lease_expiry() -> Result<()> {
        use crate::game_executor_v2::{LeaseWatchdogEvent, spawn_lease_watchdog};
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("owner-change-{salt}"))?;
        let owner_a = BootIdentity::new();
        let owner_b = BootIdentity::new();
        let mut assignment = AssignmentDocument {
            schema_version: ASSIGNMENT_SCHEMA_VERSION,
            version: 1,
            region: namespace.region().to_owned(),
            computed_at_ms: chrono::Utc::now().timestamp_millis(),
            eligible_members: vec![owner_a.clone(), owner_b.clone()],
            owners: BTreeMap::from([(0, owner_a.clone())]),
        };
        let mut setup = manager.clone();
        let _: () = setup
            .set(
                namespace.partition_assignment(0),
                serde_json::to_vec(&assignment)?,
            )
            .await?;
        let leases = PartitionLeaseStore::new(
            manager.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;
        let guard = leases
            .try_acquire(0, &owner_a)
            .await?
            .context("initial owner did not acquire its assigned lease")?;
        let stop = CancellationToken::new();
        let handoff_requested = CancellationToken::new();
        let (watchdog, mut events) = spawn_lease_watchdog(
            leases.clone(),
            guard.clone(),
            stop.clone(),
            handoff_requested.clone(),
        );

        assignment.version += 1;
        assignment.computed_at_ms += 1;
        assignment.owners.insert(0, owner_b);
        let _: () = setup
            .set(
                namespace.partition_assignment(0),
                serde_json::to_vec(&assignment)?,
            )
            .await?;

        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .context("owner change did not wake the lease watchdog")?;
        assert!(matches!(event, Some(LeaseWatchdogEvent::AssignmentMoved)));
        assert!(handoff_requested.is_cancelled());
        assert!(
            leases.validate(&guard).await?,
            "owner change must request cooperative handoff while the exact incumbent lease remains fenced"
        );

        stop.cancel();
        watchdog.await?;
        let _ = leases.release(&guard).await?;
        let _: () = setup
            .del(&[
                namespace.partition_assignment(0),
                namespace.partition_lease(0),
            ])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn draining_coordinator_releases_exact_term_before_ttl() -> Result<()> {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("coordinator-drain-{salt}"))?;
        let membership = MembershipStore::new(
            manager.clone(),
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        )?;
        let coordinator = CoordinatorLeaseStore::new(
            manager.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;
        let owner_a = BootIdentity::new();
        let owner_b = BootIdentity::new();
        let token_a = coordinator
            .try_acquire(&owner_a)
            .await?
            .context("initial coordinator lease was not acquired")?;
        let cancellation = CancellationToken::new();
        let handle = ExecutorClusterHandle {
            state: Arc::new(AtomicU8::new(STATE_ACTIVE)),
            boot_id: owner_a.clone(),
            membership,
            coordinator: coordinator.clone(),
            coordinator_token: Arc::new(Mutex::new(Some(token_a.clone()))),
            metadata: Arc::new(TaskMetadata {
                server_id: 1,
                ecs_task_id: None,
                task_definition: Some("test:2".into()),
            }),
            executors: Arc::new(Mutex::new(HashMap::new())),
            cancellation,
        };

        let started = tokio::time::Instant::now();
        handle
            .drain(tokio::time::Instant::now() + Duration::from_secs(2))
            .await?;
        let token_b = tokio::time::timeout(
            Duration::from_millis(500),
            coordinator.try_acquire(&owner_b),
        )
        .await
        .context("successor waited for coordinator TTL during planned drain")??
        .context("successor did not acquire released coordinator term")?;
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            !coordinator.release(&token_a).await?,
            "stale drain deleted the successor coordinator term"
        );
        assert!(coordinator.renew(&token_b).await?);

        let _ = coordinator.release(&token_b).await?;
        let mut cleanup = manager;
        let _: () = cleanup
            .del(&[
                namespace.assignment_lease(),
                namespace.members(),
                namespace.member(&owner_a),
            ])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn crashed_owner_expires_and_survivor_takes_over_without_cleanup() -> Result<()> {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("crash-takeover-{salt}"))?;
        let membership = MembershipStore::new(
            manager.clone(),
            namespace.clone(),
            Duration::from_millis(500),
        )?;
        let assignment = AssignmentStore::new(manager.clone(), namespace.clone());
        let coordinator = CoordinatorLeaseStore::new(
            manager.clone(),
            namespace.clone(),
            Duration::from_millis(600),
            Duration::from_millis(100),
        )?;
        let leases = PartitionLeaseStore::new(
            manager.clone(),
            namespace.clone(),
            Duration::from_secs(1),
            Duration::from_millis(100),
        )?;
        let owner_a = BootIdentity::new();
        let owner_b = BootIdentity::new();
        let active_member = |boot_id: BootIdentity, now_ms: i64| {
            TaskMembership::new(
                boot_id,
                1,
                None,
                Some("test:2".into()),
                ClusterTaskLifecycle::Active,
                now_ms,
                membership.ttl(),
            )
        };
        let started_ms = chrono::Utc::now().timestamp_millis();
        membership
            .heartbeat(&active_member(owner_a.clone(), started_ms))
            .await?;
        membership
            .heartbeat(&active_member(owner_b.clone(), started_ms))
            .await?;
        let token_a = coordinator
            .try_acquire(&owner_a)
            .await?
            .context("initial coordinator did not acquire")?;
        let members = membership.list_live(started_ms).await?;
        let (_, initial) = assignment
            .reconcile(&token_a.encode(), PARTITION_COUNT, &members, started_ms)
            .await?;
        let partition = initial
            .owners
            .iter()
            .find_map(|(partition, owner)| (owner == &owner_a).then_some(*partition))
            .context("initial assignment gave A no partition")?;
        let guard_a = leases
            .try_acquire(partition, &owner_a)
            .await?
            .context("initial owner did not acquire its partition")?;

        // A now crashes: no DRAINING write, lease release, membership removal,
        // or coordinator release occurs. Keep only B's membership alive.
        tokio::time::sleep(Duration::from_millis(350)).await;
        let refreshed_ms = chrono::Utc::now().timestamp_millis();
        membership
            .heartbeat(&active_member(owner_b.clone(), refreshed_ms))
            .await?;
        tokio::time::sleep(Duration::from_millis(350)).await;
        let takeover_ms = chrono::Utc::now().timestamp_millis();
        membership
            .heartbeat(&active_member(owner_b.clone(), takeover_ms))
            .await?;
        let token_b = coordinator
            .try_acquire(&owner_b)
            .await?
            .context("survivor waited for crashed coordinator cleanup")?;
        let members = membership.list_live(takeover_ms).await?;
        assert_eq!(
            members
                .iter()
                .map(|member| member.boot_id.clone())
                .collect::<Vec<_>>(),
            vec![owner_b.clone()]
        );
        let (_, reassigned) = assignment
            .reconcile(&token_b.encode(), PARTITION_COUNT, &members, takeover_ms)
            .await?;
        assert!(reassigned.owners.values().all(|owner| owner == &owner_b));
        assert!(
            leases.try_acquire(partition, &owner_b).await?.is_none(),
            "successor bypassed the crashed owner's still-live exact token"
        );

        tokio::time::sleep(Duration::from_millis(400)).await;
        let guard_b = leases
            .try_acquire(partition, &owner_b)
            .await?
            .context("survivor did not acquire after crashed lease expiry")?;
        assert_ne!(guard_a.encoded_token(), guard_b.encoded_token());
        assert!(!leases.validate(&guard_a).await?);
        assert!(leases.validate(&guard_b).await?);

        let mut cleanup = manager;
        let _: () = cleanup
            .del(&[
                namespace.members(),
                namespace.member(&owner_a),
                namespace.member(&owner_b),
                namespace.assignment(),
                namespace.assignment_lease(),
                namespace.partition_lease(partition),
            ])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn successful_warming_heartbeat_never_sets_http_readiness() -> Result<()> {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("warming-{salt}"))?;
        let store = MembershipStore::new(
            manager,
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        )?;
        let lifecycle = LocalTaskLifecycle::new(format!("warming-{salt}"));
        lifecycle.mark_listener_bound();
        lifecycle.mark_replicas_ready(false);
        lifecycle.mark_assignment_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        let boot_id = BootIdentity::new();
        let state = Arc::new(AtomicU8::new(STATE_WARMING));
        let cancellation = CancellationToken::new();
        let worker = tokio::spawn(run_membership_heartbeat(
            store.clone(),
            boot_id.clone(),
            state.clone(),
            Arc::new(TaskMetadata {
                server_id: 1,
                ecs_task_id: None,
                task_definition: Some("test:2".into()),
            }),
            lifecycle.clone(),
            cancellation.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let members = store
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                if members.iter().any(|member| {
                    member.boot_id == boot_id && member.lifecycle == ClusterTaskLifecycle::Warming
                }) {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("WARMING membership was not published")??;
        assert_eq!(state.load(Ordering::Acquire), STATE_WARMING);
        assert!(
            !lifecycle.is_ready(),
            "a successful WARMING heartbeat must not satisfy readiness"
        );

        lifecycle.mark_replicas_ready(true);
        tokio::time::timeout(Duration::from_secs(3), async {
            while !lifecycle.is_ready() {
                lifecycle.mark_redis_success_now();
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("ACTIVE heartbeat did not restore readiness")?;
        assert_eq!(state.load(Ordering::Acquire), STATE_ACTIVE);

        cancellation.cancel();
        worker.await??;
        let direct = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let mut direct = direct.get_multiplexed_async_connection().await?;
        let _: () = direct
            .del(&[namespace.members(), namespace.member(&boot_id)])
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn membership_outage_at_start_and_runtime_recovers_without_restart() -> Result<()> {
        use redis::AsyncCommands;

        let proxy = RedisFaultProxy::start("127.0.0.1:6379".parse()?).await?;
        let client = redis::Client::open(format!("redis://{}/1?protocol=resp3", proxy.address))?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = redis::aio::ConnectionManager::new_with_config(
            client.clone(),
            redis::aio::ConnectionManagerConfig::new()
                .set_push_sender(pubsub_tx)
                .set_automatic_resubscription()
                .set_connection_timeout(Duration::from_millis(250))
                .set_response_timeout(Duration::from_millis(250))
                .set_number_of_retries(50)
                .set_factor(10)
                .set_max_delay(100),
        )
        .await?;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("outage-{salt}"))?;
        let store = MembershipStore::new(
            manager,
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        )?;
        let lifecycle = LocalTaskLifecycle::new(format!("outage-{salt}"));
        lifecycle.mark_listener_bound();
        lifecycle.mark_replicas_ready(true);
        lifecycle.mark_assignment_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        let boot_id = BootIdentity::new();
        let cancellation = CancellationToken::new();
        let worker_state = Arc::new(AtomicU8::new(STATE_WARMING));

        // The connection manager was established during process bootstrap,
        // then Valkey became unavailable before this critical worker began.
        // This is the production startup boundary under test: the listener is
        // live, readiness is false, and the worker must remain available to
        // reconnect instead of making GameServer::start fail after 30s.
        proxy.set_available(false);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let worker = tokio::spawn(run_membership_heartbeat(
            store.clone(),
            boot_id.clone(),
            worker_state.clone(),
            Arc::new(TaskMetadata {
                server_id: 1,
                ecs_task_id: None,
                task_definition: Some("test:2".into()),
            }),
            lifecycle.clone(),
            cancellation.clone(),
        ));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!lifecycle.is_ready());
        assert!(lifecycle.is_live());
        assert!(
            !worker.is_finished(),
            "startup coordination outage killed heartbeat worker"
        );

        proxy.set_available(true);
        let probe_client =
            redis::Client::open(format!("redis://{}/1?protocol=resp3", proxy.address))?;
        let mut probe = tokio::time::timeout(
            Duration::from_secs(2),
            probe_client.get_multiplexed_async_connection(),
        )
        .await
        .context("restored proxy did not accept a new Redis connection")??;
        let pong: String = redis::cmd("PING").query_async(&mut probe).await?;
        assert_eq!(pong, "PONG");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                lifecycle.mark_redis_success_now();
                if lifecycle.is_ready() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("membership readiness did not recover after Valkey restoration")?;
        assert!(
            !worker.is_finished(),
            "startup restoration required a worker restart"
        );
        assert_eq!(worker_state.load(Ordering::Acquire), STATE_ACTIVE);
        let active = store
            .list_live(chrono::Utc::now().timestamp_millis())
            .await?
            .into_iter()
            .find(|member| member.boot_id == boot_id)
            .context("restored worker did not publish membership")?;
        assert_eq!(active.lifecycle, ClusterTaskLifecycle::Active);

        // A local readiness predicate dropping must remove assignment
        // eligibility even though Redis itself remains healthy.
        lifecycle.mark_replicas_ready(false);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let members = store
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                if members.iter().any(|member| {
                    member.boot_id == boot_id && member.lifecycle == ClusterTaskLifecycle::Warming
                }) {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("readiness drop did not demote membership")??;
        assert_eq!(worker_state.load(Ordering::Acquire), STATE_WARMING);
        assert!(!lifecycle.is_ready());

        lifecycle.mark_replicas_ready(true);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                lifecycle.mark_redis_success_now();
                let members = store
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                if members.iter().any(|member| {
                    member.boot_id == boot_id && member.lifecycle == ClusterTaskLifecycle::Active
                }) && lifecycle.is_ready()
                {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("restored readiness did not reactivate membership")??;
        assert_eq!(worker_state.load(Ordering::Acquire), STATE_ACTIVE);

        // Exercise the same path after the worker has become healthy. This
        // guards both startup convergence and an outage during steady state.
        proxy.set_available(false);
        wait_until(Duration::from_secs(3), || !lifecycle.is_ready()).await?;
        assert!(lifecycle.is_live());
        assert!(
            !worker.is_finished(),
            "runtime coordination outage killed heartbeat worker"
        );
        proxy.set_available(true);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                lifecycle.mark_redis_success_now();
                if lifecycle.is_ready() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("membership readiness did not recover after runtime outage")?;
        assert!(
            !worker.is_finished(),
            "runtime restoration required a worker restart"
        );

        cancellation.cancel();
        worker.await??;
        proxy.stop().await;
        let direct = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let mut direct = direct.get_multiplexed_async_connection().await?;
        let _: () = direct
            .del(&[namespace.members(), namespace.member(&boot_id)])
            .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_valkey_outage_recovers_one_fenced_authority_across_two_tasks() -> Result<()> {
        use redis::AsyncCommands;

        let proxy = RedisFaultProxy::start("127.0.0.1:6379".parse()?).await?;
        let manager_config = || {
            redis::aio::ConnectionManagerConfig::new()
                .set_connection_timeout(Duration::from_millis(250))
                .set_response_timeout(Duration::from_millis(250))
                .set_number_of_retries(50)
                .set_factor(10)
                .set_max_delay(100)
        };
        let client_a = redis::Client::open(format!("redis://{}/1?protocol=resp3", proxy.address))?;
        let client_b = redis::Client::open(format!("redis://{}/1?protocol=resp3", proxy.address))?;
        let manager_a =
            redis::aio::ConnectionManager::new_with_config(client_a, manager_config()).await?;
        let manager_b =
            redis::aio::ConnectionManager::new_with_config(client_b, manager_config()).await?;

        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("shared-outage-{salt}"))?;
        let membership_a = MembershipStore::new(
            manager_a.clone(),
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        )?;
        let membership_b = MembershipStore::new(
            manager_b.clone(),
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        )?;
        let assignment_a = AssignmentStore::new(manager_a.clone(), namespace.clone());
        let assignment_b = AssignmentStore::new(manager_b.clone(), namespace.clone());
        let coordinator_a = CoordinatorLeaseStore::new(
            manager_a.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;
        let coordinator_b = CoordinatorLeaseStore::new(
            manager_b.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;
        let leases_a = PartitionLeaseStore::new(
            manager_a.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;
        let leases_b = PartitionLeaseStore::new(
            manager_b.clone(),
            namespace.clone(),
            DEFAULT_PARTITION_LEASE_TTL,
            DEFAULT_COORDINATION_OPERATION_TIMEOUT,
        )?;

        let boot_a = BootIdentity::new();
        let boot_b = BootIdentity::new();
        let lifecycle_a = LocalTaskLifecycle::new(format!("shared-outage-a-{salt}"));
        let lifecycle_b = LocalTaskLifecycle::new(format!("shared-outage-b-{salt}"));
        for lifecycle in [&lifecycle_a, &lifecycle_b] {
            lifecycle.mark_listener_bound();
            lifecycle.mark_replicas_ready(true);
            lifecycle.mark_assignment_ready(true);
            lifecycle.mark_redis_success_now();
            lifecycle.activate();
        }
        let state_a = Arc::new(AtomicU8::new(STATE_WARMING));
        let state_b = Arc::new(AtomicU8::new(STATE_WARMING));
        let cancellation_a = CancellationToken::new();
        let cancellation_b = CancellationToken::new();
        let worker_a = tokio::spawn(run_membership_heartbeat(
            membership_a.clone(),
            boot_a.clone(),
            state_a.clone(),
            Arc::new(TaskMetadata {
                server_id: 1,
                ecs_task_id: Some("task-a".into()),
                task_definition: Some("test:2".into()),
            }),
            lifecycle_a.clone(),
            cancellation_a.clone(),
        ));

        // Publish A alone first so recovery must advance the assignment from
        // version 1 once B becomes eligible, rather than taking the unchanged
        // assignment fast path.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                lifecycle_a.mark_redis_success_now();
                let members = membership_a
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                if lifecycle_a.is_ready()
                    && members.iter().any(|member| {
                        member.boot_id == boot_a && member.lifecycle == ClusterTaskLifecycle::Active
                    })
                {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("task A did not publish initial ACTIVE membership")??;
        let stale_coordinator = coordinator_a
            .try_acquire(&boot_a)
            .await?
            .context("task A did not acquire initial coordinator term")?;
        let initial_members = membership_a
            .list_live(chrono::Utc::now().timestamp_millis())
            .await?;
        let (initial_write, initial_assignment) = assignment_a
            .reconcile(
                &stale_coordinator.encode(),
                PARTITION_COUNT,
                &initial_members,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        assert_eq!(initial_write, AssignmentWrite::Written);
        assert_eq!(initial_assignment.version, 1);
        assert!(
            initial_assignment
                .owners
                .values()
                .all(|owner| owner == &boot_a)
        );
        let stale_partition = leases_a
            .try_acquire(0, &boot_a)
            .await?
            .context("task A did not acquire its initial partition term")?;

        let worker_b = tokio::spawn(run_membership_heartbeat(
            membership_b.clone(),
            boot_b.clone(),
            state_b.clone(),
            Arc::new(TaskMetadata {
                server_id: 2,
                ecs_task_id: Some("task-b".into()),
                task_definition: Some("test:2".into()),
            }),
            lifecycle_b.clone(),
            cancellation_b.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                lifecycle_a.mark_redis_success_now();
                lifecycle_b.mark_redis_success_now();
                let members = membership_a
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                let active: std::collections::BTreeSet<_> = members
                    .iter()
                    .filter(|member| member.lifecycle == ClusterTaskLifecycle::Active)
                    .map(|member| member.boot_id.clone())
                    .collect();
                if lifecycle_a.is_ready()
                    && lifecycle_b.is_ready()
                    && active == std::collections::BTreeSet::from([boot_a.clone(), boot_b.clone()])
                {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("both tasks did not become ACTIVE before the outage")??;
        assert!(coordinator_a.renew(&stale_coordinator).await?);
        assert!(leases_a.renew(&stale_partition).await?);
        lifecycle_a.mark_redis_success_now();
        lifecycle_b.mark_redis_success_now();

        let outage_started = tokio::time::Instant::now();
        proxy.set_available(false);
        wait_until(Duration::from_secs(7), || {
            !lifecycle_a.is_ready() && !lifecycle_b.is_ready()
        })
        .await
        .context("both tasks did not leave readiness during shared Valkey outage")?;
        assert!(lifecycle_a.is_live());
        assert!(lifecycle_b.is_live());
        assert_eq!(state_a.load(Ordering::Acquire), STATE_WARMING);
        assert_eq!(state_b.load(Ordering::Acquire), STATE_WARMING);
        assert!(!worker_a.is_finished());
        assert!(!worker_b.is_finished());

        // Valkey itself keeps running behind the failed network path, so both
        // exact authority TTLs must expire before connectivity is restored.
        tokio::time::sleep_until(
            outage_started + DEFAULT_PARTITION_LEASE_TTL + Duration::from_millis(250),
        )
        .await;
        proxy.set_available(true);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut redis_a = manager_a.clone();
                let mut redis_b = manager_b.clone();
                let (pong_a, pong_b) = tokio::join!(
                    async { redis::cmd("PING").query_async::<String>(&mut redis_a).await },
                    async { redis::cmd("PING").query_async::<String>(&mut redis_b).await },
                );
                if pong_a.as_deref() == Ok("PONG") {
                    lifecycle_a.mark_redis_success_now();
                }
                if pong_b.as_deref() == Ok("PONG") {
                    lifecycle_b.mark_redis_success_now();
                }
                let members = membership_a
                    .list_live(chrono::Utc::now().timestamp_millis())
                    .await?;
                let active: std::collections::BTreeSet<_> = members
                    .iter()
                    .filter(|member| member.lifecycle == ClusterTaskLifecycle::Active)
                    .map(|member| member.boot_id.clone())
                    .collect();
                if lifecycle_a.is_ready()
                    && lifecycle_b.is_ready()
                    && active == std::collections::BTreeSet::from([boot_a.clone(), boot_b.clone()])
                {
                    return Result::<()>::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("both tasks did not recover readiness after Valkey restoration")??;
        assert_eq!(state_a.load(Ordering::Acquire), STATE_ACTIVE);
        assert_eq!(state_b.load(Ordering::Acquire), STATE_ACTIVE);

        let (candidate_a, candidate_b) = tokio::join!(
            coordinator_a.try_acquire(&boot_a),
            coordinator_b.try_acquire(&boot_b),
        );
        let candidate_a = candidate_a?;
        let candidate_b = candidate_b?;
        assert_eq!(
            usize::from(candidate_a.is_some()) + usize::from(candidate_b.is_some()),
            1,
            "restored tasks acquired competing coordinator terms"
        );
        let (current_coordinator, current_assignment) = if let Some(token) = candidate_a {
            (token, assignment_a.clone())
        } else {
            (
                candidate_b.context("neither task acquired the coordinator term")?,
                assignment_b.clone(),
            )
        };
        assert_ne!(current_coordinator, stale_coordinator);
        assert!(coordinator_a.renew(&current_coordinator).await?);
        let restored_members = membership_a
            .list_live(chrono::Utc::now().timestamp_millis())
            .await?;
        let (restored_write, restored_assignment) = current_assignment
            .reconcile(
                &current_coordinator.encode(),
                PARTITION_COUNT,
                &restored_members,
                chrono::Utc::now().timestamp_millis(),
            )
            .await?;
        assert_eq!(restored_write, AssignmentWrite::Written);
        assert_eq!(restored_assignment.version, 2);
        restored_assignment.validate(PARTITION_COUNT)?;

        let desired_owner = restored_assignment
            .desired_owner(0)
            .context("restored assignment omitted partition 0")?;
        let (partition_a, partition_b) = tokio::join!(
            leases_a.try_acquire(0, &boot_a),
            leases_b.try_acquire(0, &boot_b),
        );
        let partition_a = partition_a?;
        let partition_b = partition_b?;
        assert_eq!(
            usize::from(partition_a.is_some()) + usize::from(partition_b.is_some()),
            1,
            "restored tasks acquired competing partition terms"
        );
        let (current_partition, current_leases) = if desired_owner == &boot_a {
            (
                partition_a.context("assigned task A did not acquire partition 0")?,
                leases_a.clone(),
            )
        } else {
            assert_eq!(desired_owner, &boot_b);
            (
                partition_b.context("assigned task B did not acquire partition 0")?,
                leases_b.clone(),
            )
        };
        assert_ne!(
            current_partition.encoded_token(),
            stale_partition.encoded_token()
        );

        let mut inspector = manager_a.clone();
        let stored_coordinator: Option<String> =
            inspector.get(namespace.assignment_lease()).await?;
        assert_eq!(stored_coordinator, Some(current_coordinator.encode()));
        let coordinator_ttl_ms: i64 = redis::cmd("PTTL")
            .arg(namespace.assignment_lease())
            .query_async(&mut inspector)
            .await?;
        assert!(coordinator_ttl_ms > 0);
        let stored_partition: Option<String> = inspector.get(namespace.partition_lease(0)).await?;
        assert_eq!(stored_partition, Some(current_partition.encoded_token()));
        let partition_ttl_ms: i64 = redis::cmd("PTTL")
            .arg(namespace.partition_lease(0))
            .query_async(&mut inspector)
            .await?;
        assert!(partition_ttl_ms > 0);
        assert_eq!(
            assignment_b
                .load()
                .await?
                .context("restored assignment disappeared")?,
            restored_assignment
        );

        let mut stale_candidate = restored_assignment.clone();
        stale_candidate.version += 1;
        stale_candidate.computed_at_ms += 1;
        assert_eq!(
            assignment_a
                .compare_and_set(
                    &stale_coordinator.encode(),
                    Some(restored_assignment.version),
                    &stale_candidate,
                )
                .await?,
            AssignmentWrite::CoordinatorLeaseLost,
            "expired coordinator term committed a new assignment version"
        );
        assert!(
            !leases_a.renew(&stale_partition).await?,
            "expired partition term renewed over its successor"
        );
        assert!(
            !leases_a.release(&stale_partition).await?,
            "expired partition term deleted its successor"
        );
        assert!(current_leases.validate(&current_partition).await?);
        assert!(
            coordinator_a.renew(&current_coordinator).await?,
            "current coordinator term was displaced by stale authority"
        );

        cancellation_a.cancel();
        cancellation_b.cancel();
        worker_a.await??;
        worker_b.await??;
        let _ = current_leases.release(&current_partition).await?;
        let _ = coordinator_a.release(&current_coordinator).await?;
        let _: () = inspector
            .del(&[
                namespace.members(),
                namespace.member(&boot_a),
                namespace.member(&boot_b),
                namespace.assignment(),
                namespace.assignment_lease(),
                namespace.partition_lease(0),
            ])
            .await?;
        proxy.stop().await;
        Ok(())
    }
}
