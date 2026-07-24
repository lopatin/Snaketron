use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::broadcast;

const PHASE_STARTING: u8 = 0;
const PHASE_ACTIVE: u8 = 1;
const PHASE_DRAINING: u8 = 2;
const PHASE_STOPPING: u8 = 3;

/// Capabilities advertised only after the server has actually authenticated a
/// WebSocket. Keep these stable: clients use them to decide whether a planned
/// make-before-break handoff is supported.
pub const WS_PROTOCOL_VERSION: u16 = 2;
pub const WS_BASE_CAPABILITIES: &[&str] = &[
    "explicit-auth-v1",
    "planned-drain-v1",
    "socket-generation-v1",
    "command-delivery-v2",
    "command-outcomes-v1",
    "command-outcome-barrier-v1",
];

/// A planned task-removal notification. The absolute deadline avoids clients
/// having to guess how much of a relative grace period was consumed in transit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DrainNotice {
    pub task_boot_id: String,
    pub deadline_unix_ms: i64,
}

/// Small, process-local lifecycle state shared by health handlers and critical
/// workers. This is deliberately not a general supervisor: task failure still
/// terminates the process; the bits only make readiness truthful.
#[derive(Clone)]
pub struct TaskLifecycle {
    inner: Arc<TaskLifecycleInner>,
}

struct TaskLifecycleInner {
    task_boot_id: String,
    phase: AtomicU8,
    listener_bound: AtomicBool,
    replicas_ready: AtomicBool,
    membership_ready: AtomicBool,
    assignment_ready: AtomicBool,
    critical_failure: AtomicBool,
    last_redis_success_ms: AtomicI64,
    redis_freshness_ms: AtomicI64,
    active_websockets: AtomicUsize,
    socket_generation: AtomicU64,
    drain_deadline_unix_ms: AtomicI64,
    drain_announced: AtomicBool,
    drain_tx: broadcast::Sender<DrainNotice>,
}

impl TaskLifecycle {
    pub fn new(task_boot_id: impl Into<String>) -> Self {
        let (drain_tx, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(TaskLifecycleInner {
                task_boot_id: task_boot_id.into(),
                phase: AtomicU8::new(PHASE_STARTING),
                listener_bound: AtomicBool::new(false),
                replicas_ready: AtomicBool::new(false),
                membership_ready: AtomicBool::new(false),
                assignment_ready: AtomicBool::new(false),
                critical_failure: AtomicBool::new(false),
                last_redis_success_ms: AtomicI64::new(0),
                redis_freshness_ms: AtomicI64::new(5_000),
                active_websockets: AtomicUsize::new(0),
                socket_generation: AtomicU64::new(0),
                drain_deadline_unix_ms: AtomicI64::new(0),
                drain_announced: AtomicBool::new(false),
                drain_tx,
            }),
        }
    }

    pub fn task_boot_id(&self) -> &str {
        &self.inner.task_boot_id
    }

    pub fn protocol_capabilities(&self) -> Vec<String> {
        WS_BASE_CAPABILITIES
            .iter()
            .map(|value| (*value).to_owned())
            .collect()
    }

    pub fn set_redis_freshness(&self, freshness: Duration) {
        self.inner.redis_freshness_ms.store(
            freshness.as_millis().min(i64::MAX as u128) as i64,
            Ordering::Release,
        );
    }

    pub fn mark_listener_bound(&self) {
        self.inner.listener_bound.store(true, Ordering::Release);
    }

    pub fn listener_is_bound(&self) -> bool {
        self.inner.listener_bound.load(Ordering::Acquire)
    }

    pub fn mark_replicas_ready(&self, ready: bool) {
        self.inner.replicas_ready.store(ready, Ordering::Release);
    }

    pub fn mark_membership_ready(&self, ready: bool) {
        self.inner.membership_ready.store(ready, Ordering::Release);
    }

    pub fn mark_assignment_ready(&self, ready: bool) {
        self.inner.assignment_ready.store(ready, Ordering::Release);
    }

    pub fn mark_redis_success_now(&self) {
        self.inner
            .last_redis_success_ms
            .store(now_unix_ms(), Ordering::Release);
    }

    pub fn mark_critical_failure(&self) {
        self.inner.critical_failure.store(true, Ordering::Release);
    }

    pub fn activate(&self) {
        let _ = self.inner.phase.compare_exchange(
            PHASE_STARTING,
            PHASE_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn begin_draining(&self, deadline_unix_ms: i64) -> DrainNotice {
        self.inner
            .drain_deadline_unix_ms
            .store(deadline_unix_ms, Ordering::Release);
        self.inner.phase.store(PHASE_DRAINING, Ordering::Release);
        DrainNotice {
            task_boot_id: self.task_boot_id().to_owned(),
            deadline_unix_ms,
        }
    }

    pub fn announce_drain(&self, notice: DrainNotice) {
        // Publish the replayable bit before the edge-triggered broadcast. A
        // connection that subscribes after this store can recover the notice
        // from `current_drain_notice`; one that subscribed before it receives
        // the broadcast, so the readiness-check/upgrade race cannot strand a
        // late socket on the departing task.
        self.inner.drain_announced.store(true, Ordering::Release);
        // No receivers is valid when a task has no connected clients.
        let _ = self.inner.drain_tx.send(notice);
    }

    pub fn current_drain_notice(&self) -> Option<DrainNotice> {
        if !self.inner.drain_announced.load(Ordering::Acquire) {
            return None;
        }
        let deadline_unix_ms = self.inner.drain_deadline_unix_ms.load(Ordering::Acquire);
        (deadline_unix_ms > 0).then(|| DrainNotice {
            task_boot_id: self.task_boot_id().to_owned(),
            deadline_unix_ms,
        })
    }

    pub fn begin_stopping(&self) {
        self.inner.phase.store(PHASE_STOPPING, Ordering::Release);
    }

    pub fn subscribe_to_drain(&self) -> broadcast::Receiver<DrainNotice> {
        self.inner.drain_tx.subscribe()
    }

    pub fn next_socket_generation(&self) -> u64 {
        self.inner.socket_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn websocket_opened(&self) -> usize {
        self.inner.active_websockets.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn websocket_closed(&self) -> usize {
        // A close callback must never wrap the counter if a future bug invokes
        // it twice. compare_exchange keeps the health metric conservative.
        let mut current = self.inner.active_websockets.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return 0;
            }
            match self.inner.active_websockets.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return current - 1,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn active_websockets(&self) -> usize {
        self.inner.active_websockets.load(Ordering::Acquire)
    }

    pub fn is_draining(&self) -> bool {
        self.inner.phase.load(Ordering::Acquire) >= PHASE_DRAINING
    }

    pub fn is_live(&self) -> bool {
        // Liveness means this process can answer HTTP, not that it is eligible
        // for traffic. It therefore remains true through STOPPING; once the
        // HTTP future exits there is no endpoint left to answer at all.
        true
    }

    /// Local prerequisites for publishing this task as executor-placement
    /// eligible. Membership freshness itself is deliberately excluded: the
    /// heartbeat publishes ACTIVE only after these predicates converge, then
    /// that successful write supplies the final readiness bit.
    pub fn is_assignment_eligible(&self) -> bool {
        if self.inner.phase.load(Ordering::Acquire) != PHASE_ACTIVE
            || !self.inner.listener_bound.load(Ordering::Acquire)
            || !self.inner.replicas_ready.load(Ordering::Acquire)
            || self.inner.critical_failure.load(Ordering::Acquire)
        {
            return false;
        }

        if !self.inner.assignment_ready.load(Ordering::Acquire) {
            return false;
        }

        let last_success = self.inner.last_redis_success_ms.load(Ordering::Acquire);
        let freshness = self.inner.redis_freshness_ms.load(Ordering::Acquire);
        last_success > 0 && now_unix_ms().saturating_sub(last_success) <= freshness
    }

    pub fn is_ready(&self) -> bool {
        if !self.is_assignment_eligible() {
            return false;
        }
        self.inner.membership_ready.load(Ordering::Acquire)
    }
}

fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::TaskLifecycle;
    use std::time::Duration;

    #[test]
    fn readiness_requires_every_local_dependency_and_fresh_redis() {
        let lifecycle = TaskLifecycle::new("boot-a");
        lifecycle.set_redis_freshness(Duration::from_secs(1));
        assert!(!lifecycle.is_ready());

        lifecycle.mark_listener_bound();
        lifecycle.mark_replicas_ready(true);
        lifecycle.mark_assignment_ready(true);
        lifecycle.mark_membership_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        assert!(lifecycle.is_ready());

        lifecycle.mark_replicas_ready(false);
        assert!(!lifecycle.is_ready());
    }

    #[test]
    fn readiness_always_requires_cluster_workers() {
        let lifecycle = TaskLifecycle::new("boot-a");
        lifecycle.mark_listener_bound();
        lifecycle.mark_replicas_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        assert!(!lifecycle.is_ready());
        lifecycle.mark_assignment_ready(true);
        assert!(!lifecycle.is_ready());
        lifecycle.mark_membership_ready(true);
        assert!(lifecycle.is_ready());
    }

    #[test]
    fn draining_is_immediately_unready_and_broadcasts_once() {
        let lifecycle = TaskLifecycle::new("boot-a");
        lifecycle.mark_listener_bound();
        lifecycle.mark_replicas_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        let mut drain_rx = lifecycle.subscribe_to_drain();

        let notice = lifecycle.begin_draining(1234);
        assert!(!lifecycle.is_ready());
        assert!(lifecycle.is_draining());
        assert_eq!(lifecycle.current_drain_notice(), None);
        lifecycle.announce_drain(notice.clone());
        assert_eq!(drain_rx.try_recv().unwrap(), notice);
        assert_eq!(lifecycle.current_drain_notice(), Some(notice));
    }

    #[test]
    fn stale_close_cannot_underflow_active_socket_count() {
        let lifecycle = TaskLifecycle::new("boot-a");
        assert_eq!(lifecycle.websocket_opened(), 1);
        assert_eq!(lifecycle.websocket_closed(), 0);
        assert_eq!(lifecycle.websocket_closed(), 0);
    }

    #[test]
    fn liveness_remains_true_through_stopping() {
        let lifecycle = TaskLifecycle::new("boot-a");
        lifecycle.begin_stopping();
        assert!(lifecycle.is_live());
        assert!(!lifecycle.is_ready());
    }

    #[test]
    fn current_command_capabilities_are_always_advertised() {
        let lifecycle = TaskLifecycle::new("boot-a");
        assert!(
            lifecycle
                .protocol_capabilities()
                .iter()
                .any(|value| value == "command-delivery-v2")
        );
        assert!(
            lifecycle
                .protocol_capabilities()
                .iter()
                .any(|value| value == "command-outcome-barrier-v1")
        );
    }
}
