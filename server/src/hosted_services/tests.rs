//! Supervisor and exclusion tests.
//!
//! The exclusion cases are the load-bearing ones: each guarantee in the spec's
//! §2.2 table gets its own test, because the difference between "usually one"
//! and "effectively one" is otherwise only documentation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use snaketron_service_api::deps::{CasOutcome, KeyValueStore, LifecycleView};
use snaketron_service_api::{
    Environment, ExclusionKey, FailurePolicy, HostedService, HostedServiceFactory, LeaseHandle,
    RegionId, ServiceConfig, ServiceContext, ServiceError, TaskIdentity,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::lease::ExclusionLeaseStore;
use super::supervisor::{SupervisorConfig, spawn_supervisor};

/// In-memory store with real compare-and-set semantics, including expiry, so
/// takeover after a holder stalls can be exercised without a live Valkey.
#[derive(Default)]
struct MemoryStore {
    values: Mutex<HashMap<String, (String, Option<tokio::time::Instant>)>>,
    counters: Mutex<HashMap<String, u64>>,
}

impl MemoryStore {
    async fn live(&self, key: &str) -> Option<String> {
        let mut values = self.values.lock().await;
        match values.get(key) {
            Some((_, Some(expiry))) if *expiry <= tokio::time::Instant::now() => {
                values.remove(key);
                None
            }
            Some((value, _)) => Some(value.clone()),
            None => None,
        }
    }
}

#[async_trait]
impl KeyValueStore for MemoryStore {
    async fn get(&self, key: &str) -> Result<Option<String>, ServiceError> {
        Ok(self.live(key).await)
    }

    async fn try_acquire_lease(
        &self,
        key: &str,
        holder: &str,
        rank: u32,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        // Mirrors the real stores: free, expired, or held by a strictly
        // less-preferred node.
        if let Some(current) = self.live(key).await {
            let holder_rank: u32 = current
                .split('|')
                .next()
                .and_then(|r| r.parse().ok())
                .unwrap_or(u32::MAX);
            if rank >= holder_rank {
                return Ok(CasOutcome::Rejected);
            }
        }
        self.values.lock().await.insert(
            key.to_owned(),
            (holder.to_owned(), Some(tokio::time::Instant::now() + ttl)),
        );
        Ok(CasOutcome::Applied)
    }

    async fn extend_if_equal(
        &self,
        key: &str,
        expected: &str,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        if self.live(key).await.as_deref() != Some(expected) {
            return Ok(CasOutcome::Rejected);
        }
        self.values.lock().await.insert(
            key.to_owned(),
            (expected.to_owned(), Some(tokio::time::Instant::now() + ttl)),
        );
        Ok(CasOutcome::Applied)
    }

    async fn delete_if_equal(&self, key: &str, expected: &str) -> Result<CasOutcome, ServiceError> {
        if self.live(key).await.as_deref() != Some(expected) {
            return Ok(CasOutcome::Rejected);
        }
        self.values.lock().await.remove(key);
        Ok(CasOutcome::Applied)
    }

    async fn increment(&self, key: &str) -> Result<u64, ServiceError> {
        let mut counters = self.counters.lock().await;
        let slot = counters.entry(key.to_owned()).or_insert(0);
        *slot += 1;
        Ok(*slot)
    }
}

struct NeverDrains;

#[async_trait]
impl LifecycleView for NeverDrains {
    async fn on_drain(&self) {
        std::future::pending::<()>().await;
    }
    fn is_draining(&self) -> bool {
        false
    }
}

fn context(kv: Arc<dyn KeyValueStore>) -> ServiceContext {
    ServiceContext::new(
        Environment("test".to_owned()),
        RegionId("use1".to_owned()),
        "us-east-1".to_owned(),
        TaskIdentity {
            server_id: 1,
            boot_id: "boot".to_owned(),
            task_boot_id: "1:boot".to_owned(),
        },
        kv,
        Arc::new(NeverDrains),
        ServiceConfig::default(),
        None,
    )
}

/// Records how many instances are concurrently inside `run`, and the peak.
#[derive(Default)]
struct Concurrency {
    current: AtomicU32,
    peak: AtomicU32,
    starts: AtomicU32,
    epochs: Mutex<Vec<u64>>,
}

struct CountingService {
    counters: Arc<Concurrency>,
}

#[async_trait]
impl HostedService for CountingService {
    async fn run(&mut self, cancel: CancellationToken) -> Result<(), ServiceError> {
        let now = self.counters.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.counters.peak.fetch_max(now, Ordering::SeqCst);
        cancel.cancelled().await;
        self.counters.current.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CountingFactory {
    counters: Arc<Concurrency>,
    key: Option<ExclusionKey>,
}

#[async_trait]
impl HostedServiceFactory for CountingFactory {
    fn name(&self) -> &'static str {
        "counting"
    }

    fn exclusion_key(&self, _ctx: &ServiceContext) -> Option<ExclusionKey> {
        self.key.clone()
    }

    async fn build(&self, ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        self.counters.starts.fetch_add(1, Ordering::SeqCst);
        if let Some(lease) = ctx.lease.as_ref() {
            self.counters.epochs.lock().await.push(lease.epoch());
        }
        Ok(Box::new(CountingService {
            counters: self.counters.clone(),
        }))
    }
}

fn supervisor_config(kv: Arc<dyn KeyValueStore>, holder: &str) -> SupervisorConfig {
    ranked_supervisor_config(kv, holder, 0)
}

fn ranked_supervisor_config(
    kv: Arc<dyn KeyValueStore>,
    holder: &str,
    rank: u32,
) -> SupervisorConfig {
    let store = ExclusionLeaseStore::new(
        kv.clone(),
        "test",
        Duration::from_millis(600),
        Duration::from_millis(100),
        rank,
    )
    .unwrap();
    SupervisorConfig {
        context: context(kv),
        region_leases: Some(store.clone()),
        global_leases: Some(store),
        holder_id: holder.to_owned(),
        stop_budget: Duration::from_secs(1),
    }
}

/// No key means N instances by design (spec HS-1).
#[tokio::test]
async fn without_an_exclusion_key_every_instance_runs() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let counters = Arc::new(Concurrency::default());
    let cancel = CancellationToken::new();

    let mut handles = Vec::new();
    for index in 0..3 {
        let factory = Arc::new(CountingFactory {
            counters: counters.clone(),
            key: None,
        });
        handles.push(spawn_supervisor(
            factory,
            supervisor_config(kv.clone(), &format!("task-{index}")),
            cancel.clone(),
        ));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        counters.peak.load(Ordering::SeqCst),
        3,
        "all three should run"
    );

    cancel.cancel();
    for handle in handles {
        handle.await.unwrap();
    }
}

/// A key means at most one runs in steady state.
#[tokio::test]
async fn an_exclusion_key_admits_only_one_instance() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let counters = Arc::new(Concurrency::default());
    let cancel = CancellationToken::new();
    let key = ExclusionKey::global("committer");

    let mut handles = Vec::new();
    for index in 0..4 {
        let factory = Arc::new(CountingFactory {
            counters: counters.clone(),
            key: Some(key.clone()),
        });
        handles.push(spawn_supervisor(
            factory,
            supervisor_config(kv.clone(), &format!("task-{index}")),
            cancel.clone(),
        ));
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counters.peak.load(Ordering::SeqCst),
        1,
        "exclusion must admit exactly one"
    );

    cancel.cancel();
    for handle in handles {
        handle.await.unwrap();
    }
}

/// Distinct keys must NOT contend — this is the whole point of a keyed
/// singleton rather than a single global one, and it is what lets the Iceberg
/// committer fold two tables concurrently.
#[tokio::test]
async fn distinct_keys_do_not_contend() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let counters = Arc::new(Concurrency::default());
    let cancel = CancellationToken::new();

    let mut handles = Vec::new();
    for table in ["game_events", "websocket_events"] {
        let factory = Arc::new(CountingFactory {
            counters: counters.clone(),
            key: Some(ExclusionKey::global(format!("committer/{table}"))),
        });
        handles.push(spawn_supervisor(
            factory,
            supervisor_config(kv.clone(), table),
            cancel.clone(),
        ));
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counters.peak.load(Ordering::SeqCst),
        2,
        "distinct keys must run concurrently"
    );

    cancel.cancel();
    for handle in handles {
        handle.await.unwrap();
    }
}

/// When the holder goes away, a contender takes over within the TTL.
#[tokio::test]
async fn a_contender_takes_over_after_the_holder_stops() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let counters = Arc::new(Concurrency::default());
    let key = ExclusionKey::region("solo");

    let first_cancel = CancellationToken::new();
    let first = spawn_supervisor(
        Arc::new(CountingFactory {
            counters: counters.clone(),
            key: Some(key.clone()),
        }),
        supervisor_config(kv.clone(), "first"),
        first_cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(counters.current.load(Ordering::SeqCst), 1);

    let second_cancel = CancellationToken::new();
    let second = spawn_supervisor(
        Arc::new(CountingFactory {
            counters: counters.clone(),
            key: Some(key),
        }),
        supervisor_config(kv.clone(), "second"),
        second_cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(counters.current.load(Ordering::SeqCst), 1, "still only one");

    first_cancel.cancel();
    first.await.unwrap();

    // The first releases on stop, so takeover is prompt rather than waiting
    // out the full TTL.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        counters.current.load(Ordering::SeqCst),
        1,
        "successor picked it up"
    );

    second_cancel.cancel();
    second.await.unwrap();
}

/// Epochs must strictly increase across acquisitions, because that ordering is
/// what lets a downstream resource reject a stale writer.
#[tokio::test]
async fn epochs_increase_monotonically_across_acquisitions() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let store = ExclusionLeaseStore::new(
        kv,
        "test",
        Duration::from_millis(500),
        Duration::from_millis(100),
        0,
    )
    .unwrap();
    let key = ExclusionKey::global("fenced");

    let mut seen = Vec::new();
    for holder in ["a", "b", "c"] {
        let lease = store
            .try_acquire(&key, holder)
            .await
            .unwrap()
            .expect("free");
        seen.push(lease.epoch());
        // Renewal must NOT bump the epoch: a holder's fencing token is stable
        // for the life of its term.
        assert!(store.renew(&lease).await.unwrap());
        assert_eq!(lease.epoch(), *seen.last().unwrap());
        store.release(&lease).await.unwrap();
    }
    assert!(
        seen.windows(2).all(|w| w[1] > w[0]),
        "epochs must increase: {seen:?}"
    );
}

/// A contended acquisition returns None rather than an error, and does not
/// hand out a lease.
#[tokio::test]
async fn a_contended_lease_is_not_granted_twice() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let store = ExclusionLeaseStore::new(
        kv,
        "test",
        Duration::from_secs(5),
        Duration::from_millis(100),
        0,
    )
    .unwrap();
    let key = ExclusionKey::global("solo");

    let first = store.try_acquire(&key, "a").await.unwrap();
    assert!(first.is_some());
    assert!(store.try_acquire(&key, "b").await.unwrap().is_none());

    // A stale holder cannot release a successor's lease.
    let stale = LeaseHandle::new(
        key.clone(),
        1,
        "a:stale".to_owned(),
        0,
        Duration::from_secs(5),
    );
    assert!(!store.release(&stale).await.unwrap());
    assert!(store.release(first.as_ref().unwrap()).await.unwrap());
    assert!(store.try_acquire(&key, "b").await.unwrap().is_some());
}

struct AlwaysFails {
    attempts: Arc<AtomicU64>,
}

#[async_trait]
impl HostedService for AlwaysFails {
    async fn run(&mut self, _cancel: CancellationToken) -> Result<(), ServiceError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(ServiceError::failed("boom"))
    }
}

struct FailingFactory {
    attempts: Arc<AtomicU64>,
    max_consecutive: u32,
}

#[async_trait]
impl HostedServiceFactory for FailingFactory {
    fn name(&self) -> &'static str {
        "failing"
    }
    fn failure_policy(&self) -> FailurePolicy {
        FailurePolicy::Restart {
            max_consecutive: self.max_consecutive,
        }
    }
    async fn build(&self, _ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        Ok(Box::new(AlwaysFails {
            attempts: self.attempts.clone(),
        }))
    }
}

/// A crashlooping service is retried, then disabled — and the host keeps
/// running throughout. That last part is the point.
#[tokio::test]
async fn a_failing_service_is_disabled_without_taking_down_the_host() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let attempts = Arc::new(AtomicU64::new(0));
    let cancel = CancellationToken::new();

    let handle = spawn_supervisor(
        Arc::new(FailingFactory {
            attempts: attempts.clone(),
            max_consecutive: 3,
        }),
        supervisor_config(kv, "task"),
        cancel.clone(),
    );

    // The supervisor returns on its own once the ceiling is reached; the host
    // token is never cancelled by it.
    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("supervisor should give up on its own")
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert!(
        !cancel.is_cancelled(),
        "the host must not be cancelled by a plugin failure"
    );
}

struct Sleeper;

#[async_trait]
impl HostedService for Sleeper {
    async fn run(&mut self, cancel: CancellationToken) -> Result<(), ServiceError> {
        cancel.cancelled().await;
        Ok(())
    }
}

struct SleeperFactory;

#[async_trait]
impl HostedServiceFactory for SleeperFactory {
    fn name(&self) -> &'static str {
        "sleeper"
    }
    async fn build(&self, _ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        Ok(Box::new(Sleeper))
    }
}

#[tokio::test]
async fn cancellation_stops_a_service_promptly() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let cancel = CancellationToken::new();
    let handle = spawn_supervisor(
        Arc::new(SleeperFactory),
        supervisor_config(kv, "task"),
        cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("must stop promptly")
        .unwrap();
}

// ---------------------------------------------------------------------------
// Node preference.
//
// Lower rank is more preferred. The load-bearing asymmetry: a strictly better
// rank preempts, an EQUAL rank never does. Equal-rank preemption would let two
// equally preferred nodes evict each other forever, and "two US nodes are never
// both leaders" would degrade from a guarantee into a probability.
// ---------------------------------------------------------------------------

fn ranked_store(kv: Arc<dyn KeyValueStore>, rank: u32) -> ExclusionLeaseStore {
    ExclusionLeaseStore::new(
        kv,
        "test",
        Duration::from_secs(30),
        Duration::from_millis(100),
        rank,
    )
    .unwrap()
}

/// The core requirement: a preferred node reclaims leadership from a
/// less-preferred holder.
#[tokio::test]
async fn a_preferred_node_preempts_a_less_preferred_holder() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");

    let eu = ranked_store(kv.clone(), 1);
    let us = ranked_store(kv.clone(), 0);

    let eu_lease = eu.try_acquire(&key, "eu-1").await.unwrap().expect("free");
    let us_lease = us
        .try_acquire(&key, "us-1")
        .await
        .unwrap()
        .expect("a better rank must preempt");

    assert!(
        us_lease.epoch() > eu_lease.epoch(),
        "preemption must fence the loser"
    );
    // The displaced holder discovers it no longer holds the lease, which is
    // what makes preemption safe rather than merely fast.
    assert!(
        !eu.renew(&eu_lease).await.unwrap(),
        "the displaced holder must fail renewal"
    );
}

/// The inverse must NOT happen: a less-preferred node cannot take the lease
/// from a preferred one.
#[tokio::test]
async fn a_less_preferred_node_cannot_preempt() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");

    let us = ranked_store(kv.clone(), 0);
    let eu = ranked_store(kv.clone(), 1);

    let us_lease = us.try_acquire(&key, "us-1").await.unwrap().expect("free");
    assert!(
        eu.try_acquire(&key, "eu-1").await.unwrap().is_none(),
        "a worse rank must never preempt"
    );
    assert!(
        us.renew(&us_lease).await.unwrap(),
        "the preferred holder keeps its lease"
    );
}

/// Two equally preferred nodes must never both hold it, and must not trade it
/// back and forth either.
#[tokio::test]
async fn two_equally_preferred_nodes_never_both_lead() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");

    let first = ranked_store(kv.clone(), 0);
    let second = ranked_store(kv.clone(), 0);

    let held = first
        .try_acquire(&key, "us-1")
        .await
        .unwrap()
        .expect("free");
    for _ in 0..5 {
        assert!(
            second.try_acquire(&key, "us-2").await.unwrap().is_none(),
            "an equal rank must never preempt"
        );
    }
    assert!(
        first.renew(&held).await.unwrap(),
        "the holder is undisturbed"
    );
}

/// When the preferred region is entirely absent, a less-preferred node leads —
/// availability is not sacrificed for preference.
#[tokio::test]
async fn a_less_preferred_node_leads_when_no_preferred_node_exists() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");
    let eu = ranked_store(kv, 1);
    assert!(eu.try_acquire(&key, "eu-1").await.unwrap().is_some());
}

/// Recovery: once a holder stops, a contender picks the lease up promptly
/// rather than waiting out the full TTL.
#[tokio::test]
async fn leadership_recovers_promptly_after_the_holder_releases() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");
    let first = ranked_store(kv.clone(), 0);
    let second = ranked_store(kv.clone(), 0);

    let held = first
        .try_acquire(&key, "us-1")
        .await
        .unwrap()
        .expect("free");
    assert!(second.try_acquire(&key, "us-2").await.unwrap().is_none());

    first.release(&held).await.unwrap();
    let successor = second
        .try_acquire(&key, "us-2")
        .await
        .unwrap()
        .expect("a released lease must be immediately available");
    assert!(
        successor.epoch() > held.epoch(),
        "the successor must fence its predecessor"
    );
}

/// Recovery without a clean release: the lease expires and is taken over. A
/// brief gap is acceptable; a permanent one is not.
#[tokio::test]
async fn leadership_recovers_after_an_unclean_holder_death() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");
    // A short TTL keeps the test fast while exercising the real expiry path.
    let store = ExclusionLeaseStore::new(
        kv.clone(),
        "test",
        Duration::from_millis(300),
        Duration::from_millis(100),
        0,
    )
    .unwrap();

    let dead = store
        .try_acquire(&key, "us-1")
        .await
        .unwrap()
        .expect("free");
    // The holder vanishes without releasing: no renewal, no delete.
    assert!(store.try_acquire(&key, "us-2").await.unwrap().is_none());

    tokio::time::sleep(Duration::from_millis(400)).await;
    let successor = store
        .try_acquire(&key, "us-2")
        .await
        .unwrap()
        .expect("an expired lease must be reclaimable");
    assert!(successor.epoch() > dead.epoch());
}

/// Preemption must bump the epoch, or the displaced holder's writes would
/// still be accepted downstream.
#[tokio::test]
async fn every_acquisition_including_preemption_advances_the_epoch() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("committer");
    let worst = ranked_store(kv.clone(), 2);
    let middle = ranked_store(kv.clone(), 1);
    let best = ranked_store(kv.clone(), 0);

    let a = worst.try_acquire(&key, "c").await.unwrap().unwrap();
    let b = middle.try_acquire(&key, "b").await.unwrap().unwrap();
    let c = best.try_acquire(&key, "a").await.unwrap().unwrap();

    assert!(a.epoch() < b.epoch() && b.epoch() < c.epoch());
    assert_eq!(c.rank(), 0);
}

#[test]
fn rank_ordering_is_strict() {
    use snaketron_service_api::NodeRank;
    assert!(NodeRank(0).may_preempt(NodeRank(1)));
    assert!(!NodeRank(1).may_preempt(NodeRank(0)));
    assert!(
        !NodeRank(0).may_preempt(NodeRank(0)),
        "equal ranks must not preempt"
    );
}

// ---------------------------------------------------------------------------
// Elected-task telemetry.
//
// Only one task in the fleet runs an elected singleton, so a fleet-average p99
// dilutes that task's contribution by the fleet size. These tests pin the
// signal that makes the per-elected-task comparison possible: it must track the
// lease itself, not the happy path. Each factory below uses its own `&'static`
// name because the leadership registry is process-global and this file shares a
// test binary with the rest of the crate's unit tests.
// ---------------------------------------------------------------------------

/// `None` means the task never contended for the service at all, which is a
/// different observation from `Some(0)`, "contended and lost".
fn leases_held(name: &'static str) -> Option<u64> {
    crate::otel_metrics::hosted_service_leases_held_for_test(name)
}

struct ElectedFactory {
    name: &'static str,
    key: ExclusionKey,
}

#[async_trait]
impl HostedServiceFactory for ElectedFactory {
    fn name(&self) -> &'static str {
        self.name
    }

    fn exclusion_key(&self, _ctx: &ServiceContext) -> Option<ExclusionKey> {
        Some(self.key.clone())
    }

    async fn build(&self, _ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        Ok(Box::new(Sleeper))
    }
}

/// Acquiring marks the task elected; a clean stop unmarks it.
#[tokio::test]
async fn leadership_telemetry_is_set_on_acquire_and_cleared_on_release() {
    const NAME: &str = "test-telemetry-elected";
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let cancel = CancellationToken::new();

    assert_eq!(leases_held(NAME), None, "nothing contended before the run");
    let handle = spawn_supervisor(
        Arc::new(ElectedFactory {
            name: NAME,
            key: ExclusionKey::global("telemetry-elected"),
        }),
        supervisor_config(kv, "task"),
        cancel.clone(),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        leases_held(NAME),
        Some(1),
        "the holder must report as elected"
    );

    cancel.cancel();
    handle.await.unwrap();
    assert_eq!(
        leases_held(NAME),
        Some(0),
        "a stopped holder is not elected"
    );
}

/// Losing the lease mid-run must clear the signal. A task that stood down but
/// kept reporting as elected would attribute the wrong latency series to the
/// pipeline for the rest of its life.
#[tokio::test]
async fn leadership_telemetry_clears_when_the_lease_is_lost() {
    const NAME: &str = "test-telemetry-preempted";
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("telemetry-preempted");
    let cancel = CancellationToken::new();

    let handle = spawn_supervisor(
        Arc::new(ElectedFactory {
            name: NAME,
            key: key.clone(),
        }),
        ranked_supervisor_config(kv.clone(), "holder", 1),
        cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(leases_held(NAME), Some(1));

    // A strictly preferred node takes the lease *without* a supervisor, so the
    // only thing that can clear the signal is the holder discovering that its
    // own renewal failed. Its worse rank also stops it from taking the lease
    // back, which keeps the assertion below deterministic.
    let preferred = ExclusionLeaseStore::new(
        kv,
        "test",
        Duration::from_secs(30),
        Duration::from_millis(100),
        0,
    )
    .unwrap();
    preferred
        .try_acquire(&key, "preferred")
        .await
        .unwrap()
        .expect("a better rank must preempt");

    // The renewer ticks at TTL/3 = 200 ms; the stand-down follows immediately.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        leases_held(NAME),
        Some(0),
        "a displaced holder must stop reporting as elected"
    );

    cancel.cancel();
    handle.await.unwrap();
}

struct FailedBuildFactory {
    attempts: Arc<AtomicU64>,
}

#[async_trait]
impl HostedServiceFactory for FailedBuildFactory {
    fn name(&self) -> &'static str {
        "test-telemetry-failed-build"
    }

    fn failure_policy(&self) -> FailurePolicy {
        FailurePolicy::Disable
    }

    fn exclusion_key(&self, _ctx: &ServiceContext) -> Option<ExclusionKey> {
        Some(ExclusionKey::global("telemetry-failed-build"))
    }

    async fn build(&self, _ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(ServiceError::failed("boom"))
    }
}

/// The discriminating case: `build` runs *after* the lease is taken, and its
/// failure returns early past the release at the bottom of `run_once`. Clearing
/// the signal only on the happy path would strand this task at "elected"
/// forever, on a task that is running nothing at all.
#[tokio::test]
async fn a_failed_build_does_not_strand_the_leadership_signal() {
    const NAME: &str = "test-telemetry-failed-build";
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let attempts = Arc::new(AtomicU64::new(0));
    let cancel = CancellationToken::new();

    let handle = spawn_supervisor(
        Arc::new(FailedBuildFactory {
            attempts: attempts.clone(),
        }),
        supervisor_config(kv, "task"),
        cancel.clone(),
    );
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("Disable ends the supervisor on the first failure")
        .unwrap();

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the lease must have been taken for the build to have been attempted"
    );
    assert_eq!(
        leases_held(NAME),
        Some(0),
        "an early return after acquisition must still clear leadership"
    );
    assert!(!cancel.is_cancelled());
}

/// The tasks that lose the election are the baseline the elected task is
/// compared against, so they have to report zero rather than nothing. Without
/// this a never-elected task would be indistinguishable from a task whose build
/// does not carry the service at all.
#[tokio::test]
async fn a_task_that_loses_the_election_still_reports_zero() {
    const NAME: &str = "test-telemetry-loser";
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let key = ExclusionKey::global("telemetry-loser");
    let cancel = CancellationToken::new();

    // Taken by a strictly preferred node first, so the supervisor below never
    // wins and the only value it can ever report is the contending zero.
    let preferred = ranked_store(kv.clone(), 0);
    preferred
        .try_acquire(&key, "preferred")
        .await
        .unwrap()
        .expect("free");

    let handle = spawn_supervisor(
        Arc::new(ElectedFactory { name: NAME, key }),
        ranked_supervisor_config(kv, "loser", 1),
        cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        leases_held(NAME),
        Some(0),
        "a contender must be observable at zero"
    );

    cancel.cancel();
    handle.await.unwrap();
}
