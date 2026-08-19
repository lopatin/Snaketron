//! Public surface for injecting supervised background work into a Snaketron
//! game server.
//!
//! The game crate provides the *mechanism* — a supervised, optionally
//! exclusive, gracefully-shut-down slot for long-running work. The operator
//! provides the *policy* by registering implementations of [`HostedServiceFactory`].
//!
//! This crate deliberately depends on nothing from the game crate, so an
//! operator's dependency graph never acquires `RedisConnection`, `DynamoDb`,
//! or any other host internal. Host capabilities reach a service through the
//! narrow object-safe traits in [`deps`].
//!
//! # Exclusion
//!
//! See `snaketron/specs/hosted-services.md` §2. The short version, because
//! getting it wrong produces silent duplicate side effects:
//!
//! - No [`ExclusionKey`] means N instances run, by design.
//! - An `ExclusionKey` alone means *usually* one. Overlap remains possible
//!   across a GC pause, a store blip, or clock skew, so the service must be
//!   idempotent.
//! - Exclusion becomes *effective* only when [`LeaseHandle::epoch`] is threaded
//!   into a conditional write that the downstream resource can reject.

pub mod deps;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

pub use deps::{KeyValueStore, LifecycleView};

/// Failure returned by a hosted service or its factory.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// A dependency could not be resolved at build time. Fails at boot rather
    /// than at 3am.
    #[error("missing dependency: {0}")]
    MissingDependency(String),
    /// Configuration was absent or unparseable.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// Anything else. Subject to the service's [`FailurePolicy`].
    #[error("{0}")]
    Failed(String),
}

impl ServiceError {
    pub fn failed(message: impl fmt::Display) -> Self {
        Self::Failed(message.to_string())
    }
}

/// Where an [`ExclusionKey`] is unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExclusionDomain {
    /// Unique within one region. Backed by that region's key-value store.
    Region,
    /// Unique across every region. Backed by the cross-region store, because
    /// the regional stores do not span regions.
    Global,
}

/// The key by which instances of a service must be mutually exclusive.
///
/// This subsumes what would otherwise be a scope enum: no key means run
/// everywhere, a `Region` key means one per (region, key), and a `Global` key
/// means one per key across all regions. It also allows *partitioned*
/// exclusion — keying by a work unit lets distinct units run concurrently
/// without contending.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExclusionKey {
    pub domain: ExclusionDomain,
    pub key: String,
}

impl ExclusionKey {
    pub fn region(key: impl Into<String>) -> Self {
        Self {
            domain: ExclusionDomain::Region,
            key: key.into(),
        }
    }

    pub fn global(key: impl Into<String>) -> Self {
        Self {
            domain: ExclusionDomain::Global,
            key: key.into(),
        }
    }
}

impl fmt::Display for ExclusionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let domain = match self.domain {
            ExclusionDomain::Region => "region",
            ExclusionDomain::Global => "global",
        };
        write!(f, "{domain}:{}", self.key)
    }
}

/// What the supervisor does when a service returns an error, panics, or exits
/// before cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Rebuild with jittered exponential backoff. After `max_consecutive`
    /// failures the service is disabled — the host is never taken down.
    Restart { max_consecutive: u32 },
    /// Log and leave it stopped.
    Disable,
    /// Escalate to host shutdown. Almost nothing should use this: a game server
    /// must keep serving players when auxiliary work is broken.
    FailHost,
}

impl Default for FailurePolicy {
    fn default() -> Self {
        Self::Restart {
            max_consecutive: 10,
        }
    }
}

/// A held exclusion lease.
///
/// `epoch` is the fencing token. It increases monotonically across
/// *acquisitions* of a key (renewals do not bump it), so a stale holder always
/// carries a lower epoch than its successor. That ordering is what lets a
/// downstream resource reject a stale write; the identity `holder` string
/// cannot, because it is random rather than ordered.
#[derive(Debug, Clone)]
pub struct LeaseHandle {
    key: ExclusionKey,
    epoch: u64,
    holder: String,
    rank: u32,
    last_renewed_at: Arc<Mutex<Instant>>,
    ttl: Duration,
}

impl LeaseHandle {
    pub fn new(key: ExclusionKey, epoch: u64, holder: String, rank: u32, ttl: Duration) -> Self {
        Self {
            key,
            epoch,
            holder,
            rank,
            last_renewed_at: Arc::new(Mutex::new(Instant::now())),
            ttl,
        }
    }

    pub fn key(&self) -> &ExclusionKey {
        &self.key
    }

    /// The fencing token. Thread this into every conditional write that the
    /// downstream resource can reject (spec HS-3).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Opaque identity of this holder. Useful only for effects checked
    /// atomically inside the store that issued the lease.
    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Preference rank of the holder. Lower is more preferred; a node may
    /// preempt only a strictly higher rank.
    pub fn rank(&self) -> u32 {
        self.rank
    }

    /// Records a confirmed renewal. Called by the supervisor, not by services.
    pub fn mark_renewed(&self) {
        *self.last_renewed_at.lock().expect("lease clock poisoned") = Instant::now();
    }

    pub fn last_renewed_at(&self) -> Instant {
        *self.last_renewed_at.lock().expect("lease clock poisoned")
    }

    /// Whether the lease is still safely held, given the time a subsequent
    /// operation may take.
    ///
    /// Deliberately conservative: it measures from the last *confirmed*
    /// renewal, never from wall-clock optimism, so a stalled renewal reads as
    /// "not held" rather than "probably fine".
    pub fn is_held_for(&self, operation_timeout: Duration) -> bool {
        self.last_renewed_at().elapsed() + operation_timeout < self.ttl
    }
}

/// How preferred this node is as a leader for excluded services.
///
/// Lower is more preferred. Deployment policy, not game policy: the game crate
/// has no opinion about which region should lead, so the default is a single
/// flat rank (first-come-first-served) and the operator orders its regions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct NodeRank(pub u32);

impl NodeRank {
    /// Whether a node at this rank may take a lease from `holder`.
    ///
    /// Strictly better only. Equal ranks must NOT preempt, or two equally
    /// preferred nodes would take turns evicting each other forever.
    pub fn may_preempt(self, holder: NodeRank) -> bool {
        self.0 < holder.0
    }
}

/// Identity of the task running a service.
#[derive(Debug, Clone)]
pub struct TaskIdentity {
    /// Registry id assigned by the control plane.
    pub server_id: i32,
    /// Unique per process start.
    pub boot_id: String,
    /// `{server_id}:{boot_id}`, matching the host's log field.
    pub task_boot_id: String,
}

/// Deployment environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment(pub String);

/// Logical region, e.g. `use1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionId(pub String);

impl fmt::Display for RegionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Environment-derived configuration, already namespaced to the service.
#[derive(Debug, Clone, Default)]
pub struct ServiceConfig {
    values: HashMap<String, String>,
}

impl ServiceConfig {
    pub fn from_map(values: HashMap<String, String>) -> Self {
        Self { values }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// Parses a value, returning [`ServiceError::InvalidConfig`] rather than a
    /// silent default, so a typo surfaces at boot.
    pub fn parse<T>(&self, key: &str, default: T) -> Result<T, ServiceError>
    where
        T: std::str::FromStr,
        T::Err: fmt::Display,
    {
        match self.values.get(key) {
            None => Ok(default),
            Some(raw) => raw
                .parse::<T>()
                .map_err(|error| ServiceError::InvalidConfig(format!("{key}: {error}"))),
        }
    }
}

/// Everything a service is given at build time.
#[derive(Clone)]
pub struct ServiceContext {
    pub environment: Environment,
    pub region: RegionId,
    pub aws_region: String,
    pub identity: TaskIdentity,
    pub kv: Arc<dyn KeyValueStore>,
    pub lifecycle: Arc<dyn LifecycleView>,
    pub config: ServiceConfig,
    /// Present exactly when the factory returned an [`ExclusionKey`].
    pub lease: Option<LeaseHandle>,
    ready: Arc<Mutex<bool>>,
}

impl ServiceContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        environment: Environment,
        region: RegionId,
        aws_region: String,
        identity: TaskIdentity,
        kv: Arc<dyn KeyValueStore>,
        lifecycle: Arc<dyn LifecycleView>,
        config: ServiceConfig,
        lease: Option<LeaseHandle>,
    ) -> Self {
        Self {
            environment,
            region,
            aws_region,
            identity,
            kv,
            lifecycle,
            config,
            lease,
            ready: Arc::new(Mutex::new(false)),
        }
    }

    /// Signals that this service has finished starting.
    ///
    /// Informational only: it deliberately does not gate the task's own
    /// readiness, because auxiliary work must never keep a game server out of
    /// the load balancer.
    pub fn mark_ready(&self) {
        *self.ready.lock().expect("readiness flag poisoned") = true;
    }

    pub fn is_ready(&self) -> bool {
        *self.ready.lock().expect("readiness flag poisoned")
    }

    /// Resolves when the task begins draining — the flush window, which opens
    /// before the hard cancellation.
    pub async fn on_drain(&self) {
        self.lifecycle.on_drain().await;
    }
}

/// Long-running work supervised by the host.
#[async_trait]
pub trait HostedService: Send + 'static {
    /// Runs until `cancel` fires, then returns promptly.
    ///
    /// Returning `Ok(())` before cancellation counts as an unexpected exit and
    /// is subject to the [`FailurePolicy`]; a service that has genuinely
    /// finished should park on `cancel.cancelled()`.
    async fn run(&mut self, cancel: CancellationToken) -> Result<(), ServiceError>;
}

/// Builds [`HostedService`] instances and declares how they are scheduled.
#[async_trait]
pub trait HostedServiceFactory: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// The key by which instances must be mutually exclusive, or `None` for no
    /// exclusion. Computed once per instance, so it may depend on region,
    /// environment, or configuration.
    ///
    /// Per-work-item locking is deliberately out of scope: that is a
    /// distributed lock manager, not a service scope.
    fn exclusion_key(&self, ctx: &ServiceContext) -> Option<ExclusionKey> {
        let _ = ctx;
        None
    }

    fn failure_policy(&self) -> FailurePolicy {
        FailurePolicy::default()
    }

    /// Builds a fresh instance. Called once per start and once per restart, so
    /// a service may hold non-reusable state without an internal reset path.
    async fn build(&self, ctx: ServiceContext) -> Result<Box<dyn HostedService>, ServiceError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(ttl_ms: u64) -> LeaseHandle {
        LeaseHandle::new(
            ExclusionKey::global("committer/game_events"),
            7,
            "boot:acq".to_owned(),
            0,
            Duration::from_millis(ttl_ms),
        )
    }

    #[test]
    fn an_exclusion_key_renders_its_domain() {
        assert_eq!(ExclusionKey::region("a").to_string(), "region:a");
        assert_eq!(ExclusionKey::global("a").to_string(), "global:a");
        assert_ne!(ExclusionKey::region("a"), ExclusionKey::global("a"));
    }

    /// A freshly renewed lease is held; one that has not renewed within its TTL
    /// is not. The operation timeout is subtracted so the caller never starts
    /// work that could outlive the lease.
    #[test]
    fn lease_validity_is_measured_from_the_last_confirmed_renewal() {
        let lease = handle(3_000);
        assert!(lease.is_held_for(Duration::from_millis(750)));
        // An operation longer than the whole TTL can never be safe.
        assert!(!lease.is_held_for(Duration::from_millis(3_000)));
    }

    #[test]
    fn renewal_refreshes_the_validity_window() {
        let lease = handle(3_000);
        let before = lease.last_renewed_at();
        std::thread::sleep(Duration::from_millis(5));
        lease.mark_renewed();
        assert!(lease.last_renewed_at() > before);
    }

    /// The epoch is the fencing token; the holder string is only an identity.
    /// Conflating them is the mistake the spec exists to prevent.
    #[test]
    fn the_epoch_is_exposed_for_fencing() {
        assert_eq!(handle(3_000).epoch(), 7);
        assert_eq!(handle(3_000).holder(), "boot:acq");
    }

    #[test]
    fn config_parses_or_reports_the_offending_key() {
        let config = ServiceConfig::from_map(
            [
                ("BATCH".to_owned(), "42".to_owned()),
                ("BAD".to_owned(), "x".to_owned()),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(config.parse::<u32>("BATCH", 1).unwrap(), 42);
        assert_eq!(config.parse::<u32>("ABSENT", 9).unwrap(), 9);
        let error = config.parse::<u32>("BAD", 1).unwrap_err();
        assert!(matches!(error, ServiceError::InvalidConfig(ref m) if m.contains("BAD")));
    }

    #[test]
    fn the_default_failure_policy_restarts_rather_than_failing_the_host() {
        assert_eq!(
            FailurePolicy::default(),
            FailurePolicy::Restart {
                max_consecutive: 10
            }
        );
    }
}
