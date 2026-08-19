//! Narrow, object-safe capability traits.
//!
//! Host capabilities reach a service through these rather than through the
//! host's concrete types, so an operator's dependency graph never acquires
//! `RedisConnection`, `DynamoDb`, or anything else internal. Each trait is
//! deliberately the smallest useful surface: a service that only needs
//! get/set should not be handed a full Redis client.

use std::time::Duration;

use async_trait::async_trait;

use crate::ServiceError;

/// Outcome of a conditional store write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasOutcome {
    /// The write was applied.
    Applied,
    /// The precondition did not hold. **Not an error** — for a fenced writer
    /// this means "someone else holds the lease now", and the correct response
    /// is to stop, not to retry.
    Rejected,
}

/// A minimal key-value store with the conditional operations a lease needs.
#[async_trait]
pub trait KeyValueStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<String>, ServiceError>;

    /// Takes a lease, honouring node preference.
    ///
    /// Succeeds when the lease is free, expired, or held by a **strictly
    /// less-preferred** node (a higher `rank`). Equal ranks never preempt each
    /// other, which is what stops two equally-preferred nodes from trading the
    /// lease back and forth — and what makes "two US nodes are never both
    /// leaders" hold rather than merely being likely.
    ///
    /// Preemption is safe only because every acquisition bumps the fencing
    /// epoch: the displaced holder's next conditional write is rejected, and
    /// its renewal fails, so it stands down.
    async fn try_acquire_lease(
        &self,
        key: &str,
        holder: &str,
        rank: u32,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError>;

    /// Extends the expiry only if the stored value still equals `expected`.
    async fn extend_if_equal(
        &self,
        key: &str,
        expected: &str,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError>;

    /// Deletes only if the stored value still equals `expected`, so a delayed
    /// drain from an old holder cannot delete a successor's lease.
    async fn delete_if_equal(&self, key: &str, expected: &str) -> Result<CasOutcome, ServiceError>;

    /// Atomically increments a counter and returns the new value. This is the
    /// source of monotonic lease epochs.
    async fn increment(&self, key: &str) -> Result<u64, ServiceError>;
}

/// Read-only view of the host task's lifecycle.
#[async_trait]
pub trait LifecycleView: Send + Sync + 'static {
    /// Resolves when the task begins draining, before the hard cancellation.
    async fn on_drain(&self);

    /// Whether the task is currently draining.
    fn is_draining(&self) -> bool;
}
