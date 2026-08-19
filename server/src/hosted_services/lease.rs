//! Exclusion leases with monotonic fencing epochs.
//!
//! This differs from [`crate::partition_lease::CoordinatorLeaseStore`] in two
//! ways that the hosted-service spec requires (§6.1):
//!
//! 1. It is **parameterized by key**, rather than hardcoded to the assignment
//!    lease, so any number of independent exclusion domains can coexist.
//! 2. It issues a **monotonic epoch**. The existing `LeaseToken` is a random
//!    UUID, which answers "is this still my lease?" but not "is this lease
//!    newer?" — and only the latter lets a downstream resource reject a stale
//!    writer. Equality suffices solely when checked atomically with the effect,
//!    which an external resource cannot do.
//!
//! Only a *new acquisition* bumps the epoch; renewals do not, so a holder's
//! fencing token is stable for the life of its term.

use std::time::Duration;

use snaketron_service_api::deps::{CasOutcome, KeyValueStore};
use snaketron_service_api::{ExclusionKey, LeaseHandle, ServiceError};
use std::sync::Arc;
use uuid::Uuid;

/// Encodes the preference rank into the stored lease value.
///
/// The rank has to travel with the value so a contender can compare ranks
/// atomically inside the store, and the encoded form is what `LeaseHandle`
/// carries — renew and release compare against exactly what was stored, so a
/// mismatch here would silently break every renewal.
pub fn encode_holder(rank: u32, holder: &str) -> String {
    format!("{rank:010}|{holder}")
}

/// Acquires and renews exclusion leases against a [`KeyValueStore`].
///
/// The store determines the domain: a regional store yields region-unique
/// leases, a cross-region store yields globally-unique ones. The logic is
/// identical, which is why `ExclusionDomain` selects a store rather than a
/// code path.
#[derive(Clone)]
pub struct ExclusionLeaseStore {
    kv: Arc<dyn KeyValueStore>,
    prefix: String,
    ttl: Duration,
    operation_timeout: Duration,
    /// Preference of the node this store runs on. Lower is more preferred.
    rank: u32,
}

impl ExclusionLeaseStore {
    pub fn new(
        kv: Arc<dyn KeyValueStore>,
        prefix: impl Into<String>,
        ttl: Duration,
        operation_timeout: Duration,
        rank: u32,
    ) -> Result<Self, ServiceError> {
        if operation_timeout >= ttl {
            return Err(ServiceError::InvalidConfig(
                "exclusion lease operation timeout must be shorter than the TTL".to_owned(),
            ));
        }
        Ok(Self {
            kv,
            prefix: prefix.into(),
            ttl,
            operation_timeout,
            rank,
        })
    }

    pub fn rank(&self) -> u32 {
        self.rank
    }

    fn lease_key(&self, key: &ExclusionKey) -> String {
        format!("{}:lease:{}", self.prefix, key.key)
    }

    fn epoch_key(&self, key: &ExclusionKey) -> String {
        format!("{}:epoch:{}", self.prefix, key.key)
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Attempts to take the lease.
    ///
    /// The epoch is allocated *before* the lease is taken. Allocating after
    /// would let two contenders take the lease and then read epochs in the
    /// opposite order, inverting the fencing order — so an unused epoch on a
    /// lost race is the correct trade. Epochs are cheap and gaps are harmless;
    /// only monotonicity matters.
    pub async fn try_acquire(
        &self,
        key: &ExclusionKey,
        holder_id: &str,
    ) -> Result<Option<LeaseHandle>, ServiceError> {
        let epoch = self.kv.increment(&self.epoch_key(key)).await?;
        let holder = encode_holder(self.rank, &format!("{holder_id}:{}", Uuid::new_v4()));
        match self
            .kv
            .try_acquire_lease(&self.lease_key(key), &holder, self.rank, self.ttl)
            .await?
        {
            CasOutcome::Rejected => Ok(None),
            CasOutcome::Applied => Ok(Some(LeaseHandle::new(
                key.clone(),
                epoch,
                holder,
                self.rank,
                self.ttl,
            ))),
        }
    }

    /// Extends the term. Returns `false` when the lease is no longer ours,
    /// which the supervisor treats as loss of leadership rather than an error.
    pub async fn renew(&self, lease: &LeaseHandle) -> Result<bool, ServiceError> {
        let outcome = self
            .kv
            .extend_if_equal(&self.lease_key(lease.key()), lease.holder(), self.ttl)
            .await?;
        if outcome == CasOutcome::Applied {
            lease.mark_renewed();
        }
        Ok(outcome == CasOutcome::Applied)
    }

    /// Releases only our exact term, so a delayed drain cannot delete a
    /// successor's lease.
    pub async fn release(&self, lease: &LeaseHandle) -> Result<bool, ServiceError> {
        Ok(self
            .kv
            .delete_if_equal(&self.lease_key(lease.key()), lease.holder())
            .await?
            == CasOutcome::Applied)
    }
}
