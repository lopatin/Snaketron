//! Unique, assignment-aware executor leases and exact-token fencing.

use crate::cluster_membership::{BootIdentity, ClusterNamespace};
use anyhow::{Context, Result, bail};
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

pub const DEFAULT_PARTITION_LEASE_TTL: Duration = Duration::from_secs(3);
pub const DEFAULT_COORDINATION_OPERATION_TIMEOUT: Duration = Duration::from_millis(750);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseToken {
    pub boot_id: BootIdentity,
    pub acquisition_id: Uuid,
}

impl LeaseToken {
    pub fn new(boot_id: BootIdentity) -> Self {
        Self {
            boot_id,
            acquisition_id: Uuid::new_v4(),
        }
    }

    pub fn encode(&self) -> String {
        // The delimiter is not accepted in UUIDs, making the representation
        // unambiguous and cheap to compare inside Lua.
        format!("{}:{}", self.boot_id, self.acquisition_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLeaseGuard {
    namespace: ClusterNamespace,
    partition: u32,
    token: LeaseToken,
    /// Local receipt time of the successful acquire response. The Redis TTL
    /// began no later than one bounded operation timeout before this instant,
    /// so watchdogs subtract that timeout when calculating their fail-closed
    /// deadline.
    acquired_at: Instant,
}

impl PartitionLeaseGuard {
    pub fn namespace(&self) -> &ClusterNamespace {
        &self.namespace
    }

    pub fn partition(&self) -> u32 {
        self.partition
    }

    pub fn token(&self) -> &LeaseToken {
        &self.token
    }

    pub fn encoded_token(&self) -> String {
        self.token.encode()
    }

    pub fn lease_key(&self) -> String {
        self.namespace.partition_lease(self.partition)
    }

    pub(crate) fn acquired_at(&self) -> Instant {
        self.acquired_at
    }
}

#[derive(Clone)]
pub struct PartitionLeaseStore {
    redis: ConnectionManager,
    namespace: ClusterNamespace,
    ttl: Duration,
    operation_timeout: Duration,
}

impl PartitionLeaseStore {
    pub fn new(
        redis: ConnectionManager,
        namespace: ClusterNamespace,
        ttl: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        if ttl < Duration::from_secs(1) {
            bail!("partition lease TTL must be at least one second");
        }
        if operation_timeout >= ttl {
            bail!("coordination operation timeout must be shorter than lease TTL");
        }
        Ok(Self {
            redis,
            namespace,
            ttl,
            operation_timeout,
        })
    }

    pub fn namespace(&self) -> &ClusterNamespace {
        &self.namespace
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(crate) fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    /// Acquires a fresh term only when the atomic desired assignment selects
    /// this boot and no other acquisition currently holds the partition.
    pub async fn try_acquire(
        &self,
        partition: u32,
        boot_id: &BootIdentity,
    ) -> Result<Option<PartitionLeaseGuard>> {
        let token = LeaseToken::new(boot_id.clone());
        let encoded = token.encode();
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            local assignment_json = redis.call('GET', KEYS[1])
            if not assignment_json then return -2 end
            local assignment_ok, assignment = pcall(cjson.decode, assignment_json)
            if not assignment_ok then return -3 end
            local owner = assignment.owners[tostring(ARGV[1])]
            if owner ~= ARGV[2] then return 0 end

            if redis.call('EXISTS', KEYS[2]) ~= 0 then return 0 end
            redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[4])
            return 1
                "#,
            );
            script
                .key(self.namespace.assignment())
                .key(self.namespace.partition_lease(partition))
                .arg(partition)
                .arg(boot_id.as_str())
                .arg(&encoded)
                .arg(self.ttl.as_millis() as u64)
                .invoke_async::<i32>(&mut redis)
                .await
        };
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("partition lease acquisition timed out")?
            .context("partition lease acquisition failed")?;
        match result {
            1 => Ok(Some(PartitionLeaseGuard {
                namespace: self.namespace.clone(),
                partition,
                token,
                acquired_at: Instant::now(),
            })),
            0 => Ok(None),
            -2 => bail!("partition assignment is missing"),
            -3 => bail!("partition assignment is malformed"),
            other => bail!("unknown partition lease result {other}"),
        }
    }

    /// Renews only while both the desired owner and exact acquisition token
    /// still match.  Assignment change therefore naturally starts handoff.
    pub async fn renew(&self, guard: &PartitionLeaseGuard) -> Result<bool> {
        self.ensure_guard_namespace(guard)?;
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            local assignment_json = redis.call('GET', KEYS[1])
            if not assignment_json then return 0 end
            local ok, assignment = pcall(cjson.decode, assignment_json)
            if not ok or assignment.owners[tostring(ARGV[1])] ~= ARGV[2] then return 0 end
            if redis.call('GET', KEYS[2]) ~= ARGV[3] then return 0 end
            return redis.call('PEXPIRE', KEYS[2], ARGV[4])
                "#,
            );
            script
                .key(self.namespace.assignment())
                .key(guard.lease_key())
                .arg(guard.partition)
                .arg(guard.token.boot_id.as_str())
                .arg(guard.encoded_token())
                .arg(self.ttl.as_millis() as u64)
                .invoke_async::<i32>(&mut redis)
                .await
        };
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("partition lease renewal timed out")?
            .context("partition lease renewal failed")?;
        Ok(result == 1)
    }

    pub async fn validate(&self, guard: &PartitionLeaseGuard) -> Result<bool> {
        self.ensure_guard_namespace(guard)?;
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then return 1 end
            return 0
                "#,
            );
            script
                .key(guard.lease_key())
                .arg(guard.encoded_token())
                .invoke_async::<i32>(&mut redis)
                .await
        };
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("partition lease validation timed out")?
            .context("partition lease validation failed")?;
        Ok(result == 1)
    }

    /// Extends the exact incumbent token while it is executing a cooperative
    /// barrier after assignment has already moved. No new owner can acquire
    /// until this token is compare-deleted, and a stale token cannot extend a
    /// successor's lease.
    pub async fn renew_for_handoff(&self, guard: &PartitionLeaseGuard) -> Result<bool> {
        self.ensure_guard_namespace(guard)?;
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
                "#,
            );
            script
                .key(guard.lease_key())
                .arg(guard.encoded_token())
                .arg(self.ttl.as_millis() as u64)
                .invoke_async::<i32>(&mut redis)
                .await
        };
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("partition handoff lease renewal timed out")?
            .context("partition handoff lease renewal failed")?;
        Ok(result == 1)
    }

    /// Graceful release is compare-delete.  A delayed release can never delete
    /// a successor's different acquisition token.
    pub async fn release(&self, guard: &PartitionLeaseGuard) -> Result<bool> {
        self.ensure_guard_namespace(guard)?;
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            redis.call('DEL', KEYS[1])
            return 1
                "#,
            );
            script
                .key(guard.lease_key())
                .arg(guard.encoded_token())
                .invoke_async::<i32>(&mut redis)
                .await
        };
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("partition lease release timed out")?
            .context("partition lease release failed")?;
        Ok(result == 1)
    }

    fn ensure_guard_namespace(&self, guard: &PartitionLeaseGuard) -> Result<()> {
        if guard.namespace != self.namespace {
            bail!("partition lease guard belongs to a different region");
        }
        Ok(())
    }
}

/// A small unique-token lease used for the assignment coordinator.  Unlike a
/// partition lease it has no desired-owner predicate.
#[derive(Clone)]
pub struct CoordinatorLeaseStore {
    redis: ConnectionManager,
    namespace: ClusterNamespace,
    ttl: Duration,
    operation_timeout: Duration,
}

impl CoordinatorLeaseStore {
    pub fn new(
        redis: ConnectionManager,
        namespace: ClusterNamespace,
        ttl: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        if operation_timeout >= ttl {
            bail!("coordinator operation timeout must be shorter than lease TTL");
        }
        Ok(Self {
            redis,
            namespace,
            ttl,
            operation_timeout,
        })
    }

    pub async fn try_acquire(&self, boot_id: &BootIdentity) -> Result<Option<LeaseToken>> {
        use redis::AsyncCommands;
        let token = LeaseToken::new(boot_id.clone());
        let mut redis = self.redis.clone();
        let future = redis.set_options::<_, _, Option<String>>(
            self.namespace.assignment_lease(),
            token.encode(),
            redis::SetOptions::default()
                .conditional_set(redis::ExistenceCheck::NX)
                .with_expiration(redis::SetExpiry::PX(self.ttl.as_millis() as u64)),
        );
        let result = tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("coordinator lease acquisition timed out")?
            .context("coordinator lease acquisition failed")?;
        Ok(result.map(|_| token))
    }

    pub async fn renew(&self, token: &LeaseToken) -> Result<bool> {
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
                "#,
            );
            script
                .key(self.namespace.assignment_lease())
                .arg(token.encode())
                .arg(self.ttl.as_millis() as u64)
                .invoke_async::<i32>(&mut redis)
                .await
        };
        Ok(tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("coordinator lease renewal timed out")?
            .context("coordinator lease renewal failed")?
            == 1)
    }

    /// Releases only the caller's exact coordinator term. A delayed drain
    /// from an old task cannot delete a successor's lease.
    pub async fn release(&self, token: &LeaseToken) -> Result<bool> {
        let mut redis = self.redis.clone();
        let future = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            redis.call('DEL', KEYS[1])
            return 1
                "#,
            );
            script
                .key(self.namespace.assignment_lease())
                .arg(token.encode())
                .invoke_async::<i32>(&mut redis)
                .await
        };
        Ok(tokio::time::timeout(self.operation_timeout, future)
            .await
            .context("coordinator lease release timed out")?
            .context("coordinator lease release failed")?
            == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_acquisition_token_is_unique() {
        let boot = BootIdentity::new();
        let first = LeaseToken::new(boot.clone());
        let second = LeaseToken::new(boot);
        assert_ne!(first, second);
        assert_ne!(first.encode(), second.encode());
    }

    #[test]
    fn coordination_timeout_must_be_shorter_than_authority_ttl() {
        let namespace = ClusterNamespace::new("test").unwrap();
        // Constructing the manager requires a real connection, so validate the
        // public invariant through a helper-like comparison here as a regression
        // assertion for the constructor condition.
        assert!(DEFAULT_COORDINATION_OPERATION_TIMEOUT < DEFAULT_PARTITION_LEASE_TTL);
        assert!(namespace.partition_lease(1).contains(":test:"));
    }
}
