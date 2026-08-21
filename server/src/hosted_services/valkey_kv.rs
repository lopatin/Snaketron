//! Valkey-backed [`KeyValueStore`], used for `ExclusionDomain::Region` leases.
//!
//! Every conditional operation is a single Lua script so the comparison and the
//! effect are atomic. A get-then-set pair would leave a window in which a
//! successor acquires between the two calls, which is exactly the class of bug
//! exclusion is supposed to prevent.

use std::time::Duration;

use async_trait::async_trait;
use snaketron_service_api::ServiceError;
use snaketron_service_api::deps::{CasOutcome, KeyValueStore};

use crate::redis_utils::RedisConnection;

pub struct ValkeyKeyValueStore {
    redis: RedisConnection,
    operation_timeout: Duration,
}

impl ValkeyKeyValueStore {
    pub fn new(redis: RedisConnection, operation_timeout: Duration) -> Self {
        Self {
            redis,
            operation_timeout,
        }
    }

    async fn bounded<T>(
        &self,
        what: &'static str,
        future: impl std::future::Future<Output = redis::RedisResult<T>>,
    ) -> Result<T, ServiceError> {
        match tokio::time::timeout(self.operation_timeout, future).await {
            Err(_) => Err(ServiceError::failed(format!("{what} timed out"))),
            Ok(Err(error)) => Err(ServiceError::failed(format!("{what} failed: {error}"))),
            Ok(Ok(value)) => Ok(value),
        }
    }
}

#[async_trait]
impl KeyValueStore for ValkeyKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<String>, ServiceError> {
        use redis::AsyncCommands;
        let mut redis = self.redis.clone();
        self.bounded("kv get", redis.get::<_, Option<String>>(key))
            .await
    }

    async fn try_acquire_lease(
        &self,
        key: &str,
        holder: &str,
        rank: u32,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        let mut redis = self.redis.clone();
        // Compare and take in one script. A GET-then-SET pair would leave a
        // window in which a contender acquires between the two calls, which is
        // precisely the race exclusion exists to close.
        //
        // Expiry needs no clause: Redis removes the key itself, so an absent
        // value already means "free".
        //
        // `holder` arrives already rank-encoded by the lease store, and is
        // stored verbatim so that renew/release can compare against exactly
        // what a caller holds.
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if current then
                local holder_rank = tonumber(string.match(current, '^(%d+)|'))
                -- An unparseable value is treated as HELD, never as free:
                -- failing closed is the only safe reading of a value we do not
                -- understand.
                if holder_rank == nil then return 0 end
                if tonumber(ARGV[2]) >= holder_rank then return 0 end
            end
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
            return 1
            "#,
        );
        let applied: i32 = self
            .bounded(
                "kv try_acquire_lease",
                script
                    .key(key)
                    .arg(holder)
                    .arg(rank)
                    .arg(ttl.as_millis() as u64)
                    .invoke_async(&mut redis),
            )
            .await?;
        Ok(if applied == 1 {
            CasOutcome::Applied
        } else {
            CasOutcome::Rejected
        })
    }

    async fn extend_if_equal(
        &self,
        key: &str,
        expected: &str,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            "#,
        );
        let applied: i32 = self
            .bounded(
                "kv extend_if_equal",
                script
                    .key(key)
                    .arg(expected)
                    .arg(ttl.as_millis() as u64)
                    .invoke_async(&mut redis),
            )
            .await?;
        Ok(if applied == 1 {
            CasOutcome::Applied
        } else {
            CasOutcome::Rejected
        })
    }

    async fn delete_if_equal(&self, key: &str, expected: &str) -> Result<CasOutcome, ServiceError> {
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
            redis.call('DEL', KEYS[1])
            return 1
            "#,
        );
        let applied: i32 = self
            .bounded(
                "kv delete_if_equal",
                script.key(key).arg(expected).invoke_async(&mut redis),
            )
            .await?;
        Ok(if applied == 1 {
            CasOutcome::Applied
        } else {
            CasOutcome::Rejected
        })
    }

    async fn increment(&self, key: &str) -> Result<u64, ServiceError> {
        use redis::AsyncCommands;
        let mut redis = self.redis.clone();
        self.bounded("kv increment", redis.incr::<_, i64, i64>(key, 1))
            .await
            .map(|value| value.max(0) as u64)
    }
}
