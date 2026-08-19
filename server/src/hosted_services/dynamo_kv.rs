//! DynamoDB-backed [`KeyValueStore`], used for `ExclusionDomain::Global`.
//!
//! Valkey does not span regions, so a globally-unique lease needs the only
//! substrate both regions share: the US DynamoDB table, which every task
//! already reaches because `AWS_REGION` is pinned to `us-east-1` fleet-wide.
//!
//! Three hazards distinguish this from the Valkey path, and each is handled
//! explicitly:
//!
//! 1. **There is no server-side clock.** Expiry is a caller-supplied epoch
//!    millisecond compared inside the condition, so safety depends on bounded
//!    inter-region clock skew rather than on the store. [`SKEW_ALLOWANCE`]
//!    makes that budget explicit instead of implicit.
//! 2. **DynamoDB TTL cannot expire a lease** — it is asynchronous and may lag
//!    by up to 48 hours. TTL is set for cleanup only; correctness comes
//!    entirely from the condition expression.
//! 3. **A failed condition must be distinguished by type**, not by re-reading.
//!    A re-read would race a successor's acquisition.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use snaketron_service_api::ServiceError;
use snaketron_service_api::deps::{CasOutcome, KeyValueStore};

/// Extra margin subtracted from a lease's usable life to absorb clock skew
/// between regions. A lease is treated as expired only once it is older than
/// its TTL *plus* this allowance, so a fast clock cannot steal a live lease.
const SKEW_ALLOWANCE: Duration = Duration::from_secs(2);

pub struct DynamoKeyValueStore {
    client: Client,
    table: String,
}

impl DynamoKeyValueStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn pk(key: &str) -> AttributeValue {
        AttributeValue::S(format!("LEASE#{key}"))
    }

    fn sk() -> AttributeValue {
        AttributeValue::S("LEASE".to_owned())
    }
}

fn is_condition_failure<E>(error: &SdkError<E>) -> bool
where
    E: ConditionAware,
{
    match error {
        SdkError::ServiceError(inner) => inner.err().is_condition_failure(),
        _ => false,
    }
}

/// Lets both operations report a failed precondition without re-reading, which
/// would race a successor.
trait ConditionAware {
    fn is_condition_failure(&self) -> bool;
}

impl ConditionAware for PutItemError {
    fn is_condition_failure(&self) -> bool {
        matches!(self, PutItemError::ConditionalCheckFailedException(_))
    }
}

impl ConditionAware for UpdateItemError {
    fn is_condition_failure(&self) -> bool {
        matches!(self, UpdateItemError::ConditionalCheckFailedException(_))
    }
}

#[async_trait]
impl KeyValueStore for DynamoKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<String>, ServiceError> {
        let response = self
            .client
            .get_item()
            .table_name(&self.table)
            .key("pk", Self::pk(key))
            .key("sk", Self::sk())
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| ServiceError::failed(format!("lease get failed: {e}")))?;

        let Some(item) = response.item else {
            return Ok(None);
        };
        let expires = item
            .get("expiresAtMs")
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<i64>().ok())
            .unwrap_or(0);
        if expires <= Self::now_ms() {
            return Ok(None);
        }
        Ok(item.get("holder").and_then(|v| v.as_s().ok()).cloned())
    }

    async fn try_acquire_lease(
        &self,
        key: &str,
        holder: &str,
        rank: u32,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        let now = Self::now_ms();
        let expires = now + ttl.as_millis() as i64;
        // Expiry is compared against a caller-supplied clock, so the skew
        // allowance keeps a fast clock from stealing a live lease.
        let stale_before = now - SKEW_ALLOWANCE.as_millis() as i64;

        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .item("pk", Self::pk(key))
            .item("sk", Self::sk())
            .item("holder", AttributeValue::S(holder.to_owned()))
            .item("holderRank", AttributeValue::N(rank.to_string()))
            .item("expiresAtMs", AttributeValue::N(expires.to_string()))
            // TTL is cleanup only; it is asynchronous (up to 48h) and never
            // load-bearing. Correctness comes entirely from the condition.
            .item(
                "ttl",
                AttributeValue::N((expires / 1000 + 3600).to_string()),
            )
            // Free, expired, or held by a strictly less-preferred node. The
            // rank clause is what lets a preferred region reclaim leadership;
            // `>` rather than `>=` is what stops two equally-ranked nodes from
            // evicting each other in a loop.
            .condition_expression(
                "attribute_not_exists(pk) OR expiresAtMs < :stale OR holderRank > :rank",
            )
            .expression_attribute_values(":stale", AttributeValue::N(stale_before.to_string()))
            .expression_attribute_values(":rank", AttributeValue::N(rank.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(CasOutcome::Applied),
            Err(error) if is_condition_failure(&error) => Ok(CasOutcome::Rejected),
            Err(error) => Err(ServiceError::failed(format!(
                "lease acquire failed: {error}"
            ))),
        }
    }

    async fn extend_if_equal(
        &self,
        key: &str,
        expected: &str,
        ttl: Duration,
    ) -> Result<CasOutcome, ServiceError> {
        let expires = Self::now_ms() + ttl.as_millis() as i64;
        let result = self
            .client
            .update_item()
            .table_name(&self.table)
            .key("pk", Self::pk(key))
            .key("sk", Self::sk())
            .update_expression("SET expiresAtMs = :expires, #ttl = :ttl")
            .condition_expression("holder = :holder")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":holder", AttributeValue::S(expected.to_owned()))
            .expression_attribute_values(":expires", AttributeValue::N(expires.to_string()))
            .expression_attribute_values(
                ":ttl",
                AttributeValue::N((expires / 1000 + 3600).to_string()),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(CasOutcome::Applied),
            Err(error) if is_condition_failure(&error) => Ok(CasOutcome::Rejected),
            Err(error) => Err(ServiceError::failed(format!("lease renew failed: {error}"))),
        }
    }

    async fn delete_if_equal(&self, key: &str, expected: &str) -> Result<CasOutcome, ServiceError> {
        let result = self
            .client
            .delete_item()
            .table_name(&self.table)
            .key("pk", Self::pk(key))
            .key("sk", Self::sk())
            .condition_expression("holder = :holder")
            .expression_attribute_values(":holder", AttributeValue::S(expected.to_owned()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(CasOutcome::Applied),
            Err(SdkError::ServiceError(inner))
                if matches!(
                    inner.err(),
                    aws_sdk_dynamodb::operation::delete_item::DeleteItemError
                        ::ConditionalCheckFailedException(_)
                ) =>
            {
                Ok(CasOutcome::Rejected)
            }
            Err(error) => Err(ServiceError::failed(format!(
                "lease release failed: {error}"
            ))),
        }
    }

    async fn increment(&self, key: &str) -> Result<u64, ServiceError> {
        let response = self
            .client
            .update_item()
            .table_name(&self.table)
            .key("pk", AttributeValue::S(format!("LEASE_EPOCH#{key}")))
            .key("sk", Self::sk())
            .update_expression("SET epoch = if_not_exists(epoch, :zero) + :one")
            .expression_attribute_values(":zero", AttributeValue::N("0".to_owned()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_owned()))
            .return_values(ReturnValue::AllNew)
            .send()
            .await
            .map_err(|e| ServiceError::failed(format!("epoch increment failed: {e}")))?;

        response
            .attributes
            .as_ref()
            .and_then(|a| a.get("epoch"))
            .and_then(|v| v.as_n().ok())
            .and_then(|n| n.parse::<u64>().ok())
            .ok_or_else(|| ServiceError::failed("epoch increment returned no value"))
    }
}
