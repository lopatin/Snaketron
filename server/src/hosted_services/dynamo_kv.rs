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
//!    inter-region clock skew rather than on the store. That bound is not
//!    assumed here: because both regions talk to the *same* table, every
//!    response carries a `Date` header from one authoritative clock, so each
//!    node can measure its own offset from it and refuse to take a global
//!    lease while that offset is outside [`ACQUIRE_OFFSET_BUDGET`]. Only
//!    acquisition is gated — see [`SKEW_ALLOWANCE`].
//! 2. **DynamoDB TTL cannot expire a lease** — it is asynchronous and may lag
//!    by up to 48 hours. TTL is set for cleanup only; correctness comes
//!    entirely from the condition expression.
//! 3. **A failed condition must be distinguished by type**, not by re-reading.
//!    A re-read would race a successor's acquisition.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::interceptors::{
    BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
};
use aws_sdk_dynamodb::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::primitives::{DateTime, DateTimeFormat};
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use snaketron_service_api::ServiceError;
use snaketron_service_api::deps::{CasOutcome, KeyValueStore};

use super::supervisor::RENEW_DIVISOR;

/// Budget for how far apart two nodes' wall clocks may be before a lease can
/// be stolen from a live holder.
///
/// Both regions write the same us-east-1 table, so there is exactly one
/// authoritative clock and every DynamoDB response reports it in a `Date`
/// header. Sampling that header from us-east-1 and eu-west-1 put the local
/// offset inside a second either way — medians of -0.52 s and -0.15 s over
/// five samples each. `Date` is stamped to whole seconds, though, so those
/// figures sit at the method's resolution limit: sub-second differences are
/// not measurable this way and must not be claimed. Two seconds is the
/// smallest budget that clears that one-second granularity on both sides.
///
/// The constant controls two things:
///
/// 1. The staleness margin inside the acquisition condition: a lease counts as
///    expired only once it is older than its term *plus* this, so a fast clock
///    cannot steal a live lease.
/// 2. Per node, at half its value ([`ACQUIRE_OFFSET_BUDGET`]), how far one
///    node's own measured offset may be before it declines to take a global
///    lease at all.
const SKEW_ALLOWANCE: Duration = Duration::from_secs(2);

/// How far a single node's clock may sit from the DynamoDB service clock and
/// still be allowed to *take* a global lease.
///
/// Half of [`SKEW_ALLOWANCE`], and that halving is the whole argument: the
/// pairwise skew between two nodes is at most the sum of their individual
/// offsets from the one clock they both observe, so holding every node to half
/// the allowance bounds any pair by the whole of it. This turns the allowance
/// from an assumption about NTP into something each node checks about itself.
///
/// It also lands at exactly the `Date` header's one-second resolution, which
/// is the smallest offset this measurement can distinguish from quantisation.
const ACQUIRE_OFFSET_BUDGET: Duration =
    Duration::from_millis(SKEW_ALLOWANCE.as_millis() as u64 / 2);

/// Shortest global lease term for which [`SKEW_ALLOWANCE`] still means
/// something.
///
/// The supervisor renews `RENEW_DIVISOR` times per term, so a holder that
/// loses one renewal has exactly one interval — `ttl / RENEW_DIVISOR` — in
/// which to recover before a contender may act. The skew allowance is charged
/// against that same interval: a retry landing inside it is racing a contender
/// that already reads the lease as stale. Once the allowance is as wide as the
/// recovery window it has stopped being a margin, which is why this floor is
/// `RENEW_DIVISOR * SKEW_ALLOWANCE` and not something rounder.
///
/// This is a floor, not a recommendation. Deployed terms are set well above it
/// so a lost renewal stays a recoverable event; see
/// `game_server::HOSTED_SERVICE_GLOBAL_LEASE_TTL`, which const-asserts against
/// this value.
pub(crate) const MIN_GLOBAL_LEASE_TTL: Duration =
    Duration::from_millis(SKEW_ALLOWANCE.as_millis() as u64 * RENEW_DIVISOR as u64);

/// Bit pattern for "no offset has been observed yet". `i64::MIN` milliseconds
/// is ~292 million years, which no `Date` header can produce, so one atomic
/// carries both the value and its validity — a reader can never pair a fresh
/// validity flag with a stale number.
const UNMEASURED_OFFSET_MS: i64 = i64::MIN;

/// One node's running estimate of how far its own wall clock sits from the
/// clock that stamps DynamoDB responses. Positive means the local clock leads.
#[derive(Debug)]
struct ServiceClockOffset {
    latest_ms: AtomicI64,
}

impl ServiceClockOffset {
    fn new() -> Self {
        Self {
            latest_ms: AtomicI64::new(UNMEASURED_OFFSET_MS),
        }
    }

    fn record(&self, offset_ms: i64) {
        self.latest_ms.store(offset_ms, Ordering::Relaxed);
    }

    fn latest(&self) -> Option<i64> {
        match self.latest_ms.load(Ordering::Relaxed) {
            UNMEASURED_OFFSET_MS => None,
            offset => Some(offset),
        }
    }
}

/// Records the local-versus-service clock offset from the `Date` header every
/// DynamoDB response already carries.
///
/// A fresh probe is attached to each operation rather than to the client, so
/// the send timestamp it stashes belongs unambiguously to that one call;
/// sharing a probe across concurrent operations would pair one call's send
/// time with another's response.
///
/// The SDK ships `ServiceClockSkewInterceptor`, which reads the same header,
/// but it clamps the result at zero and keeps it crate-private, so it can only
/// answer "is the service ahead of me" — not "is my clock trustworthy in
/// either direction", which is what the acquisition gate needs.
#[derive(Debug)]
struct ClockOffsetProbe {
    offset: Arc<ServiceClockOffset>,
    /// Local send time of the in-flight attempt. A retry overwrites it, which
    /// is correct: only the attempt that produced the observed `Date` matters.
    sent_at_ms: AtomicI64,
}

impl ClockOffsetProbe {
    fn new(offset: Arc<ServiceClockOffset>) -> Self {
        Self {
            offset,
            sent_at_ms: AtomicI64::new(UNMEASURED_OFFSET_MS),
        }
    }
}

impl Intercept for ClockOffsetProbe {
    fn name(&self) -> &'static str {
        "SnaketronClockOffsetProbe"
    }

    fn read_before_transmit(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.sent_at_ms.store(now_ms(), Ordering::Relaxed);
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let received_ms = now_ms();
        let sent_ms = self.sent_at_ms.load(Ordering::Relaxed);
        if sent_ms == UNMEASURED_OFFSET_MS {
            return Ok(());
        }
        // A missing or malformed `Date` leaves the previous estimate in place
        // rather than failing the request: 5xx responses and proxies may omit
        // it, and a lost sample is not itself evidence of a bad clock. An
        // acquisition with no sample at all is refused separately.
        if let Some(date) = context.response().headers().get("date")
            && let Some(offset) = estimate_offset_ms(date, sent_ms, received_ms)
        {
            self.offset.record(offset);
        }
        Ok(())
    }
}

/// NTP-style single-sample offset estimate: the local midpoint of send and
/// receive, minus the instant the service stamped into `Date`. The midpoint is
/// what cancels the two network legs, on the usual assumption that they are
/// roughly symmetric.
///
/// `Date` carries whole seconds, so the instant it reports is up to a second
/// earlier than the moment it was stamped, and every estimate inherits an
/// unknown error somewhere in `[0, 1 s)` no matter how precisely the midpoint
/// is taken. That resolution, not the arithmetic, is why
/// [`ACQUIRE_OFFSET_BUDGET`] is a full second rather than the tens of
/// milliseconds a real NTP client would work with.
fn estimate_offset_ms(date: &str, sent_ms: i64, received_ms: i64) -> Option<i64> {
    let service_ms = DateTime::from_str(date, DateTimeFormat::HttpDate)
        .ok()?
        .to_millis()
        .ok()?;
    let midpoint_ms = sent_ms + (received_ms - sent_ms) / 2;
    Some(midpoint_ms - service_ms)
}

/// Refuses a term so short that [`SKEW_ALLOWANCE`] would consume the holder's
/// whole recovery window. Acquisition only: a lease that cannot be taken can
/// never be renewed, so repeating the check on the renewal path would add a
/// failure mode without removing one.
fn check_lease_ttl(ttl: Duration) -> Result<(), ServiceError> {
    if ttl < MIN_GLOBAL_LEASE_TTL {
        return Err(ServiceError::InvalidConfig(format!(
            "global lease term of {} ms is below the {} ms floor: the supervisor renews \
             {RENEW_DIVISOR} times per term, so a missed renewal leaves only ttl/{RENEW_DIVISOR} \
             to recover in, and that has to stay wider than the {} ms clock-skew allowance",
            ttl.as_millis(),
            MIN_GLOBAL_LEASE_TTL.as_millis(),
            SKEW_ALLOWANCE.as_millis(),
        )));
    }
    Ok(())
}

/// Refuses to take a global lease on a node whose clock is not provably inside
/// its share of the skew budget.
///
/// Fail closed on `None`: an unmeasured clock is not a healthy one. This costs
/// no availability, because acquisition is the only operation here with a time
/// term — a node that declines simply does not become the holder, and whoever
/// already holds the lease renews on holder equality alone.
fn check_clock_offset(latest_ms: Option<i64>) -> Result<(), ServiceError> {
    let budget_ms = ACQUIRE_OFFSET_BUDGET.as_millis() as i64;
    match latest_ms {
        None => Err(ServiceError::failed(
            "refusing to acquire a global lease: this node has no measured offset from the \
             DynamoDB service clock, and an unverified clock cannot be held to the skew budget",
        )),
        Some(offset_ms) if offset_ms.abs() > budget_ms => Err(ServiceError::failed(format!(
            "refusing to acquire a global lease: this node's clock is {offset_ms} ms from the \
             DynamoDB service clock, past the {budget_ms} ms per-node budget (half of the {} ms \
             cross-region allowance, so any two nodes stay within the whole of it). Renewal and \
             release are unaffected, so an existing holder keeps its lease",
            SKEW_ALLOWANCE.as_millis(),
        ))),
        Some(_) => Ok(()),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct DynamoKeyValueStore {
    client: Client,
    table: String,
    offset: Arc<ServiceClockOffset>,
}

impl DynamoKeyValueStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
            offset: Arc::new(ServiceClockOffset::new()),
        }
    }

    /// A probe for one operation. Every call site attaches one, so the offset
    /// stays fresh for free — in particular the epoch `increment` that
    /// `ExclusionLeaseStore::try_acquire` always issues immediately before
    /// acquiring supplies the sample the gate then reads.
    fn probe(&self) -> ClockOffsetProbe {
        ClockOffsetProbe::new(self.offset.clone())
    }

    /// One read whose only purpose is its response headers, used when a node
    /// reaches acquisition before any other call has sampled the clock.
    /// Projected down to the partition key and left eventually consistent: the
    /// item's contents are irrelevant, the `Date` header is not.
    async fn sample_service_clock(&self, key: &str) -> Result<(), ServiceError> {
        self.client
            .get_item()
            .table_name(&self.table)
            .key("pk", Self::pk(key))
            .key("sk", Self::sk())
            .projection_expression("pk")
            .customize()
            .interceptor(self.probe())
            .send()
            .await
            .map_err(|e| ServiceError::failed(format!("clock sample failed: {e}")))?;
        Ok(())
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
            .customize()
            .interceptor(self.probe())
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
        if expires <= now_ms() {
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
        check_lease_ttl(ttl)?;
        if self.offset.latest().is_none() {
            // Fail closed rather than assume health: the first acquisition on
            // a freshly started node measures before it decides.
            self.sample_service_clock(key).await?;
        }
        check_clock_offset(self.offset.latest())?;

        let now = now_ms();
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
            .customize()
            .interceptor(self.probe())
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
        // Deliberately ungated. The condition below has no time term at all —
        // it compares holder equality — so renewal is clock-independent, and a
        // holder whose clock has drifted keeps its lease instead of dropping
        // work it is still the only node doing.
        let expires = now_ms() + ttl.as_millis() as i64;
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
            .customize()
            .interceptor(self.probe())
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
            .customize()
            .interceptor(self.probe())
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
            .customize()
            .interceptor(self.probe())
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use aws_sdk_dynamodb::config::retry::RetryConfig;
    use aws_sdk_dynamodb::config::timeout::TimeoutConfig;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    use super::*;

    const BUDGET_MS: i64 = 1_000;

    #[test]
    fn per_node_budget_is_half_the_cross_region_allowance() {
        // The halving is the argument that turns the allowance into a bound:
        // two nodes each inside half of it are inside all of it pairwise.
        assert_eq!(ACQUIRE_OFFSET_BUDGET * 2, SKEW_ALLOWANCE);
        assert_eq!(ACQUIRE_OFFSET_BUDGET.as_millis() as i64, BUDGET_MS);
    }

    #[test]
    fn an_offset_inside_the_per_node_budget_permits_acquisition() {
        for offset_ms in [
            0,
            1,
            -1,
            BUDGET_MS - 1,
            -(BUDGET_MS - 1),
            BUDGET_MS,
            -BUDGET_MS,
        ] {
            check_clock_offset(Some(offset_ms)).unwrap_or_else(|e| {
                panic!("{offset_ms} ms is inside the budget but was refused: {e}")
            });
        }
    }

    #[test]
    fn an_offset_outside_the_per_node_budget_refuses_acquisition() {
        for offset_ms in [BUDGET_MS + 1, -(BUDGET_MS + 1), 60_000, -60_000] {
            let error = check_clock_offset(Some(offset_ms))
                .expect_err("an out-of-budget clock must not take a global lease");
            let message = error.to_string();
            assert!(
                message.contains(&offset_ms.to_string()) && message.contains("Renewal"),
                "the refusal has to name the measured offset and say what still works: {message}"
            );
        }
    }

    #[test]
    fn an_unmeasured_clock_refuses_acquisition() {
        // Fail closed: never having looked is not the same as being fine.
        let error = check_clock_offset(None).expect_err("an unmeasured clock must not acquire");
        assert!(error.to_string().contains("no measured offset"));
    }

    #[test]
    fn a_term_the_skew_allowance_would_swallow_is_refused() {
        check_lease_ttl(MIN_GLOBAL_LEASE_TTL).expect("the floor itself is admissible");
        check_lease_ttl(MIN_GLOBAL_LEASE_TTL + Duration::from_millis(1)).expect("above the floor");

        let error = check_lease_ttl(MIN_GLOBAL_LEASE_TTL - Duration::from_millis(1))
            .expect_err("a term below the floor must be refused");
        assert!(
            error
                .to_string()
                .contains(&SKEW_ALLOWANCE.as_millis().to_string()),
            "the message has to relate the term to the allowance: {error}"
        );

        // The floor exists to keep a missed renewal recoverable: at the floor,
        // the one interval a holder has left is exactly the skew allowance.
        assert_eq!(MIN_GLOBAL_LEASE_TTL / RENEW_DIVISOR, SKEW_ALLOWANCE);
    }

    #[test]
    fn offset_estimate_is_the_send_receive_midpoint_minus_the_service_date() {
        // 2026-08-19T12:00:00Z.
        let service_ms = 1_787_140_800_000_i64;
        let date = DateTime::from_millis(service_ms)
            .fmt(DateTimeFormat::HttpDate)
            .expect("HTTP-date is representable");

        // Midpoint of [service - 200, service + 600] is service + 200, so a
        // local clock 200 ms ahead. A receive-only reading would have said 600.
        assert_eq!(
            estimate_offset_ms(&date, service_ms - 200, service_ms + 600),
            Some(200)
        );
        assert_eq!(
            estimate_offset_ms(&date, service_ms - 5_000, service_ms - 5_000),
            Some(-5_000)
        );
        assert_eq!(estimate_offset_ms("not a date", 0, 0), None);
    }

    /// Canned DynamoDB responses carrying a `Date` header from a clock that
    /// leads local time by `service_skew_ms`. Nothing here touches a socket.
    #[derive(Debug)]
    struct DatedResponses {
        service_skew_ms: i64,
        calls: Arc<AtomicU32>,
    }

    impl HttpConnector for DatedResponses {
        fn call(&self, _request: HttpRequest) -> HttpConnectorFuture {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let date = DateTime::from_millis(now_ms() + self.service_skew_ms)
                .fmt(DateTimeFormat::HttpDate)
                .expect("HTTP-date is representable");
            // An empty AwsJson1_0 document deserializes to a default output,
            // which is all these operations need: the header is the payload
            // under test.
            let mut response = HttpResponse::new(
                StatusCode::try_from(200).expect("200 is a status code"),
                SdkBody::from("{}"),
            );
            response.headers_mut().insert("date", date);
            response
                .headers_mut()
                .insert("content-type", "application/x-amz-json-1.0");
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    /// A store whose every request is answered locally, by a service whose
    /// clock leads this node's by `service_skew_ms`.
    fn store_with_service_skew(service_skew_ms: i64) -> (DynamoKeyValueStore, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        let connector = SharedHttpConnector::new(DatedResponses {
            service_skew_ms,
            calls: calls.clone(),
        });
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("ak", "sk", None, None, "test"))
            .http_client(http_client_fn(move |_, _| connector.clone()))
            // Both disabled so the runtime never asks for a sleep
            // implementation, and so a test can never sit in a retry backoff.
            .retry_config(RetryConfig::disabled())
            .timeout_config(TimeoutConfig::disabled())
            .build();
        (
            DynamoKeyValueStore::new(Client::from_conf(config), "leases"),
            calls,
        )
    }

    /// The service is half a second ahead, so after truncation to the `Date`
    /// header's whole seconds the measured offset lands well inside the budget
    /// whatever the sub-second phase of the test happens to be.
    const HEALTHY_SKEW_MS: i64 = 500;
    /// An hour out: unmistakably past the budget, and past anything the
    /// header's one-second resolution could account for.
    const BROKEN_SKEW_MS: i64 = -3_600_000;

    #[tokio::test]
    async fn acquisition_samples_the_clock_before_deciding() {
        let (store, calls) = store_with_service_skew(HEALTHY_SKEW_MS);
        assert!(store.offset.latest().is_none(), "nothing measured yet");

        let outcome = store
            .try_acquire_lease("k", "holder", 0, MIN_GLOBAL_LEASE_TTL)
            .await
            .expect("a healthy clock may acquire");
        assert_eq!(outcome, CasOutcome::Applied);
        // The sample plus the PutItem: the gate did not decide on an
        // unmeasured clock, and did not skip the write once it passed.
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(store.offset.latest().is_some());
    }

    #[tokio::test]
    async fn a_broken_clock_refuses_acquisition_after_sampling_it() {
        let (store, calls) = store_with_service_skew(BROKEN_SKEW_MS);

        let error = store
            .try_acquire_lease("k", "holder", 0, MIN_GLOBAL_LEASE_TTL)
            .await
            .expect_err("a clock an hour out must not take a global lease");
        assert!(error.to_string().contains("refusing to acquire"), "{error}");
        // Sampled, then refused: no PutItem was issued.
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_broken_clock_does_not_stop_renewal_or_release() {
        let (store, _calls) = store_with_service_skew(BROKEN_SKEW_MS);

        // Renewal and release compare holder equality; they have no time term
        // to be wrong about, so a holder whose clock has drifted keeps the
        // work it is still the only node doing.
        assert_eq!(
            store
                .extend_if_equal("k", "holder", MIN_GLOBAL_LEASE_TTL)
                .await
                .expect("renewal must not consult the clock offset"),
            CasOutcome::Applied
        );
        assert_eq!(
            store
                .delete_if_equal("k", "holder")
                .await
                .expect("release must not consult the clock offset"),
            CasOutcome::Applied
        );

        // And the renewal really did observe the bad clock — it simply did not
        // act on it, so this is not passing for want of a measurement.
        let offset = store.offset.latest().expect("renewal still samples");
        assert!(
            offset > BUDGET_MS,
            "the drift should have been recorded, got {offset} ms"
        );
        assert!(check_clock_offset(Some(offset)).is_err());
    }

    #[tokio::test]
    async fn a_term_below_the_floor_is_refused_before_any_request() {
        let (store, calls) = store_with_service_skew(HEALTHY_SKEW_MS);

        store
            .try_acquire_lease(
                "k",
                "holder",
                0,
                MIN_GLOBAL_LEASE_TTL - Duration::from_millis(1),
            )
            .await
            .expect_err("a term below the floor must be refused");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "a misconfigured term is a local decision, not a round trip"
        );
    }
}
