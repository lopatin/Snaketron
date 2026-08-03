//! Read-only control-plane inspection for steady-state executor resilience.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use common::{ClientCommandIdentityV2, GameEvent, GameEventMessage, GameType, QueueMode};
use redis::AsyncCommands;
use redis::streams::{
    StreamPendingCountReply, StreamPendingId, StreamPendingReply, StreamRangeReply,
};
use serde::{Deserialize, Serialize};
use server::cluster_membership::{
    BootIdentity, ClusterNamespace, MEMBERSHIP_SCHEMA_VERSION, TaskLifecycle, TaskMembership,
};
use server::game_executor::PARTITION_COUNT;
use server::partition_assignment::{AssignmentDocument, AssignmentStore};
use server::redis_keys::RedisKeys;
use server::redis_utils::{RedisClient, RedisConnection};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;

const PENDING_ENTRY_SAMPLE_LIMIT: usize = 128;
const OUTPUT_SCAN_PAGE_SIZE: usize = 512;
const OUTPUT_SCAN_LIMIT: usize = 8_192;
const MIN_WATCH_INTERVAL_MS: u64 = 10;
const MAX_WATCH_INTERVAL_MS: u64 = 1_000;
const MAX_OUTPUT_WAIT_MS: u64 = 5_000;
const OUTPUT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(25);

struct ObserverClock {
    monotonic_origin: Instant,
    unix_lower_origin_ms: i64,
    unix_upper_origin_ms: i64,
}

impl ObserverClock {
    fn calibrate() -> Self {
        let unix_lower_origin_ms = Utc::now().timestamp_millis();
        let monotonic_origin = Instant::now();
        // `timestamp_millis` truncates. Adding one millisecond makes the
        // completion side conservative without widening an acceptance bound.
        let unix_upper_origin_ms = Utc::now().timestamp_millis().saturating_add(1);
        Self {
            monotonic_origin,
            unix_lower_origin_ms,
            unix_upper_origin_ms,
        }
    }

    fn elapsed_floor_ms(&self) -> i64 {
        i64::try_from(self.monotonic_origin.elapsed().as_millis()).unwrap_or(i64::MAX)
    }

    fn elapsed_ceil_ms(&self) -> i64 {
        let elapsed = self.monotonic_origin.elapsed();
        let milliseconds = elapsed.as_millis();
        let rounded_up = milliseconds.saturating_add(u128::from(
            !elapsed.subsec_nanos().is_multiple_of(1_000_000),
        ));
        i64::try_from(rounded_up).unwrap_or(i64::MAX)
    }

    fn observation_start_ms(&self) -> i64 {
        self.unix_lower_origin_ms
            .saturating_add(self.elapsed_floor_ms())
    }

    fn observation_completion_ms(&self) -> i64 {
        self.unix_upper_origin_ms
            .saturating_add(self.elapsed_ceil_ms())
    }
}

#[derive(Debug)]
struct Args {
    region_key: String,
    redis_url: String,
    watch_interval_ms: Option<u64>,
    wait_ms: Option<u64>,
    operation: Operation,
}

#[derive(Debug)]
enum Operation {
    Status {
        partition: Option<u32>,
    },
    Envelope,
    Ownership {
        partition: u32,
        killed_boot_id: BootIdentity,
    },
    Pending {
        partition: u32,
        consumer: String,
    },
    Output {
        partition: u32,
        after_stream_id: String,
    },
    TakeoverOutput {
        partition: u32,
        killed_boot_id: BootIdentity,
        baseline_status_path: PathBuf,
        ready_output_path: PathBuf,
        anchor_output_path: PathBuf,
    },
}

#[derive(Serialize)]
struct PendingEntry {
    id: String,
    consumer: String,
    idle_ms: u64,
    delivery_count: u64,
}

impl From<StreamPendingId> for PendingEntry {
    fn from(entry: StreamPendingId) -> Self {
        Self {
            id: entry.id,
            consumer: entry.consumer,
            idle_ms: entry.last_delivered_ms as u64,
            delivery_count: entry.times_delivered as u64,
        }
    }
}

#[derive(Serialize)]
struct RuntimePartition {
    partition: u32,
    desired_owner: Option<String>,
    active_owner: Option<String>,
    owner_matches: bool,
    lease_token: Option<String>,
    lease_ttl_ms: i64,
    consumer_group_exists: bool,
    pending_count: u64,
    pending_entry_sample: Vec<PendingEntry>,
    pending_completion_count: u64,
    quarantined_command_count: u64,
    active_games: u64,
}

#[derive(Serialize)]
struct Status {
    region_key: String,
    captured_at_ms: i64,
    live_members: Vec<TaskMembership>,
    assignment: Option<AssignmentDocument>,
    runtime_partitions: Vec<RuntimePartition>,
    quickmatch_two_v_two_queued_lobbies: u64,
}

#[derive(Serialize)]
struct EnvelopeRuntimePartition {
    partition: u32,
    desired_owner: Option<String>,
    active_owner: Option<String>,
    owner_matches: bool,
    lease_ttl_ms: i64,
    active_games: u64,
}

#[derive(Serialize)]
struct EnvelopeStatus {
    region_key: String,
    observation_started_at_ms: i64,
    observation_completed_at_ms: i64,
    live_members: Vec<TaskMembership>,
    assignment: Option<AssignmentDocument>,
    runtime_partitions: Vec<EnvelopeRuntimePartition>,
}

#[derive(Debug, PartialEq, Eq)]
struct AuthoritySnapshot {
    assignment_payload: Vec<u8>,
    lease_token: String,
    lease_ttl_ms: i64,
    observed_at_ms: i64,
    event_tail_id: String,
}

#[derive(Serialize)]
struct OwnershipRuntimePartition {
    partition: u32,
    desired_owner: String,
    active_owner: Option<String>,
    owner_matches: bool,
    lease_token: Option<String>,
    lease_ttl_ms: i64,
}

#[derive(Serialize)]
struct OwnershipStatus {
    region_key: String,
    observation_started_at_ms: i64,
    observation_completed_at_ms: i64,
    captured_at_ms: i64,
    #[serde(rename = "redis_membership_observed_at_ms")]
    membership_observed_at_ms: i64,
    #[serde(rename = "redis_authority_observed_at_ms")]
    authority_observed_at_ms: i64,
    authority_event_tail_id: String,
    authority_stable: bool,
    killed_member_live: bool,
    live_members: Vec<TaskMembership>,
    assignment: AssignmentDocument,
    runtime_partitions: Vec<OwnershipRuntimePartition>,
}

#[derive(Serialize)]
struct PendingStatus {
    region_key: String,
    observation_started_at_ms: i64,
    observation_completed_at_ms: i64,
    captured_at_ms: i64,
    redis_observation_started_at_ms: i64,
    redis_observation_completed_at_ms: i64,
    partition: u32,
    requested_consumer: String,
    pending_entry: Option<PendingEntry>,
}

#[derive(Serialize)]
struct AuthoritativeOutput {
    stream_id: String,
    stream_unix_ms: i64,
    game_id: u32,
    command_id: ClientCommandIdentityV2,
    deduplicated_replay: bool,
}

#[derive(Serialize)]
struct OutputStatus {
    region_key: String,
    observation_started_at_ms: i64,
    observation_completed_at_ms: i64,
    captured_at_ms: i64,
    redis_observation_started_at_ms: i64,
    redis_observation_completed_at_ms: i64,
    partition: u32,
    after_stream_id: String,
    first_scheduled_output: Option<AuthoritativeOutput>,
}

#[derive(Deserialize)]
struct TakeoverBaseline {
    live_members: Vec<TaskMembership>,
    assignment: AssignmentDocument,
    runtime_partitions: Vec<TakeoverBaselinePartition>,
}

#[derive(Deserialize)]
struct TakeoverBaselinePartition {
    partition: u32,
    lease_token: Option<String>,
}

struct TakeoverCriteria {
    assignment_version: u64,
    killed_boot_id: BootIdentity,
    killed_task_id: String,
    killed_lease_token: String,
    preexisting_tasks: BTreeMap<BootIdentity, String>,
}

struct TakeoverOutputRequest {
    region_key: String,
    partition: u32,
    killed_boot_id: BootIdentity,
    baseline_status_path: PathBuf,
    ready_output_path: PathBuf,
    anchor_output_path: PathBuf,
}

fn usage() -> &'static str {
    "Usage:
  resilience_admin status --region-key REGION [--redis-url URL] [--partition NUMBER]
  resilience_admin envelope --region-key REGION [--redis-url URL]
  resilience_admin ownership --region-key REGION [--redis-url URL] --partition NUMBER --killed-boot-id UUID [--watch-interval-ms MILLISECONDS]
  resilience_admin pending --region-key REGION [--redis-url URL] --partition NUMBER --consumer LEASE_TOKEN [--watch-interval-ms MILLISECONDS]
  resilience_admin output --region-key REGION [--redis-url URL] --partition NUMBER --after-stream-id STREAM_ID [--wait-ms MILLISECONDS]
  resilience_admin takeover-output --region-key REGION [--redis-url URL] --partition NUMBER --killed-boot-id UUID --baseline-status PATH --ready-output PATH --anchor-output PATH"
}

fn parse_partition(value: &str) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .with_context(|| format!("invalid partition {value:?}"))?;
    if parsed >= PARTITION_COUNT {
        bail!("partition must be less than {PARTITION_COUNT}, found {parsed}");
    }
    Ok(parsed)
}

fn parse_bounded_milliseconds(flag: &str, value: &str, minimum: u64, maximum: u64) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("invalid {flag} value {value:?}"))?;
    if !(minimum..=maximum).contains(&parsed) {
        bail!("{flag} must be between {minimum} and {maximum}, found {parsed}");
    }
    Ok(parsed)
}

fn parse_args_from<I, S>(values: I, redis_url_from_env: Option<String>) -> Result<Args>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut values = values.into_iter().map(Into::into);
    let operation = values.next().context(usage())?;
    let mut region_key = None;
    let mut redis_url = None;
    let mut partition = None;
    let mut killed_boot_id = None;
    let mut consumer = None;
    let mut after_stream_id = None;
    let mut watch_interval_ms = None;
    let mut wait_ms = None;
    let mut baseline_status_path = None;
    let mut ready_output_path = None;
    let mut anchor_output_path = None;
    while let Some(argument) = values.next() {
        match argument.as_str() {
            "--region-key" => {
                region_key = Some(values.next().context("--region-key requires a value")?);
            }
            "--redis-url" => {
                redis_url = Some(values.next().context("--redis-url requires a value")?);
            }
            "--partition" => {
                let value = values.next().context("--partition requires a value")?;
                partition = Some(parse_partition(&value)?);
            }
            "--killed-boot-id" => {
                let value = values.next().context("--killed-boot-id requires a value")?;
                killed_boot_id = Some(BootIdentity::parse(value)?);
            }
            "--consumer" => {
                consumer = Some(values.next().context("--consumer requires a value")?);
            }
            "--after-stream-id" => {
                let value = values
                    .next()
                    .context("--after-stream-id requires a value")?;
                server::recovery::validate_stream_id(&value)
                    .context("invalid --after-stream-id")?;
                after_stream_id = Some(value);
            }
            "--watch-interval-ms" => {
                let value = values
                    .next()
                    .context("--watch-interval-ms requires a value")?;
                watch_interval_ms = Some(parse_bounded_milliseconds(
                    "--watch-interval-ms",
                    &value,
                    MIN_WATCH_INTERVAL_MS,
                    MAX_WATCH_INTERVAL_MS,
                )?);
            }
            "--wait-ms" => {
                let value = values.next().context("--wait-ms requires a value")?;
                wait_ms = Some(parse_bounded_milliseconds(
                    "--wait-ms",
                    &value,
                    1,
                    MAX_OUTPUT_WAIT_MS,
                )?);
            }
            "--baseline-status" => {
                baseline_status_path = Some(PathBuf::from(
                    values
                        .next()
                        .context("--baseline-status requires a value")?,
                ));
            }
            "--anchor-output" => {
                anchor_output_path = Some(PathBuf::from(
                    values.next().context("--anchor-output requires a value")?,
                ));
            }
            "--ready-output" => {
                ready_output_path = Some(PathBuf::from(
                    values.next().context("--ready-output requires a value")?,
                ));
            }
            "-h" | "--help" => bail!(usage()),
            other => bail!("unknown argument {other:?}\n{}", usage()),
        }
    }
    let operation = match operation.as_str() {
        "status" => {
            if killed_boot_id.is_some()
                || consumer.is_some()
                || after_stream_id.is_some()
                || watch_interval_ms.is_some()
                || wait_ms.is_some()
                || baseline_status_path.is_some()
                || ready_output_path.is_some()
                || anchor_output_path.is_some()
            {
                bail!(
                    "fault-proof arguments are not valid for status\n{}",
                    usage()
                );
            }
            Operation::Status { partition }
        }
        "envelope" => {
            if partition.is_some()
                || killed_boot_id.is_some()
                || consumer.is_some()
                || after_stream_id.is_some()
                || watch_interval_ms.is_some()
                || wait_ms.is_some()
                || baseline_status_path.is_some()
                || ready_output_path.is_some()
                || anchor_output_path.is_some()
            {
                bail!(
                    "envelope accepts only its documented arguments\n{}",
                    usage()
                );
            }
            Operation::Envelope
        }
        "ownership" => {
            if consumer.is_some()
                || after_stream_id.is_some()
                || wait_ms.is_some()
                || baseline_status_path.is_some()
                || ready_output_path.is_some()
                || anchor_output_path.is_some()
            {
                bail!(
                    "ownership accepts only its documented arguments\n{}",
                    usage()
                );
            }
            Operation::Ownership {
                partition: partition.context("ownership requires --partition")?,
                killed_boot_id: killed_boot_id.context("ownership requires --killed-boot-id")?,
            }
        }
        "pending" => {
            if killed_boot_id.is_some()
                || after_stream_id.is_some()
                || wait_ms.is_some()
                || baseline_status_path.is_some()
                || ready_output_path.is_some()
                || anchor_output_path.is_some()
            {
                bail!("pending accepts only its documented arguments\n{}", usage());
            }
            let consumer = consumer.context("pending requires --consumer")?;
            if parse_active_owner(&consumer).is_none() {
                bail!("--consumer must be a fenced executor lease token");
            }
            Operation::Pending {
                partition: partition.context("pending requires --partition")?,
                consumer,
            }
        }
        "output" => {
            if killed_boot_id.is_some()
                || consumer.is_some()
                || watch_interval_ms.is_some()
                || baseline_status_path.is_some()
                || ready_output_path.is_some()
                || anchor_output_path.is_some()
            {
                bail!("output accepts only its documented arguments\n{}", usage());
            }
            Operation::Output {
                partition: partition.context("output requires --partition")?,
                after_stream_id: after_stream_id.context("output requires --after-stream-id")?,
            }
        }
        "takeover-output" => {
            if consumer.is_some()
                || after_stream_id.is_some()
                || watch_interval_ms.is_some()
                || wait_ms.is_some()
            {
                bail!(
                    "takeover-output accepts only its documented arguments\n{}",
                    usage()
                );
            }
            Operation::TakeoverOutput {
                partition: partition.context("takeover-output requires --partition")?,
                killed_boot_id: killed_boot_id
                    .context("takeover-output requires --killed-boot-id")?,
                baseline_status_path: baseline_status_path
                    .context("takeover-output requires --baseline-status")?,
                ready_output_path: ready_output_path
                    .context("takeover-output requires --ready-output")?,
                anchor_output_path: anchor_output_path
                    .context("takeover-output requires --anchor-output")?,
            }
        }
        _ => bail!(usage()),
    };
    Ok(Args {
        region_key: region_key.context("--region-key is required")?,
        redis_url: redis_url
            .or(redis_url_from_env)
            .context("--redis-url or SNAKETRON_REDIS_URL is required")?,
        watch_interval_ms,
        wait_ms,
        operation,
    })
}

fn parse_args() -> Result<Args> {
    parse_args_from(env::args().skip(1), env::var("SNAKETRON_REDIS_URL").ok())
}

async fn read_live_members(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    now_ms: i64,
) -> Result<Vec<TaskMembership>> {
    let ids: Vec<String> = redis
        .zrangebyscore(namespace.members(), now_ms.saturating_add(1), "+inf")
        .await
        .context("failed to read live membership index")?;
    let mut members = Vec::with_capacity(ids.len());
    for id in ids {
        let Ok(boot_id) = BootIdentity::parse(id) else {
            continue;
        };
        let payload: Option<Vec<u8>> = redis
            .get(namespace.member(&boot_id))
            .await
            .context("failed to read membership document")?;
        let Some(payload) = payload else {
            continue;
        };
        let member: TaskMembership =
            serde_json::from_slice(&payload).context("malformed live membership document")?;
        if member.expires_at_ms > now_ms {
            members.push(member);
        }
    }
    members.sort_by(|left, right| left.boot_id.cmp(&right.boot_id));
    Ok(members)
}

fn parse_active_owner(token: &str) -> Option<String> {
    let (boot_id, acquisition_id) = token.split_once(':')?;
    BootIdentity::parse(boot_id).ok()?;
    Uuid::parse_str(acquisition_id).ok()?;
    Some(boot_id.to_string())
}

async fn read_partition(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    assignment: Option<&AssignmentDocument>,
    partition: u32,
) -> Result<RuntimePartition> {
    let lease_key = namespace.partition_lease(partition);
    let lease_token: Option<String> = redis
        .get(&lease_key)
        .await
        .context("failed to read partition lease")?;
    let lease_ttl_ms: i64 = redis::cmd("PTTL")
        .arg(&lease_key)
        .query_async(redis)
        .await
        .context("failed to read partition lease TTL")?;
    let active_owner = lease_token.as_deref().and_then(parse_active_owner);
    let desired_owner = assignment
        .and_then(|document| document.desired_owner(partition))
        .map(ToString::to_string);

    let stream = RedisKeys::stream_commands(partition);
    let group = namespace.command_group(partition);
    let (consumer_group_exists, pending_count) = match redis
        .xpending::<_, _, StreamPendingReply>(&stream, &group)
        .await
    {
        Ok(StreamPendingReply::Data(pending)) => (true, pending.count as u64),
        Ok(StreamPendingReply::Empty) => (true, 0),
        Err(error) if error.to_string().contains("NOGROUP") => (false, 0),
        Err(error) => return Err(error).context("failed to inspect executor pending entries"),
    };
    let pending_entry_sample = if consumer_group_exists && pending_count > 0 {
        let pending: StreamPendingCountReply = redis
            .xpending_count(&stream, &group, "-", "+", PENDING_ENTRY_SAMPLE_LIMIT)
            .await
            .context("failed to inspect exact executor pending entries")?;
        pending.ids.into_iter().map(PendingEntry::from).collect()
    } else {
        Vec::new()
    };

    let pending_completion_count: u64 = redis
        .scard(namespace.pending_completions(partition))
        .await
        .context("failed to inspect pending completions")?;
    let quarantined_command_count: u64 = redis
        .xlen(namespace.command_quarantine(partition))
        .await
        .context("failed to inspect quarantined commands")?;
    let active_games: u64 = redis
        .scard(namespace.active_games(partition))
        .await
        .context("failed to inspect active games")?;

    Ok(RuntimePartition {
        partition,
        owner_matches: desired_owner.is_some() && desired_owner == active_owner && lease_ttl_ms > 0,
        desired_owner,
        active_owner,
        lease_token,
        lease_ttl_ms,
        consumer_group_exists,
        pending_count,
        pending_entry_sample,
        pending_completion_count,
        quarantined_command_count,
        active_games,
    })
}

async fn read_envelope_partition(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    assignment: Option<&AssignmentDocument>,
    partition: u32,
) -> Result<EnvelopeRuntimePartition> {
    // These keys share the executor partition hash slot. One small script
    // gives the certification harness the authoritative game count and the
    // corresponding live lease without scanning pending command metadata.
    let (lease_token, lease_ttl_ms, active_games): (String, i64, u64) = redis::Script::new(
        r#"
            local lease = redis.call('GET', KEYS[1])
            return {
                lease or '',
                redis.call('PTTL', KEYS[1]),
                redis.call('SCARD', KEYS[2])
            }
            "#,
    )
    .key(namespace.partition_lease(partition))
    .key(namespace.active_games(partition))
    .invoke_async(redis)
    .await
    .context("failed to inspect executor capacity envelope")?;
    let active_owner = parse_active_owner(&lease_token);
    let desired_owner = assignment
        .and_then(|document| document.desired_owner(partition))
        .map(ToString::to_string);

    Ok(EnvelopeRuntimePartition {
        partition,
        owner_matches: desired_owner.is_some() && desired_owner == active_owner && lease_ttl_ms > 0,
        desired_owner,
        active_owner,
        lease_ttl_ms,
        active_games,
    })
}

async fn read_envelope_status(
    connection: &RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    region_key: String,
) -> Result<EnvelopeStatus> {
    let observation_started_at_ms = observer_clock.observation_start_ms();
    let now_ms = Utc::now().timestamp_millis();
    let assignment_store = AssignmentStore::new(connection.clone(), namespace.clone());
    let mut membership_redis = connection.clone();
    let (assignment, live_members) = tokio::try_join!(
        assignment_store.load(),
        read_live_members(&mut membership_redis, namespace, now_ms),
    )?;
    let assignment_ref = assignment.as_ref();
    let partition_reads =
        (0..PARTITION_COUNT).map(|partition| {
            let mut redis = connection.clone();
            async move {
                read_envelope_partition(&mut redis, namespace, assignment_ref, partition).await
            }
        });
    let runtime_partitions = futures_util::future::try_join_all(partition_reads).await?;

    Ok(EnvelopeStatus {
        region_key,
        observation_started_at_ms,
        observation_completed_at_ms: observer_clock.observation_completion_ms(),
        live_members,
        assignment,
        runtime_partitions,
    })
}

async fn read_authority_snapshot(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    partition: u32,
) -> Result<AuthoritySnapshot> {
    let (assignment_payload, lease_token, lease_ttl_ms, observed_at_ms, event_tail_id): (
        Vec<u8>,
        String,
        i64,
        i64,
        String,
    ) = redis::Script::new(
        r#"
            local assignment = redis.call('GET', KEYS[1])
            local lease = redis.call('GET', KEYS[2])
            local tail = redis.call('XREVRANGE', KEYS[3], '+', '-', 'COUNT', 1)
            local tail_id = '0-0'
            if #tail > 0 then tail_id = tail[1][1] end
            local now = redis.call('TIME')
            local now_ms =
                tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
            return {
                assignment or '',
                lease or '',
                redis.call('PTTL', KEYS[2]),
                now_ms,
                tail_id
            }
            "#,
    )
    .key(namespace.partition_assignment(partition))
    .key(namespace.partition_lease(partition))
    .key(RedisKeys::stream_events(partition))
    .invoke_async(redis)
    .await
    .context("failed to atomically inspect partition assignment and lease")?;
    Ok(AuthoritySnapshot {
        assignment_payload,
        lease_token,
        lease_ttl_ms,
        observed_at_ms,
        event_tail_id,
    })
}

fn authority_is_stable(before: &AuthoritySnapshot, after: &AuthoritySnapshot) -> bool {
    !before.assignment_payload.is_empty()
        && before.assignment_payload == after.assignment_payload
        && !before.lease_token.is_empty()
        && before.lease_token == after.lease_token
        && before.lease_ttl_ms > 0
        && after.lease_ttl_ms > 0
        && before.observed_at_ms <= after.observed_at_ms
}

fn decode_live_member(
    payload: &[u8],
    expected_boot_id: &BootIdentity,
    observed_at_ms: i64,
) -> Result<Option<TaskMembership>> {
    if payload.is_empty() {
        return Ok(None);
    }
    let member: TaskMembership =
        serde_json::from_slice(payload).context("malformed live membership document")?;
    if member.schema_version != MEMBERSHIP_SCHEMA_VERSION {
        bail!("unsupported membership schema version");
    }
    if member.boot_id != *expected_boot_id {
        bail!(
            "membership document boot ID {} does not match key {}",
            member.boot_id,
            expected_boot_id
        );
    }
    if member.expires_at_ms <= observed_at_ms {
        return Ok(None);
    }
    Ok(Some(member))
}

async fn read_ownership_status(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    killed_boot_id: &BootIdentity,
) -> Result<OwnershipStatus> {
    // Assignment and lease live in one partition hash slot. Membership lives
    // in a separate regional slot, so one cluster-wide Lua snapshot is not
    // possible. Read authority before and after the atomic membership read.
    // Assignment versions are monotonic and lease acquisition IDs are unique;
    // equal values on both sides therefore prove authority was unchanged
    // throughout the membership observation.
    let observation_started_at_ms = observer_clock.observation_start_ms();
    let before = read_authority_snapshot(redis, namespace, partition).await?;
    let assignment: AssignmentDocument = serde_json::from_slice(&before.assignment_payload)
        .context("partition assignment is missing or malformed")?;
    assignment.validate(PARTITION_COUNT)?;
    if assignment.region != namespace.region() {
        bail!("partition assignment region does not match its key namespace");
    }
    let desired_owner = assignment
        .desired_owner(partition)
        .cloned()
        .context("partition assignment has no desired owner")?;

    let (membership_observed_at_ms, killed_payload, owner_payload): (i64, Vec<u8>, Vec<u8>) =
        redis::Script::new(
            r#"
            local now = redis.call('TIME')
            local now_ms =
                tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

            local function live_payload(member_key, boot_id)
                local score = redis.call('ZSCORE', KEYS[1], boot_id)
                if not score or tonumber(score) <= now_ms then return '' end
                return redis.call('GET', member_key) or ''
            end

            return {
                now_ms,
                live_payload(KEYS[2], ARGV[1]),
                live_payload(KEYS[3], ARGV[2])
            }
            "#,
        )
        .key(namespace.members())
        .key(namespace.member(killed_boot_id))
        .key(namespace.member(&desired_owner))
        .arg(killed_boot_id.as_str())
        .arg(desired_owner.as_str())
        .invoke_async(redis)
        .await
        .context("failed to atomically inspect killed and owner membership")?;

    let killed_member =
        decode_live_member(&killed_payload, killed_boot_id, membership_observed_at_ms)?;
    let owner_member =
        decode_live_member(&owner_payload, &desired_owner, membership_observed_at_ms)?
            .filter(|member| member.is_assignment_eligible(membership_observed_at_ms));
    let after = read_authority_snapshot(redis, namespace, partition).await?;
    let observation_completed_at_ms = observer_clock.observation_completion_ms();
    let captured_at_ms = observation_completed_at_ms;
    let authority_observed_at_ms = after.observed_at_ms;
    let authority_event_tail_id = after.event_tail_id.clone();
    let authority_stable = authority_is_stable(&before, &after);
    let active_owner = parse_active_owner(&before.lease_token);
    let lease_ttl_ms = before.lease_ttl_ms.min(after.lease_ttl_ms);
    let lease_token = (!before.lease_token.is_empty()).then_some(before.lease_token);
    let desired_owner = desired_owner.to_string();

    Ok(OwnershipStatus {
        region_key,
        observation_started_at_ms,
        observation_completed_at_ms,
        captured_at_ms,
        membership_observed_at_ms,
        authority_observed_at_ms,
        authority_event_tail_id,
        authority_stable,
        killed_member_live: killed_member.is_some(),
        live_members: owner_member.into_iter().collect(),
        assignment,
        runtime_partitions: vec![OwnershipRuntimePartition {
            partition,
            owner_matches: authority_stable
                && active_owner.as_deref() == Some(desired_owner.as_str())
                && lease_ttl_ms > 0,
            desired_owner,
            active_owner,
            lease_token,
            lease_ttl_ms,
        }],
    })
}

async fn read_output_status(
    redis: &mut RedisConnection,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    after_stream_id: String,
) -> Result<OutputStatus> {
    let observation_started_at_ms = observer_clock.observation_start_ms();
    let after_id = server::recovery::validate_stream_id(&after_stream_id)?;
    let stream = RedisKeys::stream_events(partition);
    let redis_observation_started_at_ms: i64 = redis::Script::new(
        r#"
        local now = redis.call('TIME')
        return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
        "#,
    )
    .key(&stream)
    .invoke_async(redis)
    .await
    .context("failed to capture authoritative-output observation start")?;
    let mut cursor = after_stream_id.clone();
    let mut scanned = 0;
    let mut first_scheduled_output = None;
    'pages: while scanned < OUTPUT_SCAN_LIMIT {
        let page_size = OUTPUT_SCAN_PAGE_SIZE.min(OUTPUT_SCAN_LIMIT - scanned);
        let start = format!("({cursor}");
        let entries: StreamRangeReply =
            redis
                .xrange_count(&stream, &start, "+", page_size)
                .await
                .context("failed to inspect authoritative partition output")?;
        let entry_count = entries.ids.len();
        if entry_count == 0 {
            break;
        }
        for entry in entries.ids {
            scanned += 1;
            cursor.clone_from(&entry.id);
            let stream_id = server::recovery::validate_stream_id(&entry.id)?;
            if stream_id <= after_id {
                bail!("Valkey returned an event at or before the output anchor");
            }
            let stream_unix_ms = stream_id.0;
            let stream_unix_ms =
                i64::try_from(stream_unix_ms).context("event stream timestamp exceeds i64")?;
            let data = entry.map.get("data");
            let barrier = entry.map.get("snapshot_barrier");
            let payload = match (data, barrier, entry.map.len()) {
                (None, Some(barrier), 1) => {
                    let completion_id = redis::from_redis_value::<String>(barrier)
                        .context("authoritative snapshot barrier is not a string")?;
                    Uuid::parse_str(&completion_id)
                        .context("authoritative snapshot barrier ID is not a UUID")?;
                    continue;
                }
                (Some(data), None, 1) => data,
                _ => bail!("authoritative event stream entry has an unknown field shape"),
            };
            let payload = redis::from_redis_value::<Vec<u8>>(payload)
                .context("authoritative event stream data is not binary-safe")?;
            let event: GameEventMessage = serde_json::from_slice(&payload)
                .context("authoritative event stream contains malformed JSON")?;
            if event.game_id % PARTITION_COUNT != partition {
                bail!("authoritative event is stored under the wrong partition");
            }
            if let GameEvent::CommandScheduledV2 {
                command_id,
                deduplicated_replay,
                ..
            } = event.event
            {
                server::recovery::validate_client_command_identity(&command_id)?;
                if event.stream_seq == 0
                    || command_id.game_id != event.game_id
                    || event.user_id.is_some()
                {
                    bail!("authoritative scheduled output has inconsistent identity");
                }
                if deduplicated_replay {
                    continue;
                }
                first_scheduled_output = Some(AuthoritativeOutput {
                    stream_id: entry.id,
                    stream_unix_ms,
                    game_id: event.game_id,
                    command_id,
                    deduplicated_replay,
                });
                break 'pages;
            }
        }
        if entry_count < page_size {
            break;
        }
    }

    // Route TIME through the event-stream slot so this diagnostic timestamp
    // and Redis-generated stream IDs use the same Valkey shard clock.
    let redis_observation_completed_at_ms: i64 = redis::Script::new(
        r#"
        local now = redis.call('TIME')
        return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
        "#,
    )
    .key(&stream)
    .invoke_async(redis)
    .await
    .context("failed to capture authoritative-output observation time")?;
    let observation_completed_at_ms = observer_clock.observation_completion_ms();

    Ok(OutputStatus {
        region_key,
        observation_started_at_ms,
        observation_completed_at_ms,
        captured_at_ms: observation_completed_at_ms,
        redis_observation_started_at_ms,
        redis_observation_completed_at_ms,
        partition,
        after_stream_id,
        first_scheduled_output,
    })
}

async fn read_pending_status(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    requested_consumer: String,
) -> Result<PendingStatus> {
    let observation_started_at_ms = observer_clock.observation_start_ms();
    // Redis TIME still brackets XPENDING for payload correlation. Acceptance
    // timing uses the calibrated host-monotonic interval around this script;
    // Valkey, the observer host, and ECS are not treated as one clock.
    let (
        redis_observation_started_at_ms,
        redis_observation_completed_at_ms,
        entry_id,
        entry_consumer,
        idle_ms,
        delivery_count,
    ): (i64, i64, String, String, i64, i64) = redis::Script::new(
        r#"
        local started = redis.call('TIME')
        local started_ms =
            tonumber(started[1]) * 1000 + math.floor(tonumber(started[2]) / 1000)
        local pending =
            redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2])
        local completed = redis.call('TIME')
        local completed_ms =
            tonumber(completed[1]) * 1000
                + math.floor(tonumber(completed[2]) / 1000)
        if #pending == 0 then
            return {started_ms, completed_ms, '', '', -1, -1}
        end
        return {
            started_ms,
            completed_ms,
            pending[1][1],
            pending[1][2],
            pending[1][3],
            pending[1][4]
        }
        "#,
    )
    .key(RedisKeys::stream_commands(partition))
    .arg(namespace.command_group(partition))
    .arg(&requested_consumer)
    .invoke_async(redis)
    .await
    .context("failed to atomically inspect exact executor pending entry")?;
    let observation_completed_at_ms = observer_clock.observation_completion_ms();

    let pending_entry = if entry_id.is_empty() {
        None
    } else {
        if entry_consumer != requested_consumer || idle_ms < 0 || delivery_count < 0 {
            bail!("Valkey returned malformed exact executor pending metadata");
        }
        Some(PendingEntry {
            id: entry_id,
            consumer: entry_consumer,
            idle_ms: idle_ms as u64,
            delivery_count: delivery_count as u64,
        })
    };

    Ok(PendingStatus {
        region_key,
        observation_started_at_ms,
        observation_completed_at_ms,
        captured_at_ms: observation_completed_at_ms,
        redis_observation_started_at_ms,
        redis_observation_completed_at_ms,
        partition,
        requested_consumer,
        pending_entry,
    })
}

fn write_json_line<T: Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, value).context("failed to serialize watch sample")?;
    stdout
        .write_all(b"\n")
        .context("failed to terminate watch sample")?;
    stdout.flush().context("failed to flush watch sample")
}

async fn watch_ownership_status(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    killed_boot_id: &BootIdentity,
    watch_interval_ms: u64,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(watch_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let status = read_ownership_status(
            redis,
            namespace,
            observer_clock,
            region_key.clone(),
            partition,
            killed_boot_id,
        )
        .await?;
        write_json_line(&status)?;
    }
}

async fn watch_pending_status(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    requested_consumer: String,
    watch_interval_ms: u64,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(watch_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let status = read_pending_status(
            redis,
            namespace,
            observer_clock,
            region_key.clone(),
            partition,
            requested_consumer.clone(),
        )
        .await?;
        write_json_line(&status)?;
    }
}

async fn wait_for_output_status(
    redis: &mut RedisConnection,
    observer_clock: &ObserverClock,
    region_key: String,
    partition: u32,
    after_stream_id: String,
    wait_ms: u64,
) -> Result<OutputStatus> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    let mut last_status = None;
    loop {
        let read = read_output_status(
            redis,
            observer_clock,
            region_key.clone(),
            partition,
            after_stream_id.clone(),
        );
        let status = match tokio::time::timeout_at(deadline, read).await {
            Ok(status) => status?,
            Err(_) => {
                return last_status.context("output wait expired before the first bounded read");
            }
        };
        if status.first_scheduled_output.is_some() {
            return Ok(status);
        }
        last_status = Some(status);

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(last_status.expect("the output status was just assigned"));
        }
        tokio::time::sleep_until(deadline.min(now + OUTPUT_WAIT_POLL_INTERVAL)).await;
    }
}

fn load_takeover_criteria(
    path: &Path,
    region_key: &str,
    partition: u32,
    killed_boot_id: BootIdentity,
) -> Result<TakeoverCriteria> {
    let payload = fs::read(path)
        .with_context(|| format!("failed to read takeover baseline {}", path.display()))?;
    let baseline: TakeoverBaseline =
        serde_json::from_slice(&payload).context("takeover baseline is malformed")?;
    baseline.assignment.validate(PARTITION_COUNT)?;
    if baseline.assignment.region != region_key {
        bail!("takeover baseline assignment belongs to another region");
    }
    if baseline.assignment.desired_owner(partition) != Some(&killed_boot_id) {
        bail!("takeover baseline partition is not assigned to the killed owner");
    }

    let mut runtime = baseline
        .runtime_partitions
        .iter()
        .filter(|runtime| runtime.partition == partition);
    let old_runtime = runtime
        .next()
        .context("takeover baseline is missing the selected partition")?;
    if runtime.next().is_some() {
        bail!("takeover baseline repeats the selected partition");
    }
    let killed_lease_token = old_runtime
        .lease_token
        .clone()
        .context("takeover baseline is missing the killed lease")?;
    if parse_active_owner(&killed_lease_token).as_deref() != Some(killed_boot_id.as_str()) {
        bail!("takeover baseline lease is not owned by the killed member");
    }

    let mut killed_members = baseline.live_members.iter().filter(|member| {
        member.boot_id == killed_boot_id && member.lifecycle == TaskLifecycle::Active
    });
    let killed_member = killed_members
        .next()
        .context("takeover baseline is missing the ACTIVE killed member")?;
    if killed_members.next().is_some() {
        bail!("takeover baseline repeats the ACTIVE killed member");
    }
    let killed_task_id = killed_member
        .ecs_task_id
        .clone()
        .context("takeover baseline killed member has no ECS task ID")?;

    let mut preexisting_tasks = BTreeMap::new();
    for member in baseline.live_members {
        let Some(task_id) = member.ecs_task_id else {
            continue;
        };
        if preexisting_tasks.insert(member.boot_id, task_id).is_some() {
            bail!("takeover baseline repeats a boot identity");
        }
    }

    Ok(TakeoverCriteria {
        assignment_version: baseline.assignment.version,
        killed_boot_id,
        killed_task_id,
        killed_lease_token,
        preexisting_tasks,
    })
}

fn is_takeover_successor(status: &OwnershipStatus, criteria: &TakeoverCriteria) -> bool {
    if !status.authority_stable
        || status.killed_member_live
        || status.assignment.version <= criteria.assignment_version
        || status.runtime_partitions.len() != 1
        || status.live_members.len() != 1
    {
        return false;
    }
    let runtime = &status.runtime_partitions[0];
    let Some(active_owner) = runtime.active_owner.as_deref() else {
        return false;
    };
    let Some(lease_token) = runtime.lease_token.as_deref() else {
        return false;
    };
    if !runtime.owner_matches
        || runtime.lease_ttl_ms <= 0
        || active_owner != runtime.desired_owner
        || active_owner == criteria.killed_boot_id.as_str()
        || lease_token.is_empty()
        || lease_token == criteria.killed_lease_token
        || parse_active_owner(lease_token).as_deref() != Some(active_owner)
    {
        return false;
    }

    let member = &status.live_members[0];
    let Some(task_id) = member.ecs_task_id.as_deref() else {
        return false;
    };
    member.lifecycle == TaskLifecycle::Active
        && member.boot_id.as_str() == active_owner
        && task_id != criteria.killed_task_id
        && criteria
            .preexisting_tasks
            .get(&member.boot_id)
            .is_some_and(|preexisting_task| preexisting_task == task_id)
}

fn is_pre_fault_owner(status: &OwnershipStatus, criteria: &TakeoverCriteria) -> bool {
    if !status.authority_stable
        || !status.killed_member_live
        || status.assignment.version != criteria.assignment_version
        || status.runtime_partitions.len() != 1
        || status.live_members.len() != 1
    {
        return false;
    }
    let runtime = &status.runtime_partitions[0];
    let member = &status.live_members[0];
    runtime.owner_matches
        && runtime.lease_ttl_ms > 0
        && runtime.desired_owner == criteria.killed_boot_id.as_str()
        && runtime.active_owner.as_deref() == Some(criteria.killed_boot_id.as_str())
        && runtime.lease_token.as_deref() == Some(criteria.killed_lease_token.as_str())
        && member.lifecycle == TaskLifecycle::Active
        && member.boot_id == criteria.killed_boot_id
        && member.ecs_task_id.as_deref() == Some(criteria.killed_task_id.as_str())
}

fn publish_ownership_sample_atomically(path: &Path, status: &OwnershipStatus) -> Result<()> {
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".{}.pending", std::process::id()));
    let temporary_path = PathBuf::from(temporary_name);
    let publish = || -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .with_context(|| {
                format!(
                    "failed to create takeover ownership sample candidate {}",
                    temporary_path.display()
                )
            })?;
        serde_json::to_writer_pretty(&mut file, status)
            .context("failed to serialize takeover ownership sample")?;
        file.write_all(b"\n")
            .context("failed to terminate takeover ownership sample")?;
        file.sync_all()
            .context("failed to flush takeover ownership sample")?;
        fs::hard_link(&temporary_path, path).with_context(|| {
            format!(
                "failed to publish immutable takeover ownership sample {}",
                path.display()
            )
        })?;
        Ok(())
    };
    let result = publish();
    let _ = fs::remove_file(&temporary_path);
    result
}

async fn watch_takeover_output(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    observer_clock: &ObserverClock,
    request: TakeoverOutputRequest,
) -> Result<()> {
    let criteria = load_takeover_criteria(
        &request.baseline_status_path,
        &request.region_key,
        request.partition,
        request.killed_boot_id.clone(),
    )?;
    let mut interval = tokio::time::interval(OUTPUT_WAIT_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ready_published = false;
    let after_stream_id = loop {
        interval.tick().await;
        let status = read_ownership_status(
            redis,
            namespace,
            observer_clock,
            request.region_key.clone(),
            request.partition,
            &request.killed_boot_id,
        )
        .await?;
        if !ready_published && is_pre_fault_owner(&status, &criteria) {
            publish_ownership_sample_atomically(&request.ready_output_path, &status)?;
            ready_published = true;
        }
        if is_takeover_successor(&status, &criteria) {
            if !ready_published {
                bail!("takeover successor appeared before pre-fault readiness");
            }
            let after_stream_id = status.authority_event_tail_id.clone();
            publish_ownership_sample_atomically(&request.anchor_output_path, &status)?;
            break after_stream_id;
        }
    };

    let mut interval = tokio::time::interval(OUTPUT_WAIT_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let status = read_output_status(
            redis,
            observer_clock,
            request.region_key.clone(),
            request.partition,
            after_stream_id.clone(),
        )
        .await?;
        let observed_output = status.first_scheduled_output.is_some();
        write_json_line(&status)?;
        if observed_output {
            std::future::pending::<()>().await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let namespace = ClusterNamespace::new(args.region_key.clone())?;
    let observer_clock = ObserverClock::calibrate();
    let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
    let client =
        RedisClient::open(args.redis_url.as_str(), Some(push_tx)).context("invalid Redis URL")?;
    let connection = client
        .get_managed_connection()
        .await
        .context("failed to connect to Valkey")?;
    match args.operation {
        Operation::Status { partition } => {
            let assignment = AssignmentStore::new(connection.clone(), namespace.clone())
                .load()
                .await?;
            let now_ms = Utc::now().timestamp_millis();
            let mut redis = connection;
            let live_members = read_live_members(&mut redis, &namespace, now_ms).await?;
            let mut quickmatch_two_v_two_queued_lobbies = 0_u64;
            for matchmaking_pool in server::matchmaking_pool::MatchmakingPool::ALL {
                let pool_queue_count: u64 = redis
                    .zcard(RedisKeys::matchmaking_lobby_queue_for_pool(
                        &GameType::TeamMatch { per_team: 2 },
                        &QueueMode::Quickmatch,
                        matchmaking_pool,
                    ))
                    .await
                    .context("failed to inspect a quickmatch 2v2 pool queue")?;
                quickmatch_two_v_two_queued_lobbies += pool_queue_count;
            }
            let partitions = partition.map_or_else(
                || (0..PARTITION_COUNT).collect(),
                |partition| vec![partition],
            );
            let mut runtime_partitions = Vec::with_capacity(partitions.len());
            for partition in partitions {
                runtime_partitions.push(
                    read_partition(&mut redis, &namespace, assignment.as_ref(), partition).await?,
                );
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&Status {
                    region_key: args.region_key,
                    captured_at_ms: now_ms,
                    live_members,
                    assignment,
                    runtime_partitions,
                    quickmatch_two_v_two_queued_lobbies,
                })?
            );
        }
        Operation::Envelope => {
            let status =
                read_envelope_status(&connection, &namespace, &observer_clock, args.region_key)
                    .await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Operation::Ownership {
            partition,
            killed_boot_id,
        } => {
            let mut redis = connection;
            if let Some(watch_interval_ms) = args.watch_interval_ms {
                watch_ownership_status(
                    &mut redis,
                    &namespace,
                    &observer_clock,
                    args.region_key,
                    partition,
                    &killed_boot_id,
                    watch_interval_ms,
                )
                .await?;
            } else {
                let status = read_ownership_status(
                    &mut redis,
                    &namespace,
                    &observer_clock,
                    args.region_key,
                    partition,
                    &killed_boot_id,
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
        }
        Operation::Pending {
            partition,
            consumer,
        } => {
            let mut redis = connection;
            if let Some(watch_interval_ms) = args.watch_interval_ms {
                watch_pending_status(
                    &mut redis,
                    &namespace,
                    &observer_clock,
                    args.region_key,
                    partition,
                    consumer,
                    watch_interval_ms,
                )
                .await?;
            } else {
                let status = read_pending_status(
                    &mut redis,
                    &namespace,
                    &observer_clock,
                    args.region_key,
                    partition,
                    consumer,
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
        }
        Operation::Output {
            partition,
            after_stream_id,
        } => {
            let mut redis = connection;
            let status = if let Some(wait_ms) = args.wait_ms {
                wait_for_output_status(
                    &mut redis,
                    &observer_clock,
                    args.region_key,
                    partition,
                    after_stream_id,
                    wait_ms,
                )
                .await?
            } else {
                read_output_status(
                    &mut redis,
                    &observer_clock,
                    args.region_key,
                    partition,
                    after_stream_id,
                )
                .await?
            };
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Operation::TakeoverOutput {
            partition,
            killed_boot_id,
            baseline_status_path,
            ready_output_path,
            anchor_output_path,
        } => {
            let mut redis = connection;
            watch_takeover_output(
                &mut redis,
                &namespace,
                &observer_clock,
                TakeoverOutputRequest {
                    region_key: args.region_key,
                    partition,
                    killed_boot_id,
                    baseline_status_path,
                    ready_output_path,
                    anchor_output_path,
                },
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_valid_fenced_tokens() {
        let boot = Uuid::new_v4();
        let acquisition = Uuid::new_v4();
        assert_eq!(
            parse_active_owner(&format!("{boot}:{acquisition}")),
            Some(boot.to_string())
        );
        assert_eq!(parse_active_owner("not-a-token"), None);
        assert_eq!(parse_active_owner(&format!("{boot}:bad")), None);
    }

    #[test]
    fn preserves_exact_pending_entry_delivery_metadata() {
        let entry = PendingEntry::from(StreamPendingId {
            id: "1234-5".to_string(),
            consumer: "lease-token".to_string(),
            last_delivered_ms: 42,
            times_delivered: 3,
        });
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            serde_json::json!({
                "id": "1234-5",
                "consumer": "lease-token",
                "idle_ms": 42,
                "delivery_count": 3,
            })
        );
    }

    #[test]
    fn watch_sample_keeps_host_interval_separate_from_valkey_times() {
        let sample = PendingStatus {
            observation_started_at_ms: 10,
            observation_completed_at_ms: 12,
            region_key: "use1".to_string(),
            captured_at_ms: 11,
            redis_observation_started_at_ms: 100,
            redis_observation_completed_at_ms: 101,
            partition: 3,
            requested_consumer: "lease".to_string(),
            pending_entry: None,
        };
        assert_eq!(
            serde_json::to_value(sample).unwrap(),
            serde_json::json!({
                "observation_started_at_ms": 10,
                "observation_completed_at_ms": 12,
                "region_key": "use1",
                "captured_at_ms": 11,
                "redis_observation_started_at_ms": 100,
                "redis_observation_completed_at_ms": 101,
                "partition": 3,
                "requested_consumer": "lease",
                "pending_entry": null,
            })
        );
    }

    #[test]
    fn observer_clock_produces_a_conservative_monotonic_interval() {
        let clock = ObserverClock::calibrate();
        let started_at_ms = clock.observation_start_ms();
        std::thread::sleep(Duration::from_millis(2));
        let completed_at_ms = clock.observation_completion_ms();
        assert!(clock.unix_lower_origin_ms <= clock.unix_upper_origin_ms);
        assert!(started_at_ms <= completed_at_ms);
    }

    #[test]
    fn accepts_only_configured_partition_numbers() {
        assert_eq!(parse_partition("0").unwrap(), 0);
        assert_eq!(
            parse_partition(&(PARTITION_COUNT - 1).to_string()).unwrap(),
            PARTITION_COUNT - 1
        );
        assert!(parse_partition(&PARTITION_COUNT.to_string()).is_err());
        assert!(parse_partition("-1").is_err());
        assert!(parse_partition("not-a-number").is_err());
    }

    #[test]
    fn parses_only_exact_capacity_envelope_arguments() {
        let args = parse_args_from(
            ["envelope", "--region-key", "use1"],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.region_key, "use1");
        assert!(matches!(args.operation, Operation::Envelope));
        assert!(
            parse_args_from(
                ["envelope", "--region-key", "use1", "--partition", "0",],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn parses_ownership_probe_arguments() {
        let killed_boot_id = BootIdentity::new();
        let args = parse_args_from(
            [
                "ownership",
                "--region-key",
                "use1",
                "--partition",
                "3",
                "--killed-boot-id",
                killed_boot_id.as_str(),
                "--watch-interval-ms",
                "25",
            ],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.region_key, "use1");
        assert_eq!(args.redis_url, "redis://127.0.0.1");
        assert_eq!(args.watch_interval_ms, Some(25));
        assert_eq!(args.wait_ms, None);
        match args.operation {
            Operation::Ownership {
                partition,
                killed_boot_id: parsed,
            } => {
                assert_eq!(partition, 3);
                assert_eq!(parsed, killed_boot_id);
            }
            Operation::Status { .. }
            | Operation::Envelope
            | Operation::Pending { .. }
            | Operation::Output { .. }
            | Operation::TakeoverOutput { .. } => {
                panic!("parsed ownership as another operation")
            }
        }
    }

    #[test]
    fn parses_exact_pending_probe_arguments() {
        let boot_id = BootIdentity::new();
        let consumer = format!("{}:{}", boot_id.as_str(), Uuid::new_v4());
        let args = parse_args_from(
            [
                "pending",
                "--region-key",
                "use1",
                "--partition",
                "6",
                "--consumer",
                &consumer,
                "--watch-interval-ms",
                "50",
            ],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.watch_interval_ms, Some(50));
        assert_eq!(args.wait_ms, None);
        match args.operation {
            Operation::Pending {
                partition,
                consumer: parsed,
            } => {
                assert_eq!(partition, 6);
                assert_eq!(parsed, consumer);
            }
            Operation::Status { .. }
            | Operation::Envelope
            | Operation::Ownership { .. }
            | Operation::Output { .. }
            | Operation::TakeoverOutput { .. } => {
                panic!("parsed pending as another operation")
            }
        }
    }

    #[test]
    fn parses_bounded_authoritative_output_probe_arguments() {
        let args = parse_args_from(
            [
                "output",
                "--region-key",
                "use1",
                "--partition",
                "4",
                "--after-stream-id",
                "1000-2",
                "--wait-ms",
                "5000",
            ],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.watch_interval_ms, None);
        assert_eq!(args.wait_ms, Some(5_000));
        match args.operation {
            Operation::Output {
                partition,
                after_stream_id,
            } => {
                assert_eq!(partition, 4);
                assert_eq!(after_stream_id, "1000-2");
            }
            Operation::Status { .. }
            | Operation::Envelope
            | Operation::Ownership { .. }
            | Operation::Pending { .. }
            | Operation::TakeoverOutput { .. } => {
                panic!("parsed output as another operation")
            }
        }
    }

    #[test]
    fn parses_takeover_output_probe_arguments() {
        let killed_boot_id = BootIdentity::new();
        let args = parse_args_from(
            [
                "takeover-output",
                "--region-key",
                "use1",
                "--partition",
                "4",
                "--killed-boot-id",
                killed_boot_id.as_str(),
                "--baseline-status",
                "/tmp/pre.json",
                "--ready-output",
                "/tmp/ready.json",
                "--anchor-output",
                "/tmp/anchor.json",
            ],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.watch_interval_ms, None);
        assert_eq!(args.wait_ms, None);
        match args.operation {
            Operation::TakeoverOutput {
                partition,
                killed_boot_id: parsed,
                baseline_status_path,
                ready_output_path,
                anchor_output_path,
            } => {
                assert_eq!(partition, 4);
                assert_eq!(parsed, killed_boot_id);
                assert_eq!(baseline_status_path, PathBuf::from("/tmp/pre.json"));
                assert_eq!(ready_output_path, PathBuf::from("/tmp/ready.json"));
                assert_eq!(anchor_output_path, PathBuf::from("/tmp/anchor.json"));
            }
            Operation::Status { .. }
            | Operation::Envelope
            | Operation::Ownership { .. }
            | Operation::Pending { .. }
            | Operation::Output { .. } => panic!("parsed takeover output as another operation"),
        }
    }

    #[test]
    fn ownership_probe_requires_exact_scope() {
        assert!(
            parse_args_from(
                ["ownership", "--region-key", "use1", "--killed-boot-id",],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                ["pending", "--region-key", "use1", "--partition", "0"],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "pending",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--consumer",
                    "not-a-lease-token",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "status",
                    "--region-key",
                    "use1",
                    "--killed-boot-id",
                    Uuid::new_v4().to_string().as_str(),
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "output",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--after-stream-id",
                    "not-a-stream-id",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "pending",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--consumer",
                    &format!("{}:{}", BootIdentity::new().as_str(), Uuid::new_v4()),
                    "--watch-interval-ms",
                    "0",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "ownership",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--killed-boot-id",
                    BootIdentity::new().as_str(),
                    "--wait-ms",
                    "100",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "output",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--after-stream-id",
                    "1-0",
                    "--watch-interval-ms",
                    "25",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
        assert!(
            parse_args_from(
                [
                    "output",
                    "--region-key",
                    "use1",
                    "--partition",
                    "0",
                    "--after-stream-id",
                    "1-0",
                    "--wait-ms",
                    "5001",
                ],
                Some("redis://127.0.0.1".to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn takeover_successor_requires_a_preexisting_fenced_survivor() -> Result<()> {
        let partition = 3;
        let killed_boot_id = BootIdentity::new();
        let survivor_boot_id = BootIdentity::new();
        let killed_lease_token = format!("{}:{}", killed_boot_id, Uuid::new_v4());
        let survivor_lease_token = format!("{}:{}", survivor_boot_id, Uuid::new_v4());
        let survivor_task_id = "task-survivor".to_string();
        let mut preexisting_tasks = BTreeMap::new();
        preexisting_tasks.insert(survivor_boot_id.clone(), survivor_task_id.clone());
        let criteria = TakeoverCriteria {
            assignment_version: 41,
            killed_boot_id: killed_boot_id.clone(),
            killed_task_id: "task-killed".to_string(),
            killed_lease_token: killed_lease_token.clone(),
            preexisting_tasks,
        };
        let assignment = AssignmentDocument {
            schema_version: server::partition_assignment::ASSIGNMENT_SCHEMA_VERSION,
            version: 42,
            region: "use1".to_string(),
            computed_at_ms: 1_000,
            eligible_members: vec![survivor_boot_id.clone()],
            owners: [(partition, survivor_boot_id.clone())]
                .into_iter()
                .collect(),
        };
        let member = TaskMembership::new(
            survivor_boot_id.clone(),
            1,
            Some(survivor_task_id.clone()),
            None,
            TaskLifecycle::Active,
            1_000,
            Duration::from_secs(10),
        );
        let mut status = OwnershipStatus {
            region_key: "use1".to_string(),
            observation_started_at_ms: 1_001,
            observation_completed_at_ms: 1_002,
            captured_at_ms: 1_002,
            membership_observed_at_ms: 1_001,
            authority_observed_at_ms: 1_001,
            authority_event_tail_id: "1000-1".to_string(),
            authority_stable: true,
            killed_member_live: false,
            live_members: vec![member],
            assignment,
            runtime_partitions: vec![OwnershipRuntimePartition {
                partition,
                desired_owner: survivor_boot_id.to_string(),
                active_owner: Some(survivor_boot_id.to_string()),
                owner_matches: true,
                lease_token: Some(survivor_lease_token),
                lease_ttl_ms: 1_000,
            }],
        };

        assert!(is_takeover_successor(&status, &criteria));
        status.live_members[0].ecs_task_id = Some("task-replacement".to_string());
        assert!(!is_takeover_successor(&status, &criteria));
        status.live_members[0].ecs_task_id = Some(survivor_task_id);
        status.assignment.version = criteria.assignment_version;
        assert!(!is_takeover_successor(&status, &criteria));
        status.assignment.version += 1;
        status.runtime_partitions[0].lease_token = Some(killed_lease_token);
        assert!(!is_takeover_successor(&status, &criteria));

        status.assignment.version = criteria.assignment_version;
        status.killed_member_live = true;
        status.runtime_partitions[0].desired_owner = killed_boot_id.to_string();
        status.runtime_partitions[0].active_owner = Some(killed_boot_id.to_string());
        status.runtime_partitions[0].lease_token = Some(criteria.killed_lease_token.clone());
        status.live_members = vec![TaskMembership::new(
            killed_boot_id,
            2,
            Some(criteria.killed_task_id.clone()),
            None,
            TaskLifecycle::Active,
            1_000,
            Duration::from_secs(10),
        )];
        assert!(is_pre_fault_owner(&status, &criteria));
        status.live_members[0].ecs_task_id = Some("wrong-task".to_string());
        assert!(!is_pre_fault_owner(&status, &criteria));
        Ok(())
    }

    #[test]
    fn takeover_anchor_publish_is_atomic_and_never_replaces() -> Result<()> {
        let path = env::temp_dir().join(format!("snaketron-anchor-{}.json", Uuid::new_v4()));
        let killed_boot_id = BootIdentity::new();
        let assignment = AssignmentDocument {
            schema_version: server::partition_assignment::ASSIGNMENT_SCHEMA_VERSION,
            version: 2,
            region: "use1".to_string(),
            computed_at_ms: 1,
            eligible_members: vec![killed_boot_id.clone()],
            owners: [(0, killed_boot_id.clone())].into_iter().collect(),
        };
        let mut status = OwnershipStatus {
            region_key: "use1".to_string(),
            observation_started_at_ms: 1,
            observation_completed_at_ms: 2,
            captured_at_ms: 2,
            membership_observed_at_ms: 1,
            authority_observed_at_ms: 1,
            authority_event_tail_id: "1-0".to_string(),
            authority_stable: true,
            killed_member_live: false,
            live_members: Vec::new(),
            assignment,
            runtime_partitions: Vec::new(),
        };
        publish_ownership_sample_atomically(&path, &status)?;
        status.authority_event_tail_id = "2-0".to_string();
        assert!(publish_ownership_sample_atomically(&path, &status).is_err());
        let published: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        assert_eq!(published["authority_event_tail_id"], "1-0");
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn stable_authority_requires_identical_live_assignment_and_lease() {
        let before = AuthoritySnapshot {
            assignment_payload: br#"{"version":1}"#.to_vec(),
            lease_token: format!("{}:{}", Uuid::new_v4(), Uuid::new_v4()),
            lease_ttl_ms: 100,
            observed_at_ms: 10,
            event_tail_id: "1-0".to_string(),
        };
        let mut after = AuthoritySnapshot {
            assignment_payload: before.assignment_payload.clone(),
            lease_token: before.lease_token.clone(),
            lease_ttl_ms: 50,
            observed_at_ms: 11,
            event_tail_id: "2-0".to_string(),
        };
        assert!(authority_is_stable(&before, &after));
        after.lease_token = format!("{}:{}", Uuid::new_v4(), Uuid::new_v4());
        assert!(!authority_is_stable(&before, &after));
        after.lease_token = before.lease_token.clone();
        after.assignment_payload = br#"{"version":2}"#.to_vec();
        assert!(!authority_is_stable(&before, &after));
        after.assignment_payload = before.assignment_payload.clone();
        after.lease_ttl_ms = 0;
        assert!(!authority_is_stable(&before, &after));
        after.lease_ttl_ms = 1;
        after.observed_at_ms = 9;
        assert!(!authority_is_stable(&before, &after));
    }

    #[tokio::test]
    async fn capacity_envelope_reads_lease_and_active_games_atomically() -> Result<()> {
        let redis_url = "redis://127.0.0.1:6379/15?protocol=resp3";
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let client = RedisClient::open(redis_url, Some(push_tx))?;
        let connection = client.get_managed_connection().await?;
        let mut redis = connection;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("admin-envelope-{salt}"))?;
        let partition = PARTITION_COUNT - 1;
        let owner = BootIdentity::new();
        let lease_token = format!("{}:{}", owner.as_str(), Uuid::new_v4());
        let assignment = AssignmentDocument {
            schema_version: server::partition_assignment::ASSIGNMENT_SCHEMA_VERSION,
            version: 1,
            region: namespace.region().to_string(),
            computed_at_ms: 1,
            eligible_members: vec![owner.clone()],
            owners: [(partition, owner.clone())].into_iter().collect(),
        };
        let lease_key = namespace.partition_lease(partition);
        let active_games_key = namespace.active_games(partition);
        let _: () = redis.set_ex(&lease_key, &lease_token, 10).await?;
        let _: u64 = redis.sadd(&active_games_key, &[41_u32, 51_u32]).await?;

        let status =
            read_envelope_partition(&mut redis, &namespace, Some(&assignment), partition).await?;
        assert_eq!(status.partition, partition);
        assert_eq!(status.active_games, 2);
        assert_eq!(status.active_owner.as_deref(), Some(owner.as_str()));
        assert_eq!(status.desired_owner.as_deref(), Some(owner.as_str()));
        assert!(status.lease_ttl_ms > 0);
        assert!(status.owner_matches);

        let replacement = BootIdentity::new();
        let replacement_token = format!("{}:{}", replacement.as_str(), Uuid::new_v4());
        let _: () = redis.set_ex(&lease_key, replacement_token, 10).await?;
        let mismatch =
            read_envelope_partition(&mut redis, &namespace, Some(&assignment), partition).await?;
        assert!(!mismatch.owner_matches);

        let _: u64 = redis.del(lease_key).await?;
        let _: u64 = redis.del(active_games_key).await?;
        Ok(())
    }

    #[tokio::test]
    async fn pending_probe_reads_exact_consumer_atomically_from_live_valkey() -> Result<()> {
        let redis_url = "redis://127.0.0.1:6379/15?protocol=resp3";
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let client = RedisClient::open(redis_url, Some(push_tx))?;
        let connection = client.get_managed_connection().await?;
        let mut redis = connection;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("admin-pending-{salt}"))?;
        let partition = PARTITION_COUNT - 1;
        let stream = RedisKeys::stream_commands(partition);
        let group = namespace.command_group(partition);
        let consumer = format!("{}:{}", BootIdentity::new().as_str(), Uuid::new_v4());
        let other_consumer = format!("{}:{}", BootIdentity::new().as_str(), Uuid::new_v4());

        let _: String = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut redis)
            .await?;
        let entry_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("test")
            .arg("1")
            .query_async(&mut redis)
            .await?;
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&group)
            .arg(&consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&stream)
            .arg(">")
            .query_async(&mut redis)
            .await?;
        let (setup_seconds, setup_micros): (i64, i64) =
            redis::cmd("TIME").query_async(&mut redis).await?;
        let setup_at_ms = setup_seconds * 1_000 + setup_micros / 1_000;
        let observer_clock = ObserverClock::calibrate();

        let status = read_pending_status(
            &mut redis,
            &namespace,
            &observer_clock,
            namespace.region().to_string(),
            partition,
            consumer.clone(),
        )
        .await?;
        assert!(status.observation_started_at_ms <= status.captured_at_ms);
        assert!(status.captured_at_ms <= status.observation_completed_at_ms);
        assert!(status.redis_observation_started_at_ms >= setup_at_ms);
        assert!(status.redis_observation_started_at_ms <= status.redis_observation_completed_at_ms);
        assert_eq!(status.partition, partition);
        assert_eq!(status.requested_consumer, consumer);
        let pending = status
            .pending_entry
            .context("missing exact pending entry")?;
        assert_eq!(pending.id, entry_id);
        assert_eq!(pending.consumer, consumer);
        assert_eq!(pending.delivery_count, 1);

        let wrong_consumer = read_pending_status(
            &mut redis,
            &namespace,
            &observer_clock,
            namespace.region().to_string(),
            partition,
            other_consumer.clone(),
        )
        .await?;
        assert_eq!(wrong_consumer.requested_consumer, other_consumer);
        assert!(wrong_consumer.pending_entry.is_none());

        let _: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&stream)
            .arg(&group)
            .query_async(&mut redis)
            .await?;
        let _: i64 = redis::cmd("XDEL")
            .arg(&stream)
            .arg(&entry_id)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn output_probe_pages_past_exact_tail_and_skips_replay() -> Result<()> {
        use common::{CommandId, Direction, GameCommand, GameCommandMessage};

        let redis_url = "redis://127.0.0.1:6379/15?protocol=resp3";
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let client = RedisClient::open(redis_url, Some(push_tx))?;
        let connection = client.get_managed_connection().await?;
        let mut redis = connection;
        let partition = PARTITION_COUNT - 2;
        let stream = RedisKeys::stream_events(partition);
        let _: i64 = redis.del(&stream).await?;
        let game_id = 100 + partition;

        let event = |stream_seq, event| GameEventMessage {
            game_id,
            tick: 10,
            sequence: stream_seq,
            stream_seq,
            user_id: None,
            event,
        };
        let anchor_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("data")
            .arg(serde_json::to_vec(&event(
                1,
                GameEvent::SnakeDied { snake_id: 1 },
            ))?)
            .query_async(&mut redis)
            .await?;
        let barrier_id = Uuid::new_v4().to_string();
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("snapshot_barrier")
            .arg(&barrier_id)
            .query_async(&mut redis)
            .await?;
        for offset in 0..OUTPUT_SCAN_PAGE_SIZE {
            let stream_seq = u64::try_from(offset + 2)?;
            let _: String = redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("data")
                .arg(serde_json::to_vec(&event(
                    stream_seq,
                    GameEvent::SnakeDied { snake_id: 2 },
                ))?)
                .query_async(&mut redis)
                .await?;
        }

        let replay_command_id = ClientCommandIdentityV2 {
            game_id,
            user_id: 77,
            client_game_session_id: "output-proof-session".to_string(),
            sequence: 1,
        };
        let command_message = GameCommandMessage {
            command_id_client: CommandId {
                tick: 10,
                user_id: replay_command_id.user_id,
                sequence_number: 1,
            },
            command_id_server: Some(CommandId {
                tick: 11,
                user_id: replay_command_id.user_id,
                sequence_number: 2,
            }),
            command: GameCommand::Turn {
                snake_id: 1,
                direction: Direction::Up,
            },
        };
        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("data")
            .arg(serde_json::to_vec(&event(
                u64::try_from(OUTPUT_SCAN_PAGE_SIZE + 2)?,
                GameEvent::CommandScheduledV2 {
                    command_id: replay_command_id,
                    command_message: command_message.clone(),
                    deduplicated_replay: true,
                },
            ))?)
            .query_async(&mut redis)
            .await?;

        let command_id = ClientCommandIdentityV2 {
            game_id,
            user_id: 77,
            client_game_session_id: "output-proof-session".to_string(),
            sequence: 2,
        };
        let scheduled_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("data")
            .arg(serde_json::to_vec(&event(
                u64::try_from(OUTPUT_SCAN_PAGE_SIZE + 3)?,
                GameEvent::CommandScheduledV2 {
                    command_id: command_id.clone(),
                    command_message: command_message.clone(),
                    deduplicated_replay: false,
                },
            ))?)
            .query_async(&mut redis)
            .await?;
        let observer_clock = ObserverClock::calibrate();

        let status = read_output_status(
            &mut redis,
            &observer_clock,
            "output-proof".to_string(),
            partition,
            anchor_id.clone(),
        )
        .await?;
        assert_eq!(status.partition, partition);
        assert_eq!(status.after_stream_id, anchor_id);
        let output = status
            .first_scheduled_output
            .context("missing scheduled authoritative output")?;
        assert_eq!(output.stream_id, scheduled_id);
        assert_eq!(output.game_id, game_id);
        assert_eq!(output.command_id, command_id);
        assert!(!output.deduplicated_replay);

        let empty = read_output_status(
            &mut redis,
            &observer_clock,
            "output-proof".to_string(),
            partition,
            scheduled_id.clone(),
        )
        .await?;
        assert!(empty.first_scheduled_output.is_none());

        let delayed_command_id = ClientCommandIdentityV2 {
            game_id,
            user_id: 77,
            client_game_session_id: "output-proof-session".to_string(),
            sequence: 3,
        };
        let delayed_payload = serde_json::to_vec(&event(
            u64::try_from(OUTPUT_SCAN_PAGE_SIZE + 4)?,
            GameEvent::CommandScheduledV2 {
                command_id: delayed_command_id.clone(),
                command_message,
                deduplicated_replay: false,
            },
        ))?;
        let mut producer = redis.clone();
        let produce_output = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("data")
                .arg(delayed_payload)
                .query_async::<String>(&mut producer)
                .await
        };
        let (waited, delayed_id) = tokio::join!(
            wait_for_output_status(
                &mut redis,
                &observer_clock,
                "output-proof".to_string(),
                partition,
                scheduled_id,
                500,
            ),
            produce_output,
        );
        let delayed_id = delayed_id?;
        let delayed_output = waited?
            .first_scheduled_output
            .context("bounded output wait missed delayed scheduled output")?;
        assert_eq!(delayed_output.stream_id, delayed_id);
        assert_eq!(delayed_output.command_id, delayed_command_id);

        let _: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("unknown")
            .arg("shape")
            .query_async(&mut redis)
            .await?;
        let malformed = match read_output_status(
            &mut redis,
            &observer_clock,
            "output-proof".to_string(),
            partition,
            delayed_id,
        )
        .await
        {
            Ok(_) => panic!("unknown event-stream field shape was accepted"),
            Err(error) => error,
        };
        assert!(
            malformed.to_string().contains("unknown field shape"),
            "unexpected malformed-entry error: {malformed:#}"
        );
        let _: i64 = redis.del(&stream).await?;
        Ok(())
    }

    #[tokio::test]
    async fn authority_snapshot_atomically_includes_partition_event_tail() -> Result<()> {
        let redis_url = "redis://127.0.0.1:6379/15?protocol=resp3";
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let client = RedisClient::open(redis_url, Some(push_tx))?;
        let connection = client.get_managed_connection().await?;
        let mut redis = connection;
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("admin-authority-{salt}"))?;
        let partition = PARTITION_COUNT - 3;
        let assignment_key = namespace.partition_assignment(partition);
        let lease_key = namespace.partition_lease(partition);
        let stream = RedisKeys::stream_events(partition);
        let _: i64 = redis.del(&stream).await?;
        let assignment = br#"{"version":7}"#.to_vec();
        let lease_token = format!("{}:{}", BootIdentity::new().as_str(), Uuid::new_v4());
        let _: () = redis.set(&assignment_key, &assignment).await?;
        let _: () = redis.set_ex(&lease_key, &lease_token, 10).await?;
        let tail_id: String = redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("test")
            .arg("tail")
            .query_async(&mut redis)
            .await?;
        let (before_seconds, before_micros): (i64, i64) =
            redis::cmd("TIME").query_async(&mut redis).await?;
        let before_ms = before_seconds * 1_000 + before_micros / 1_000;

        let snapshot = read_authority_snapshot(&mut redis, &namespace, partition).await?;
        let (after_seconds, after_micros): (i64, i64) =
            redis::cmd("TIME").query_async(&mut redis).await?;
        let after_ms = after_seconds * 1_000 + after_micros / 1_000;
        assert_eq!(snapshot.assignment_payload, assignment);
        assert_eq!(snapshot.lease_token, lease_token);
        assert!(snapshot.lease_ttl_ms > 0);
        assert!(snapshot.observed_at_ms >= before_ms);
        assert!(snapshot.observed_at_ms <= after_ms);
        assert_eq!(snapshot.event_tail_id, tail_id);

        let _: i64 = redis.del(&assignment_key).await?;
        let _: i64 = redis.del(&lease_key).await?;
        let _: i64 = redis.del(&stream).await?;
        Ok(())
    }

    #[test]
    fn live_membership_decoder_rejects_wrong_identity_and_expiry() {
        use server::cluster_membership::TaskLifecycle;
        use std::time::Duration;

        let boot_id = BootIdentity::new();
        let member = TaskMembership::new(
            boot_id.clone(),
            1,
            Some("task".to_string()),
            Some("definition".to_string()),
            TaskLifecycle::Active,
            100,
            Duration::from_millis(500),
        );
        let payload = serde_json::to_vec(&member).unwrap();
        assert_eq!(
            decode_live_member(&payload, &boot_id, 599).unwrap(),
            Some(member)
        );
        assert!(
            decode_live_member(&payload, &boot_id, 600)
                .unwrap()
                .is_none()
        );
        assert!(decode_live_member(&payload, &BootIdentity::new(), 599).is_err());
        assert!(decode_live_member(&[], &boot_id, 599).unwrap().is_none());
    }
}
