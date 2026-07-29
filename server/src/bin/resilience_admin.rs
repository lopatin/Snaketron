//! Read-only control-plane inspection for steady-state executor resilience.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use common::{GameType, QueueMode};
use redis::AsyncCommands;
use redis::streams::{StreamPendingCountReply, StreamPendingId, StreamPendingReply};
use serde::Serialize;
use server::cluster_membership::{
    BootIdentity, ClusterNamespace, MEMBERSHIP_SCHEMA_VERSION, TaskMembership,
};
use server::game_executor::PARTITION_COUNT;
use server::partition_assignment::{AssignmentDocument, AssignmentStore};
use server::redis_keys::RedisKeys;
use server::redis_utils::{RedisClient, RedisConnection};
use std::env;
use uuid::Uuid;

const PENDING_ENTRY_SAMPLE_LIMIT: usize = 128;

#[derive(Debug)]
struct Args {
    region_key: String,
    redis_url: String,
    operation: Operation,
}

#[derive(Debug)]
enum Operation {
    Status {
        partition: Option<u32>,
    },
    Ownership {
        partition: u32,
        killed_boot_id: BootIdentity,
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

#[derive(Debug, PartialEq, Eq)]
struct AuthoritySnapshot {
    assignment_payload: Vec<u8>,
    lease_token: String,
    lease_ttl_ms: i64,
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
    captured_at_ms: i64,
    membership_observed_at_ms: i64,
    authority_stable: bool,
    killed_member_live: bool,
    owner_member: Option<TaskMembership>,
    assignment: AssignmentDocument,
    runtime_partition: OwnershipRuntimePartition,
}

fn usage() -> &'static str {
    "Usage:
  resilience_admin status --region-key REGION [--redis-url URL] [--partition NUMBER]
  resilience_admin ownership --region-key REGION [--redis-url URL] --partition NUMBER --killed-boot-id UUID"
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
            "-h" | "--help" => bail!(usage()),
            other => bail!("unknown argument {other:?}\n{}", usage()),
        }
    }
    let operation = match operation.as_str() {
        "status" => {
            if killed_boot_id.is_some() {
                bail!("--killed-boot-id is valid only for ownership\n{}", usage());
            }
            Operation::Status { partition }
        }
        "ownership" => Operation::Ownership {
            partition: partition.context("ownership requires --partition")?,
            killed_boot_id: killed_boot_id.context("ownership requires --killed-boot-id")?,
        },
        _ => bail!(usage()),
    };
    Ok(Args {
        region_key: region_key.context("--region-key is required")?,
        redis_url: redis_url
            .or(redis_url_from_env)
            .context("--redis-url or SNAKETRON_REDIS_URL is required")?,
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

async fn read_authority_snapshot(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    partition: u32,
) -> Result<AuthoritySnapshot> {
    let (assignment_payload, lease_token, lease_ttl_ms): (Vec<u8>, String, i64) =
        redis::Script::new(
            r#"
            local assignment = redis.call('GET', KEYS[1])
            local lease = redis.call('GET', KEYS[2])
            return {
                assignment or '',
                lease or '',
                redis.call('PTTL', KEYS[2])
            }
            "#,
        )
        .key(namespace.partition_assignment(partition))
        .key(namespace.partition_lease(partition))
        .invoke_async(redis)
        .await
        .context("failed to atomically inspect partition assignment and lease")?;
    Ok(AuthoritySnapshot {
        assignment_payload,
        lease_token,
        lease_ttl_ms,
    })
}

fn authority_is_stable(before: &AuthoritySnapshot, after: &AuthoritySnapshot) -> bool {
    !before.assignment_payload.is_empty()
        && before.assignment_payload == after.assignment_payload
        && !before.lease_token.is_empty()
        && before.lease_token == after.lease_token
        && before.lease_ttl_ms > 0
        && after.lease_ttl_ms > 0
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
    let captured_at_ms = Utc::now().timestamp_millis();
    let authority_stable = authority_is_stable(&before, &after);
    let active_owner = parse_active_owner(&before.lease_token);
    let lease_ttl_ms = before.lease_ttl_ms.min(after.lease_ttl_ms);
    let lease_token = (!before.lease_token.is_empty()).then_some(before.lease_token);
    let desired_owner = desired_owner.to_string();

    Ok(OwnershipStatus {
        region_key,
        captured_at_ms,
        membership_observed_at_ms,
        authority_stable,
        killed_member_live: killed_member.is_some(),
        owner_member,
        assignment,
        runtime_partition: OwnershipRuntimePartition {
            partition,
            owner_matches: authority_stable
                && active_owner.as_deref() == Some(desired_owner.as_str())
                && lease_ttl_ms > 0,
            desired_owner,
            active_owner,
            lease_token,
            lease_ttl_ms,
        },
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let namespace = ClusterNamespace::new(args.region_key.clone())?;
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
            let quickmatch_two_v_two_queued_lobbies: u64 = redis
                .zcard(RedisKeys::matchmaking_lobby_queue(
                    &GameType::TeamMatch { per_team: 2 },
                    &QueueMode::Quickmatch,
                ))
                .await
                .context("failed to inspect the quickmatch 2v2 queue")?;
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
        Operation::Ownership {
            partition,
            killed_boot_id,
        } => {
            let mut redis = connection;
            let status = read_ownership_status(
                &mut redis,
                &namespace,
                args.region_key,
                partition,
                &killed_boot_id,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
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
            ],
            Some("redis://127.0.0.1".to_string()),
        )
        .unwrap();
        assert_eq!(args.region_key, "use1");
        assert_eq!(args.redis_url, "redis://127.0.0.1");
        match args.operation {
            Operation::Ownership {
                partition,
                killed_boot_id: parsed,
            } => {
                assert_eq!(partition, 3);
                assert_eq!(parsed, killed_boot_id);
            }
            Operation::Status { .. } => panic!("parsed ownership as status"),
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
    }

    #[test]
    fn stable_authority_requires_identical_live_assignment_and_lease() {
        let before = AuthoritySnapshot {
            assignment_payload: br#"{"version":1}"#.to_vec(),
            lease_token: format!("{}:{}", Uuid::new_v4(), Uuid::new_v4()),
            lease_ttl_ms: 100,
        };
        let mut after = AuthoritySnapshot {
            assignment_payload: before.assignment_payload.clone(),
            lease_token: before.lease_token.clone(),
            lease_ttl_ms: 50,
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
