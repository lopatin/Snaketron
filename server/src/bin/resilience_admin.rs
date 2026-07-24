//! Read-only control-plane inspection for steady-state executor resilience.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use common::{GameType, QueueMode};
use redis::AsyncCommands;
use redis::streams::{StreamPendingCountReply, StreamPendingId, StreamPendingReply};
use serde::Serialize;
use server::cluster_membership::{BootIdentity, ClusterNamespace, TaskMembership};
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

fn usage() -> &'static str {
    "Usage: resilience_admin status --region-key REGION [--redis-url URL]"
}

fn parse_args() -> Result<Args> {
    let mut values = env::args().skip(1);
    if values.next().as_deref() != Some("status") {
        bail!(usage());
    }
    let mut region_key = None;
    let mut redis_url = None;
    while let Some(argument) = values.next() {
        match argument.as_str() {
            "--region-key" => {
                region_key = Some(values.next().context("--region-key requires a value")?);
            }
            "--redis-url" => {
                redis_url = Some(values.next().context("--redis-url requires a value")?);
            }
            "-h" | "--help" => bail!(usage()),
            other => bail!("unknown argument {other:?}\n{}", usage()),
        }
    }
    Ok(Args {
        region_key: region_key.context("--region-key is required")?,
        redis_url: redis_url
            .or_else(|| env::var("SNAKETRON_REDIS_URL").ok())
            .context("--redis-url or SNAKETRON_REDIS_URL is required")?,
    })
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
    let mut runtime_partitions = Vec::with_capacity(PARTITION_COUNT as usize);
    for partition in 0..PARTITION_COUNT {
        runtime_partitions
            .push(read_partition(&mut redis, &namespace, assignment.as_ref(), partition).await?);
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
}
