# Autoscaling resilience operations runbook

This runbook covers direct deployment and steady-state operation of the
autoscaling design in [autoscaling-resilience-prd.md](autoscaling-resilience-prd.md).
The superseded server has no users, so the first deployment intentionally uses
maintenance downtime. There is no live state migration, mixed-version mode, or
compatibility gate.

## Accepted availability boundary

- `minTasks=1` is intentional. If the sole task dies, the region is unavailable
  until ECS starts a replacement. Games recover only while their Valkey
  checkpoints remain inside `SNAKETRON_RECOVERY_RETENTION_MS`.
- Regional Valkey is one non-clustered node. A Valkey outage or loss can take
  the region down. `maxmemory-policy=noeviction` is required so memory pressure
  rejects writes visibly instead of silently deleting leases, streams,
  assignments, or checkpoints.
- Traefik/NAT remains a single ingress dependency. Its failure is outside this
  release's availability guarantee.
- A hard gateway crash necessarily drops its sockets. Clients reconnect
  automatically. Planned task removal uses make-before-break handoff and must
  maintain at least one usable authenticated socket when another ready task is
  available.
- Recovery after checkpoint retention expires is explicitly unrecoverable; the
  server must not fabricate a replacement game.

## First deployment

Take maintenance downtime, stop the superseded service, discard its ephemeral
runtime state or use an empty dedicated keyspace, and deploy the new server and
client together. No live migration or transition behavior is implemented or
tested.

For steady-state inspection after startup:

```bash
cargo run -p server --release --bin resilience_admin -- status \
  --region-key use1 \
  --redis-url redis://REGIONAL_VALKEY:6379/
```

## Local verification

With Valkey and LocalStack available, run:

```bash
(cd client/web && npx playwright install chromium)
./run_autoscaling_resilience_tests.sh
```

Local mode runs the deterministic Rust and browser tests for executor recovery,
fencing, pending entries, completion, matchmaking, and socket lifecycle, then
the load-report, production web build, parsed Traefik YAML, infrastructure
tests, and complete offline development and explicitly selected production CDK
synths. The development synth uses the same mandatory ephemeral contexts as the
certification workflow. It fails when
a required dependency is absent. The Rust suite includes a real-child-process
executor-protocol fault test using the production lease, consumer-group, and
checkpoint APIs: it SIGKILLs one incumbent and SIGSTOP/SIGCONTs another after
each has claimed a durable command, then requires a successor process to take
the expired lease, reclaim the pending entry, checkpoint and ACK it in under
five seconds. The successor acquires the production coordinator lease and
reconciles the complete ten-partition assignment before acquiring the
partition; only the initial incumbent assignment is seeded by the harness. It
also checkpoints two live games with a test-configured
60-second retention, SIGKILLs both incumbents, recovers one through a successor
process after 30 seconds, and verifies that the other produces a durable
unrecoverable marker after 61 seconds without fabricating state. The resumed
incumbent must receive the exact stale-token checkpoint rejection, and its
attempted write must leave the successor's recovery envelope unchanged. This is
a production-protocol worker, not the complete `GameExecutorV2`: it
intentionally does not boot the game actor, HTTP/WebSocket gateway, membership
heartbeat loop, or ECS. Those system boundaries require the external evidence
described below.

The same local command runs the real React client in Chromium. Its planned-drain
suite keeps the old socket active through every replacement phase and failure,
checks the visible stale/disconnected and snapshot-loading UI, and verifies one
command-owning socket. The Rust suite separately proves that group-aware command
trimming retains and reclaims a backlog beyond 8,192 pending entries, trims only
after ACK, and that the one-second checkpoint cadence and fail-closed checkpoint
age budget are independent of game tick duration. These are deterministic local
acceptance results, not additional staging fault actions.

That test is deliberately local-only and mutation-safe: it refuses non-loopback
hosts, requires dedicated Valkey database 14, serializes itself with a Redis
lock, and deletes only its exact stream and namespaced keys. To run it alone:

```bash
cargo test -p server --test executor_process_chaos_tests -- \
  --test-threads=1 --nocapture
```

## Enable autoscaling

After the one-task smoke tests and ownership inspection pass:

```bash
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/SNAKETRON_CLUSTER/SNAKETRON_SERVICE \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 1 \
  --max-capacity 10 \
  --suspended-state \
DynamicScalingInSuspended=false,DynamicScalingOutSuspended=false,ScheduledScalingSuspended=false
```

Development and production both allow a maximum of ten so the non-production
service can run the release-blocking `1 -> 10 -> 1` certification staircase.
Both retain a minimum of one. CPU 70%, memory 80%, and both 60-second cooldowns
remain unchanged.

## Routine deployments

Routine ECS deployments use the same steady-state mechanisms as autoscaling:

- a new ready task joins membership and receives a balanced desired assignment;
- executor partitions move under fenced ownership without moving WebSockets;
- a departing task becomes unready, performs bounded partition handoff, and
  requests make-before-break socket handoff;
- if SIGTERM, handoff, or any release step fails, lease expiry and pending-entry
  reclaim remain the authoritative recovery path.

Never extend shutdown to wait for games to finish. Do not delete leases, reset
consumer-group cursors, or edit assignments by hand to force recovery.

## Incident triage

### Partition unowned or recovery slow

1. Check ready task count and Valkey latency first.
2. Inspect assignment version, desired owner, active lease token/TTL, pending
   count and oldest age, active-game/checkpoint parity, pending completion count,
   and checkpoint age.
3. A stale-token rejection is a safety success but an operational alert. Find
   the paused task; do not restore its lease.
4. If the coordinator is absent, existing assignments and authorities should
   continue. Restarting a task is safe; manually assigning keys is not.
5. If retention expired, surface the explicit unrecoverable outcome. Never
   construct state from defaults.

### Planned drain does not converge

1. Verify the task became unready before it sent `Drain` and Traefik uses
   `/health/ready` for backend health.
2. Measure ECS discovery polling plus Traefik active-health removal. The server
   must reject new upgrades with retryable `503` throughout this window.
3. Check client socket generations: the old authenticated/game-ready socket
   must remain usable until the replacement authenticates, rejoins, receives a
   snapshot and resolved-command barrier, and becomes the sole command owner.
4. At the application deadline, allow crash-style recovery. Do not wait for a
   game to finish.

### Valkey pressure or outage

1. All tasks should become unready while liveness stays healthy. Do not create
   an ECS restart storm.
2. `Evictions` must remain zero. If writes are rejected, reduce admission/load
   or increase cache capacity; never switch to an evicting policy.
3. After restoration, allow exact-token leases and consumer pending state to
   reconcile. Do not bypass fencing or reset cursors.

## Required staging evidence

The minimum certification load envelope is 256 concurrent authenticated game
sessions, 128 concurrent duel games, four new sessions per second, and the
`every-tick` command profile over ten partitions. The runner targets 272 game
sessions / 136 duels so ordinary four-session game churn cannot turn a brief
peak into false five-minute evidence. During planned scale-in it adds 10 idle,
10 lobby, and three deliberately unmatched 2v2 probes, so the measured service
load is at least 295 authenticated sockets plus short dual-socket handoff
overlap. These companion probes and the churn buffer are explicit certification
traffic, not a relaxation of the 256-session/128-game minimum. The staging
runner requires that minimum at every sampled second for five minutes and
during the measured scale-in. It also requires a per-second command-write floor
consistent with the `every-tick` profile, no
active-socket hard reconnect,
zero measured usable-session gap for every observed planned handoff, at least
one nonterminal game handoff with its command-outcome barrier, and the complete
`1 -> 10 -> 1` ownership staircase. During the measured `10 -> 1` window it
also requires uninterrupted four-session launch waves from the load generator's
one-second monotonic ticker, no failed admission, and initial WebSocket
authentication within ten seconds.

`--staging` certifies the planned path and deliberately injects no crash.
`--staging-crash` is a separate invocation with no planned-handoff requirement.
The only distinct abrupt external action is one separately authorized
non-production ECS task SIGKILL during a separate run of the same fixed load
envelope while another task is ready. It must not deliver SIGTERM or otherwise
permit graceful cleanup. The local real-process tests prove the
command/checkpoint/fencing kill boundaries; the one external task kill proves
their composition with ECS membership, replacement, ingress reconnect, and the
naturally occurring partition backlog.
Crash mode verifies ECS Exec on the tagged service and every selected task,
suspends scaling policy writes, forces ten healthy tasks, and waits for a
thirty-second stable traffic window. It selects an owned partition only when it
has both active games and pending work, maps that owner to one exact task ARN,
then performs one non-retried ECS Exec command that finds exactly one non-PID-1
`server` process and sends it SIGKILL. The 200 ms control-plane observer requires
the expired member to disappear and a pre-existing survivor to hold a new
fenced lease under a later assignment version within five seconds, before the
replacement task is used. The run then requires affected gateway sessions,
fresh snapshots/outcome barriers, zero unresolved commands, the affected
partition's command output, the exact expected ECS exit-137 record, and restored
ten-task ECS/Traefik health.
A separate Fargate-host failure adds no application failure mode. A remote
Valkey outage is also not an external release action: availability during that
accepted dependency outage is out of scope, while deterministic local
fault-proxy tests cover readiness, liveness, and restoration.

UI evidence is deliberately compositional. Real-browser Playwright exercises
the production React UI and proves that planned handoff does not render the
stale/disconnected overlay. The staging protocol runner proves the actual
server, Traefik, socket-continuity, and command-outcome path. Together they
satisfy the UI criterion; a second staging-browser rendering test is not
required.

The fixed-envelope traffic itself is the ingress capacity test. It fails on a
Traefik scrape error, zero healthy backends, socket loss, failed admission, or
admission latency beyond ten seconds, and it records host CPU and network.
Connection-tracking occupancy may be collected as an optional diagnostic when
available, but it is not a release gate.

The complete evidence package combines local results for deterministic state
fingerprints and command IDs at every kill boundary, stale-owner rejection,
safe command-stream trimming, checkpoint cadence/failure, concurrent
matchmaking and completion effects, and real-browser UI behavior with external
results for:

- planned `1 -> 10 -> 1` dual-socket handoff, continuous new-user admission,
  exact healthy-backend coverage, games/lobbies/matchmaking/idle sockets,
  continuous input, per-task CPU/memory/socket load, Valkey latency/capacity,
  and Traefik/NAT CPU/network; and
- one non-production task SIGKILL during a separate run of the same load
  envelope, followed by five-second authoritative recovery for the affected
  partition's observed backlog, ten-second automatic gateway-session recovery,
  one logical outcome per command, and restored healthy ECS capacity.

Neither the planned staging run nor the non-production task-SIGKILL result has
been executed and attached in this repository. The release remains blocked
until both pass. Local success alone is not evidence of ECS routing and
autoscaling behavior.

The release is blocked if a non-production environment or credentials needed
for these two external results are unavailable.

The runner reads the private regional Valkey through `resilience_admin` and
scrapes Traefik metrics. The canonical `SNAKETRON_STAGING_REDIS_URL` and
`SNAKETRON_TRAEFIK_METRICS_URL` are always identity-checked against the tagged
deployment. Actual control traffic may use the optional
`SNAKETRON_STAGING_REDIS_CONTROL_URL` and
`SNAKETRON_TRAEFIK_METRICS_CONTROL_URL`. If either differs, it must be loopback
and `SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID` must equal the already verified
Traefik instance. Run from a VPC-connected host, VPN, or such an SSM tunnel;
public HTTPS access alone is insufficient.

Before launching load or changing desired count, the opt-in runner verifies the
AWS caller account and the Project=Snaketron, Environment, Region, and
ManagedBy=CDK tags on the ECS service/cluster, Valkey replication group, and
Traefik instance. It also verifies the task definition points at that
environment, logical/AWS region, public origin, and Valkey primary; DNS points
at that Traefik instance; Valkey is the expected available single node with
noeviction; and the supplied Prometheus endpoint belongs to that same instance.
The running image digest must carry exactly one outer-repository commit tag
matching the runner's outer checkout. Both the outer checkout and Snaketron
submodule must be clean, and the submodule HEAD must equal the outer commit's
gitlink. A missing tag, identifier, metric endpoint, account confirmation, or
exact source binding fails before mutation.

The runner changes only that verified non-production ECS service. Cleanup
retries restoration, waits for the original desired/running count, restores the
original enabled autoscaling state, and writes cleanup.json; inability to verify
restoration fails the run. It also refuses known production hosts, production
environment tags, and prod-labeled ECS identifiers:

The runner passes the load tool's generic production-host confirmation because
that tool conservatively protects every snaketron.io subdomain, including the
run-unique staging hostname. The stricter account/resource identity gate above
runs first; this flag is not permission to target a production-tagged deployment.

```bash
export SNAKETRON_STAGING_CONFIRM=RUN_SNAKETRON_STAGING_CHAOS \
SNAKETRON_STAGING_TARGET=https://STAGING_HOST \
SNAKETRON_STAGING_ACCOUNT_ID=123456789012 \
SNAKETRON_STAGING_ENVIRONMENT=dev \
SNAKETRON_ECS_CLUSTER=STAGING_CLUSTER \
SNAKETRON_ECS_SERVICE=STAGING_SERVICE \
SNAKETRON_AWS_REGION=us-east-1 \
SNAKETRON_REGION_CODE=use1 \
SNAKETRON_STAGING_REDIS_URL=redis://STAGING_VALKEY:6379/ \
SNAKETRON_STAGING_REDIS_CONTROL_URL=redis://127.0.0.1:16379/ \
SNAKETRON_VALKEY_REPLICATION_GROUP_ID=snaketron-valkey-dev-use1 \
SNAKETRON_TRAEFIK_INSTANCE_ID=i-0123456789abcdef0 \
SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID=i-0123456789abcdef0 \
SNAKETRON_TRAEFIK_METRICS_URL=http://TRAEFIK_PRIVATE_IP:9090/metrics \
SNAKETRON_TRAEFIK_METRICS_CONTROL_URL=http://127.0.0.1:19090/metrics \
./run_autoscaling_resilience_tests.sh --staging
./run_autoscaling_resilience_tests.sh --staging-crash
```

The “Ephemeral Development Certification” workflow provisions one isolated
development environment, opens both SSM tunnels, runs those commands in that
order, uploads both evidence directories, and always destroys and verifies the
absence of the ephemeral stacks afterward. It does not preserve or transition
state from a previous deployment.

At settled task counts `1`, `10`, and `1`, the runner records membership, the
complete assignment map/version, active lease tokens/TTLs, pending commands,
pending completions, and active-game counts. It fails unless leases match
desired owners, tokens are unique, owners are balanced, assignment versions
advance, the assignment's eligible-member set exactly equals active membership,
and each staircase leg moves the minimum nine partitions. A fresh ten-task
membership/ECS-health pair is captured immediately before scale-in. These
snapshots complement continuous unowned-duration and fencing metrics. It also
writes `scale-in-window.json`; report schema 9 includes each session's launch
wave, start time, and bounded initial admission-ready duration so the admission
assertion is phase-specific.

Scaling evidence has three deliberately distinct parts:

1. With policy writes enabled, load must cause an AWS-observed desired/running
   count above one and a successful target-tracking scaling activity. This is
   the automatic scale-out proof.
2. Policy writes are temporarily suspended only for the deterministic forced
   1-to-10-to-1 ownership staircase. The settled control-plane snapshots prove
   that leg's exact ownership behavior.
3. The runner establishes a fresh ten-task baseline while load is still active,
   waits for the finite load to end, re-enables target tracking, and requires an
   AWS-observed automatic scale-in to one plus a successful target-tracking
   activity. This scale-in observation is separate from the forced staircase.

Report schema 9 records coordinator-observed, server-confirmed peak
authentication concurrency, fully joined active-game concurrency, lifecycle
timestamps, exact initial task boot identity, planned-handoff evidence, and a
per-second aggregate of successful command writes plus first-seen authoritative
scheduled outcomes by partition. It also retains each successful hard recovery's
old/new task identity and socket generation, detection/ready timestamps, fresh
snapshot proof, and pending-command counts before and after the outcome barrier.
The count after the initial barrier is diagnostic, not a zero gate: a command
whose write outcome was ambiguous may be absent from that first response and is
then resent with the same stable identity. Certification instead requires every
session to finish with zero pending commands; first-seen terminal outcomes and
the deterministic deduplication tests enforce one logical result despite a
physical resend.
The
base game gate requires at least 256 authenticated sessions and 128 simultaneous
duel games throughout the five-minute interval, in addition to phase-specific
scale-in admission checks. Separate population reports require every idle, lobby, and
queued probe to reach its intended state before scale-in and remain alive until
that transition finishes; their exact task identities must collectively cover
the settled ten-task membership.

The runner derives the Traefik service label from the verified ECS task
definition, continuously scrapes its service-server-up gauge, and fails on
any scrape error or zero-healthy-backend sample. Settled ECS phase snapshots
require every running task to be healthy. After CloudWatch ingestion settles,
the runner requires complete time-bucket coverage for ECS CPU/memory, ElastiCache
CPU/memory/connections/evictions, Traefik-host CPU/network, and resilience
metric series. It also saves and gates a Container Insights Logs Insights result
with CPU/memory samples for every exact ECS task ID in the fresh ten-task
membership snapshot. It fails on a zero-ready sample, recovery fingerprint divergence,
ownership/index mismatch, planned drain failure, unexpected Valkey eviction,
insufficient Valkey memory headroom, or failure to corroborate the 295-socket
measured load (272 game sessions plus 23 mixed-population probes).
