# Autoscaling resilience operations runbook

This runbook covers deployment, certification, and steady-state operation of
the autoscaling design in [autoscaling-resilience-prd.md](autoscaling-resilience-prd.md).

## Accepted availability boundary

- `minTasks=1` is intentional. If the sole task dies, the region is unavailable
  until ECS starts a replacement. Games recover only while their Valkey
  checkpoints remain inside `SNAKETRON_RECOVERY_RETENTION_MS`.
- Regional Valkey is one logical ElastiCache Serverless Valkey 8 cache. A cache
  outage or data loss can take the region down. Serverless is TLS-only,
  cluster-mode-only, and fixes `maxmemory-policy=volatile-lru`; CDK deliberately
  sets no paid ECPU minimum and no data or ECPU maximum. Any `Evictions` or
  `ThrottledCmds` sample is a release failure and production alarm because
  leases, streams, assignments, and checkpoints are correctness-bearing.
- Traefik/NAT remains a single ingress dependency. Its failure is outside this
  release's availability guarantee.
- A hard gateway crash necessarily drops its sockets. Clients reconnect
  automatically. Planned task removal uses make-before-break handoff and must
  maintain at least one usable authenticated socket when another ready task is
  available.
- Recovery after checkpoint retention expires is explicitly unrecoverable; the
  server must not fabricate a replacement game.

## One-time production Serverless Valkey cutover

The first production deployment of this design is intentionally destructive.
There are no users or cache state to migrate, so the production workflow does
not dual-run the old node cache, preserve its plaintext endpoint, or attempt a
zero-downtime compatibility transition.

Production deployment is manual-only and restricted to the current `main`
commit. Every dispatch, including a dry run with credentialed CDK planning,
must provide the GitHub Actions run ID of a successful main-branch Ephemeral
Development Certification for that exact outer-repository commit. That run
must complete the planned suite, hard-crash suite, and verified cleanup; a run
for another commit, branch, repository, or workflow is rejected before AWS
credentials are used.

For each production region, the workflow detects the old
`AWS::ElastiCache::ReplicationGroup`, deletes and waits for the Monitoring and
Server stacks that consume its generated endpoint exports, and removes the
Server stack's retained `/ecs/snaketron/prod` log group. It leaves the Valkey
stack itself in place so the ordinary dependency-ordered CDK deployment can
replace its resource with Valkey 8 Serverless before recreating Server and
Monitoring. The step is idempotent. While the legacy resource exists it performs
the destructive cutoff; after Valkey is already Serverless, a rerun also removes
failed, non-updatable Server or Monitoring stacks, cleans the retained Server
log group when recreation is needed, and lets the ordinary deployment recreate
missing consumers.

Expect service downtime and loss of the old cache contents during this one-time
cutover. Old task definitions are not a valid rollback because they contain the
deleted plaintext endpoint; a failed cutover is recovered by rerunning or
fixing the deployment forward. After deployment, the workflow requires one
available Valkey 8 Serverless cache per region, no old Snaketron replication
group, and an exact task URL of
`rediss://HOST:6379/?protocol=resp3&cluster=true`.
The production cache has CloudFormation deletion and replacement policies of
`Retain`; ephemeral development does not, so its mandatory cleanup remains
unchanged. Deleting a retained production cache is a separate deliberate
operator action, not a consequence of deleting its stack.

For steady-state inspection after startup:

```bash
cargo run -p server --release --bin resilience_admin -- status \
  --region-key use1 \
  --redis-url 'rediss://SERVERLESS_VALKEY:6379/?protocol=resp3&cluster=true'
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
acceptance results, not additional staging fault actions. Local standalone
Valkey preserves numbered test databases; static key-family tests cover Cluster
slot compatibility. Only the public ephemeral run against actual ElastiCache
Serverless proves TLS, cluster routing across every slot family, and provider behavior.

That test is deliberately local-only and mutation-safe: it refuses non-loopback
hosts, requires dedicated standalone Valkey database 14, serializes itself with a Redis
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
Both retain a minimum of one. The application task uses one vCPU and two GiB so
the one-task floor has takeover and burst headroom while target tracking is
still observing load. CPU 70%, memory 80%, and both 60-second cooldowns remain
unchanged.

## Routine deployments

Routine ECS deployments use the same steady-state mechanisms as autoscaling:

- staggered ready-task joins and planned drains coalesce behind a four-second
  quiet window while every incumbent can continue serving;
- a missing, expired, warming, or incompatible owner bypasses that window and
  is reassigned immediately after failure detection;
- the final ready-task set receives one balanced desired assignment;
- executor partitions move under fenced ownership without moving WebSockets;
- a departing task publishes `DRAINING`, keeps serving until its monotonic
  partition views move, performs bounded partition handoff, and requests
  make-before-break socket handoff;
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
2. Verify every incumbent game barrier wrote its fenced
   `planned-handoff-watermark:v1`, the successor recovery snapshot advanced
   beyond it, and the successor checkpoint removed it. An executor handoff
   failure must suppress `Drain` entirely so clients take the crash-reconnect
   path instead of using a planned stream frontier. Do not treat an empty local
   executor registry as success; inspect the process-boot failure latch for an
   authority-loss, worker, cancellation, or cleanup exit.
3. Measure ECS discovery polling plus Traefik active-health removal. The server
   must reject new upgrades with retryable `503` throughout this window.
4. Check client socket generations: the old authenticated/game-ready socket
   must remain usable until the replacement authenticates, rejoins, receives a
   snapshot and its paired resolved-command barrier, catches the fixed
   post-Pong stream frontier, and becomes the sole command owner. Events from
   the old socket must remain visible while the candidate catches up. After
   promotion, covered candidate snapshots or deltas that arrive late must stay
   suppressed until that stream advances beyond the old applied watermark.
5. At the application deadline, allow crash-style recovery. Do not wait for a
   game to finish.

### Replica warming or WebSocket burst rejection

1. A playing session that receives `GameWarming` keeps its authenticated socket,
   pauses command emission, and retries `JoinGame` after the server hint within
   the existing game deadline. It resumes only after a fresh snapshot and
   `CommandOutcomesComplete`, then resends any still-unresolved commands with
   their stable identities. Do not force a reconnect or extend the deadline.
2. Gateway replicas read only partition events. If event delivery stalls, check
   that the event-only reader is alive and inspect its last stream ID and any
   trim-horizon warning. Executor command or snapshot-request channel depth is
   not a gateway replica dependency.
3. Traefik keeps a sustained WebSocket-upgrade average of 50 per source IP and a
   burst of 512. A `429` is retryable inside the client's existing admission
   deadline. If the 512 burst is exceeded, inspect the source and cohort; do
   not raise the sustained average or admission deadline to make a test pass.

### Valkey pressure or outage

1. All tasks should become unready while liveness stays healthy. Do not create
   an ECS restart storm.
2. `Evictions` and `ThrottledCmds` must remain zero. If either is nonzero,
   preserve diagnostics and stop the release. Do not add a storage/ECPU ceiling
   or pretend Serverless can be configured to `noeviction`.
3. After restoration, allow exact-token leases and consumer pending state to
   reconcile. Do not bypass fencing or reset cursors.

## Required staging evidence

Certification has three independent load gates.

**Gate A — natural scale-out.** Run a fixed 224-session / 112-duel
`every-tick` cohort from the one-vCPU minimum task. It retains one stage, the
20-minute runner, eight-minute target-tracking observation budget, and the
existing one-second command-outcome budget. It does not use synthetic CPU,
lower a target, force the transition, or adapt load from live metrics. CPU or
memory target tracking must add capacity naturally; failure to trigger or to
preserve command continuity fails this gate. Once the automatic scale-out
evidence is complete, let this runner finish and require its WebSockets and
authoritative games to reach zero. Gate A traffic must not be carried into the
reset or forced staircase.

**Gate B — planned ownership and socket transition.** With Gate A traffic
gone, suspend policy writes, return to one healthy task, and launch a separate
fixed 128-session / 64-duel `every-tick` cohort. Exact-source run
`30046381977` held this envelope at 45.18--48.83% average one-task CPU over
complete steady minutes, resolved every command inside 500 milliseconds, and
recorded zero checkpoint failures. Before movement, require active games and
scheduled command traffic on every executor partition and prove the one-task
baseline remains inside the one-second command budget without lease,
heartbeat, checkpoint, or event-publication timeouts.

Force the direct `1 -> 10 -> 1` staircase under that same 128-session cohort.
At ten tasks add 10 idle, 10 lobby, and three deliberately unmatched 2v2
matchmaking probes. During planned scale-in, use the bounded open-loop
admission mode: start four additional idle sessions every
second regardless of the current ready count, hold each successful session for
one second after it becomes ready, and enforce a 64-session in-flight safety
ceiling. The unchanged ten-second admission deadline plus the one-second hold
would account for at most 44 normally progressing sessions at four starts per
second; the ceiling leaves scheduling margin while still failing a stuck
probe. Allocate enough total session budget to sustain launches for the
complete 45-second scale-in window. Assert every four-session wave, its bounded
admission-ready time, the 64-session in-flight ceiling, and continuous wave
coverage from before scale-in starts until after it finishes.

This complete Gate B destination is bounded at 215 sockets (128 game, 23
context, and 64 admission) and only 128 are command-bearing. Gate A has already
placed 224 command-bearing sockets on the one-task origin while ramping at the
same four starts per second, so it is the conservative one-task capacity
precondition rather than a duplicated rehearsal. Gate B must still prove the
final survivor remains ready and resolves every command inside budget.

Gate B must prove no active-socket hard reconnect, zero measured usable-session
gap, terminal command outcomes, nonterminal game handoffs with
command-outcome barriers, and exactly nine partition moves in each direction.
No game completion is awaited before either desired-count change. Its steady
population is 128 command-bearing game sockets plus 23 context probes. The
open-loop admission sessions are bounded transient traffic rather than another
steady target, and make-before-break candidates are additional transient
sockets.

**Gate C — ten-task capacity.** Only after Gate B traffic is gone, establish
ten healthy tasks in ECS and Traefik, settled membership, assignment, and
partition leases. Configure 272 game sessions / 136 duels so ordinary churn
cannot turn one brief peak into false evidence. Require at least 256 concurrent
authenticated game sessions / 128 duels and `every-tick` traffic on every
partition for five continuous minutes after ramping at four new sessions per
second. This capacity envelope never runs on the one-task baseline.

A forced scale-in is valid only when its complete destination load is sized
from demonstrated one-task capacity. Gate B proves the command-bearing cohort
on one task before movement and must prove the complete destination remains
healthy after movement. If the survivor reaches CPU starvation and lease
probes, membership heartbeats, checkpoints, or event writes time out together,
the run has violated that capacity precondition; it has not isolated a
handoff-ordering defect. Do not weaken fencing to make such a run pass. A write
without current lease proof remains rejected, the executor and planned drain
fail closed, and ordinary lease-expiry recovery remains authoritative.

`--staging` certifies the planned path and deliberately injects no crash.
`--staging-crash` is a separate invocation with no planned-handoff requirement.
The only distinct abrupt external action is one separately authorized
non-production ECS task SIGKILL during a separate run of the ten-task
272-session crash envelope while another task is ready. It must not deliver
SIGTERM or otherwise permit graceful cleanup. The local real-process tests prove the
command/checkpoint/fencing kill boundaries; the one external task kill proves
their composition with ECS membership, replacement, ingress reconnect, and the
naturally occurring partition backlog.
Crash mode verifies ECS Exec on the tagged service and every selected task,
suspends scaling policy writes, and forces and verifies ten healthy/ready tasks
before launching the 272-session load. It then requires at least 256 public
WebSockets and 128 authoritative active games for thirty consecutive seconds;
the final load report is the authority for authenticated session count. It
selects an owned partition only when it
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

Gate C traffic is the ingress capacity test. It fails on a
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
- one non-production task SIGKILL during a separate run of the ten-task
  272-session crash envelope, followed by five-second authoritative recovery
  for the affected partition's observed backlog, ten-second automatic
  gateway-session recovery, one logical outcome per command, and restored
  healthy ECS capacity.

The records below retain the earlier combined-harness `Run A` / `Run B`
terminology and its then-current load decisions. They are historical diagnostic
evidence, not the current three-gate definition above.

Neither the planned staging run nor the non-production task-SIGKILL result has
a passing report attached in this repository. The first Serverless-backed
planned run, GitHub Actions `29990657012`, provisioned and exercised Valkey 8.1
without cache throttling or eviction and cleaned up successfully, but exposed
one-task saturation plus concurrent snapshot/checkpoint amplification and
exceeded the one-second command budget.
The follow-up exact-source run, GitHub Actions `29996912370`, again provisioned
Valkey 8.1 over TLS/RESP3, recorded zero cache throttling and eviction, admitted
208 of 208 new sessions, and completed 61 of 61 planned active-game handoffs
with no socket reconnect or usable-session gap. It still failed: every full
one-task baseline second exceeded the one-second command-outcome budget (12.114
seconds maximum), and six sessions across three newly created lobbies missed
their authoritative roster because at-most-once Pub/Sub had no read-repair
path. The crash phase therefore did not run. Its cleanup completed and an
independent inventory found no development resource remaining. Both runs are
diagnostic evidence, not release evidence. The release remains blocked
until fresh planned and crash runs pass. Local success alone is not evidence of
ECS routing and autoscaling behavior.

The next exact-source run, GitHub Actions `30007863987`, also kept Serverless
Valkey healthy: zero throttling and eviction, average service-side read/write
latency below 1.5 milliseconds, and 197,000--234,000 commands per minute. It
failed before scale-out because adjacent same-game commands caused a global
dispatcher settlement barrier. The task averaged only 30% CPU while pending
command age reached 65.739 seconds, maximum command outcome latency reached
51.605 seconds, and 24 of 574 sessions timed out waiting for an initial
snapshot. Cleanup succeeded and an independent inventory found no development
resource remaining. After changing the barrier to settle only the repeated
game, the same 96-socket / 48-duel profile completed 172,093 commands locally
with a 170-millisecond maximum outcome latency, zero failed session attempts,
and a sub-second pending backlog. That local result identifies the bottleneck
but does not replace the required fresh AWS certification.

The selective-settlement AWS follow-up, GitHub Actions `30014346604`, showed
that command-only interleaving still left lifecycle markers as cross-game
barriers. It failed before scale-out with about 70 seconds of pending age and a
53.6-second maximum command outcome. GitHub Actions `30021797806` then
successfully exercised automatic `1 -> 2 -> 1`, forced `1 -> 10 -> 1`, balanced
lease movement, and 64 of 64 zero-gap planned handoffs on actual Serverless
Valkey. It still failed the command budget during reset-to-one and later game
rollover; maximum outcome latency reached 44.765 seconds and the capacity and
SIGKILL phases did not run. Valkey recorded zero throttling and eviction.
DynamoDB completion/admission writes throttled independently, but their timing
and the Valkey-only command scheduling path do not explain the sustained
command-outcome backlog by themselves. Cleanup again removed every development
runtime resource.

After full-event per-game interleaving and game-local lifecycle settlement, the
local six-millisecond-cache-RTT rollover profile passed 288 of 288 sessions,
144 of 144 games, and 251,700 command outcomes with no disconnect, no failed
session, no sent-second above one second, and a 291-millisecond maximum. Treat
this as diagnostic only. Do not mark the release complete until a fresh AWS
planned run reaches the full capacity phase and a separate authorized SIGKILL
run passes.

The next exact-source Serverless run, GitHub Actions `30030317623`, completed
538 of 538 sessions and returned terminal outcomes for all 399,655 submitted
commands with no disconnect, while Valkey remained free of throttling and
eviction with roughly one-millisecond service-side latency. It still failed
before scale-out: the 96-session cohort held service-average CPU below the 70%
target, maximum command-outcome latency reached 32.1 seconds, game-join p99
reached 32.7 seconds, and pending age reached roughly 33 seconds. Recovery
payload inspection found that every authoritative server command created a
tombstone intended only to cancel a speculative client command; those unused
tombstones grew forever and were serialized into every recovery checkpoint and
snapshot. Cleanup succeeded and an independent inventory found no development
runtime resource remaining.

After bounding that queue bookkeeping, resetting a slow tick interval so it
cannot repeatedly beat queued actor mail, and isolating large recovery traffic
on one independent Redis dispatcher, the recalibrated 144-session / 72-duel
local profile passed 288 of 288 sessions, 144 of 144 games, and 258,446 of
258,446 command outcomes. Maximum outcome latency was 148 milliseconds, no
sent-second exceeded one second, no client disconnected, pending age remained
below one second, and the recovery envelope plateaued near 244 KB instead of
growing without bound. The process used roughly 79% CPU at the full plateau.
This is causal diagnostic evidence only; actual Serverless cluster-mode
planned and SIGKILL runs remain mandatory.

Exact-source Serverless run
[`30039460661`](https://github.com/lopatin/snaketron-io/actions/runs/30039460661)
then proved natural CPU target-tracking `1 -> 2`, the forced `1 -> 10 -> 1`
staircase, all 1,852 continuity sessions, all 926 games, all 1,653,922 command outcomes,
256 of 256 planned handoffs, zero reconnects, and zero measured usable-session
gap. It still failed the unchanged one-second latency gate. The 144-session
one-task baseline spent five complete minutes at 95.7--98.3% CPU and had 20
failing sent-seconds with a 2.023-second maximum. Forced scale-out had 12
failing seconds with a 3.278-second maximum; scale-in had seven with a
2.081-second maximum. Serverless Valkey reported zero throttle and eviction
with sub-1.4-millisecond average successful request latency.

At the scale-out burst, seven recovery-envelope reads and one fenced checkpoint
write sharing the recovery dispatcher hit their 750-millisecond client
deadlines together. The checkpoint retained unacknowledged work, retried, and
the affected game completed durably; there was no fence rejection or data
loss. That correction added exactly one fresh checkpoint-write dispatcher per
task while leaving takeover/reconnect reads and best-effort regional metrics on
the then-existing recovery-read dispatcher. The first follow-up used 128 sessions /
64 duels based on a cross-topology projection from the saturated 144-session
run.

Exact-source Serverless run
[`30046381977`](https://github.com/lopatin/snaketron-io/actions/runs/30046381977)
showed why that projection was invalid after the dispatcher split. The
configured 128-session / 64-duel stage remained active for the full eight-minute
scale-out observation window and completed 768 of 768 sessions, 384 of 384
games, and all 687,455 commands. Worst sent-second outcome latency was 488
milliseconds, no second
exceeded 500 milliseconds or the one-second gate, disconnects/reconnects were
zero, and checkpoint failures were zero. Complete-minute CPU averages remained
45.18--48.83%, so target tracking correctly stayed at one task and the runner
failed closed before forced handoff, capacity, or SIGKILL. The smaller cohort
and connection topology changed together, so do not claim the split alone
halved CPU. Command rate fell only about 10% while CPU, pending age, and latency
collapsed, which is consistent with removal of nonlinear queue amplification.

Run A is fixed once from this same-version evidence at 224 sessions / 112
duels. After subtracting the measured 3.5% idle CPU, the observed range projects
to 76.4--82.8% at 224. Keep the existing CPU 70% / memory 80% targets,
one-second gate, one-stage 20-minute runner, and eight-minute target-tracking
budget. Do not adjust the cohort again if a later run fails.
One hundred two best-effort active-game mapping lookups also timed out during
the 144-session ownership bursts without causing a failed admission or usable
gap; retain this as a diagnostic risk and investigate the matchmaking-manager
critical section only if it recurs at the bounded 224-session run. Cleanup
succeeded and its full absence verification passed for both follow-ups. The
capacity and SIGKILL phases did not run in either one.

The first run at the frozen cohort, GitHub Actions
[`30050625836`](https://github.com/lopatin/snaketron-io/actions/runs/30050625836),
used the same exact server binary and the same ECS availability zone as
`30046381977`. Its ordinary successful Serverless Valkey request latency was
about 1.2--1.3 milliseconds instead of about 0.2 milliseconds, with zero
throttling, zero eviction, and service CPU around 40%. The run failed before
scale-out: 220 of 488 sessions timed out waiting for their initial game
snapshot. All 156,742 commands eventually received terminal outcomes, but only
70,137 were scheduled, 86,605 were rejected after the backlog formed, maximum
outcome latency reached 65.749 seconds, and oldest pending age reached 95.251
seconds. This is not evidence of cache capacity exhaustion or of reaching the
CPU target.

The causal risk is cross-partition head-of-line coupling in the client.
All ten partition consumers and hot-path writes cloned one `redis-rs`
`ClusterConnection`; the clones share one bounded dispatcher and the same
underlying per-node multiplexed connections. `ClusterConnection` does support
multiple in-flight requests, so do not describe this as strict Redis request
serialization. The correction below isolates the fixed partition hot paths
from one another while preserving the existing control and bulk-role
connections. Run `30050625836` is diagnostic evidence only. Do not change the
224-session / 112-duel cohort, the CPU 70% / memory 80% policy, the one-second
gate, or any other acceptance criterion. A fresh full planned run and a
separate authorized SIGKILL run remain mandatory.

Exact-source Serverless run
[`30057487544`](https://github.com/lopatin/snaketron-io/actions/runs/30057487544)
used outer commit `e23c6b5f3a62bdacdb51742aa12b03b5d8836a0c` and Snaketron
commit `36f7ac51912072fa6de3d6f2f43f9410d801c6de`. Natural CPU target
tracking moved `1 -> 2`; the deterministic staircase then moved
`1 -> 10 -> 1` with healthy assignment and lease movement. Actual Valkey 8
Serverless recorded zero `Evictions` and zero `ThrottledCmds`. The run attempted
2,770 sessions, completed 2,747, and submitted 2,167,559 commands.

The run failed the unchanged planned-path gates. Twenty-three sessions failed:
19 timed out waiting for their games, and the other four were two WebSocket
upgrade `429` responses plus the paired lobby-session cancellations. Forty-six
planned handoffs hard-reconnected or were marked failed, maximum usable-session
gap was 3,497 milliseconds, 300 commands remained unresolved, maximum
command-outcome latency was 10,381 milliseconds, and 534 original-send seconds
exceeded one second. The separate SIGKILL certification therefore remains
pending.

Application logs contain 1,215 exact
`Timed out loading command outcomes for snapshot; retrying` warnings. This log
site is gateway-only and none exhausted the hard warm-up deadline, but the reads
shared one bounded recovery dispatcher with all ten partition takeover
bootstraps and regional metrics. The CPU-saturated scale-in survivor emitted
1,172 warnings across all ten partitions, while each metrics pass spent roughly
2--5.6 seconds scanning about 100 recovery envelopes. Partition-scoped lanes and
a separate metrics dispatcher remove that cross-partition/cross-role risk; they
are not a claim that topology alone removes CPU saturation or duplicate reads
within one partition. Separately, gateway replicas used one sequential
reader for partition events, executor commands, and snapshot requests.
Continuous event traffic starved command-channel draining; a full channel then
stopped the reader from fetching later events. The 19 game-wait failures map to
19 `Replica did not become subscribable after recovery snapshot` warnings even
though those authoritative games later completed durably. The two `429`
responses came from the valid same-IP scale-in reconnect/admission burst
exceeding Traefik's configured 100-upgrade burst, not from an unhealthy
backend.

The minimum correction is deliberately bounded. Each task opens ten independent
partition-scoped recovery-read lanes plus one separate best-effort metrics
dispatcher; gateway replicas read only the partition event stream; and a
terminal snapshot is broadcast and then immediately evicted locally because
the same fenced completion script made its completion record, final recovery
envelope, stored snapshot, and pending-effect index durable before the event
became observable.
The load client pauses commands and retries playing-phase `GameWarming` on the
same authenticated socket. Traefik retains the 50-upgrade-per-second average,
raises only its burst to 512 for the certified make-before-break cohort, and
the load client retries `429` through its existing admission/reconnect deadline
just as the browser does. Keep the one checkpoint-write dispatcher per task.
Do not add more pools, lengthen the 750-millisecond Redis deadline or any
admission deadline, add a recovery cache, adjust the frozen load, change CPU or
memory targets, or relax any acceptance gate.
The next run must record Serverless connection count and any remaining
recovery-read warnings before considering another optimization.

Cleanup for run `30057487544` succeeded. Independent inventory found no
development stacks, Serverless cache, ECS/EC2/EIP resources, ECR repository,
DynamoDB tables, alarms, log groups, DNS records, or scaling targets; the
imported production VPC remained untouched. Treat this run as diagnostic
evidence only. Fresh complete planned and separately authorized SIGKILL runs
are both required.

The release is blocked if a non-production environment or credentials needed
for these two external results are unavailable.

The runner reads the private regional Serverless Valkey through `resilience_admin` and
scrapes Traefik metrics. The canonical `SNAKETRON_STAGING_REDIS_URL` and
`SNAKETRON_TRAEFIK_METRICS_URL` are always identity-checked against the tagged
deployment. `SNAKETRON_STAGING_REDIS_CONTROL_URL` must equal the canonical URL
so TLS SNI and Cluster topology retain the real cache hostname. Traefik control
traffic may use a differing loopback
`SNAKETRON_TRAEFIK_METRICS_CONTROL_URL`; then
`SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID` must equal the already verified Traefik
instance. Run from a VPC-connected host, VPN, or such an SSM tunnel;
public HTTPS access alone is insufficient. The certification workflow preserves
the real cache hostname in the `rediss://` URL, maps that hostname to loopback,
and forwards both Serverless ports 6379 (primary) and 6380 (read endpoint)
through separate SSM sessions so rustls still validates the AWS certificate
and redis-rs can complete cluster discovery. The application URL remains
anchored to port 6379.

The server opens a separate reconnecting Redis connection pool for RESP3
Pub/Sub. Subscription confirmations must never share the reply queue used by
matchmaking, executor, or recovery commands; this role separation is required
for Serverless certification.

Before declaring ready, each task independently bootstraps exactly ten
partition-hot `redis-rs` cluster connections, one deterministic lane for each
fixed executor partition. Partition-scoped `GameBus` command publication and
consumption, ordinary events, snapshot anchors, acknowledgements, and fenced
mutations use the lane selected by partition. A lane must not be a clone of the
global control connection or of another lane: clones share the bounded
client-side dispatcher and underlying per-node connection set. This fixed
one-lane-per-partition map is intentional; do not replace it with a tunable
generic pool, per-game connections, a priority scheduler, or retries that hide
queueing.

Gateway replicas anchor and resume one event-only reader per partition. They do
not read or drain executor commands or snapshot requests, so backpressure in
those streams cannot halt user-visible event delivery. On a terminal full
snapshot, the replica broadcasts first and then evicts the local game. This is
safe because the single fenced completion script committed the immutable
completion record, final recovery envelope, stored snapshot, pending-effect
index, and terminal publications before any reader could observe the snapshot;
do not reintroduce a command-stream completion marker as an eviction
dependency.

The durable `GameCreated` scanner groups each validated scan page by partition
and uses nonblocking sends to ten delivery workers, one per fixed partition.
Each lane holds one active and at most one queued batch. Its worker preserves
publish, compare-delete, and marker-expiry order while continuing through the
batch after a record-specific error. A full worker leaves the batch's records
in the authoritative Redis outbox for a later scan, so one slow lane cannot
stall admission for another partition and the in-memory queue never becomes a
retry source.

Keep one low-volume global control connection for `PartitionLeaseStore`
acquire/renew/release, membership, assignment, matchmaking, and readiness.
Fenced partition-hot scripts execute through their partition lane and validate
the live lease key atomically there; moving lease liveness traffic onto a busy
data lane would weaken takeover timing. Full-state periodic checkpoints and
terminal completion commits keep one independently bootstrapped checkpoint-write
dispatcher per task. Independently bootstrap exactly ten partition-scoped
recovery-read connections and route takeover journal/envelope loads,
stored-snapshot and recovery-failure loads, reconnect outcome reads, and
immutable completion-record loads by partition. Regional resilience metrics
use one additional best-effort dispatcher so telemetry cannot queue ahead of
recovery.
RESP3 Pub/Sub and stream readers also keep their separate connections. This is
the minimum role topology required by the observed Serverless latency
variance. Do not add a dynamic or per-game pool, longer deadlines, a recovery
cache, or another persistence layer.

Before launching load or changing desired count, the opt-in runner verifies the
AWS caller account and the Project=Snaketron, Environment, Region, and
ManagedBy=CDK tags on the ECS service/cluster, Serverless Valkey cache, and
Traefik instance. It also verifies the task definition points at that
environment, logical/AWS region, public origin, and exact TLS/RESP3/cluster
Valkey endpoint; DNS points at that Traefik instance; the cache is available;
and the supplied Prometheus endpoint belongs to that same instance.
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
SNAKETRON_STAGING_REDIS_URL='rediss://STAGING_SERVERLESS_VALKEY:6379/?protocol=resp3&cluster=true' \
SNAKETRON_STAGING_REDIS_CONTROL_URL='rediss://STAGING_SERVERLESS_VALKEY:6379/?protocol=resp3&cluster=true' \
SNAKETRON_VALKEY_SERVERLESS_CACHE_NAME=snaketron-valkey-serverless-dev-use1 \
SNAKETRON_TRAEFIK_INSTANCE_ID=i-0123456789abcdef0 \
SNAKETRON_CONTROL_TUNNEL_INSTANCE_ID=i-0123456789abcdef0 \
SNAKETRON_TRAEFIK_METRICS_URL=http://TRAEFIK_PRIVATE_IP:9090/metrics \
SNAKETRON_TRAEFIK_METRICS_CONTROL_URL=http://127.0.0.1:19090/metrics \
./run_autoscaling_resilience_tests.sh --staging
./run_autoscaling_resilience_tests.sh --staging-crash
```

The “Ephemeral Development Certification” workflow provisions one short-lived
development environment, opens three SSM sessions (Valkey 6379, Valkey 6380,
and Traefik metrics), runs those commands in that order, uploads both evidence
directories, and always destroys and verifies the absence of the ephemeral
stacks afterward. The workflow discovers and validates the production Network
stack's VPC, then imports it read-only. Development owns only its separately
tagged security groups, Serverless cache, ECS resources, Traefik/EIP, and
run-unique DNS record; cleanup must prove the shared VPC still exists and must
never create, replace, or delete its routes, endpoints, or flow logs.
Each run-unique public host can consume one new certificate, so the workflow
fails closed after 30 attempts in a rolling seven-day window. This preserves
20 issuance opportunities against Let's Encrypt's current 50-certificate
registered-domain allowance; it is a conservative workflow budget, not proof
of the domain's live CA quota.

At settled Gate B task counts `1`, `10`, and `1`, the runner records membership, the
complete assignment map/version, active lease tokens/TTLs, pending commands,
pending completions, and active-game counts. It fails unless leases match
desired owners, tokens are unique, owners are balanced, the assignment's
eligible-member set exactly equals active membership, and each forced staircase
leg advances assignment monotonically while moving the minimum nine partitions
between its settled endpoints. Closely spaced membership waves should coalesce,
but task-readiness waves separated by more than the quiet window may correctly
produce more than one intermediate version. A fresh ten-task
membership/ECS-health pair is captured immediately before scale-in. These
snapshots complement continuous unowned-duration and fencing metrics. It also
records the automatic, reset, forced scale-out, and forced scale-in windows;
report schema 10 includes each session's launch
wave, start time, and bounded initial admission-ready duration so the admission
assertion is phase-specific.

Scaling evidence has five deliberately distinct parts:

1. With policy writes enabled, fixed Gate A load must cause an AWS-observed
   desired/running count above one and a successful target-tracking scaling
   activity. The runner fails immediately if Gate A exits first. This is the
   automatic scale-out proof. Its strict continuity window begins at the first
   successful scaling activity, excluding only the preceding client ramp when
   no ownership transition exists.
2. Gate A then finishes. The runner requires zero remaining Gate A WebSockets
   and authoritative games before returning the service to one task. It starts
   the separate 128-session Gate B cohort only after that one-task baseline is
   healthy and inside the command and control-operation budgets.
3. Policy writes remain suspended for Gate B's deterministic
   `1 -> 10 -> 1` ownership staircase, 23 durable context probes, and bounded
   open-loop admissions. The settled control-plane snapshots prove exact
   ownership behavior without placing either high-load Gate A or capacity Gate
   C on one task.
4. With policy writes still suspended and all Gate B clients gone, the runner
   establishes ten verified tasks and runs the separate 272-session Gate C. The
   per-second 256-session/128-duel and command gates cover five continuous
   minutes, and exact task identities prove socket/event distribution.
5. Only after Gate C has ended does the runner re-enable target tracking and
   require an AWS-observed automatic scale-in from ten to one plus a successful
   target-tracking activity. This observation is separate from the forced
   staircase.

Report schema 10 records coordinator-observed, server-confirmed peak
authentication concurrency, fully joined active-game concurrency, lifecycle
timestamps, exact initial task boot identity, planned-handoff evidence, and a
per-second aggregate of logical command submissions, receipt-time scheduled
outcomes by partition, and accepted/scheduled outcomes by original send second.
It also records every terminal command outcome by original send second and that
second's maximum end-to-end latency. Every
full second under executor movement and capacity load must resolve exactly its
sent-command count with no result taking more than one second; this prevents a
catch-up burst from hiding an authoritative pause. The report retains each successful hard recovery's
old/new task identity and socket generation, detection/ready timestamps, fresh
snapshot proof, and pending-command counts before and after the outcome barrier.
The count after the initial barrier is diagnostic, not a zero gate: a command
whose write outcome was ambiguous may be absent from that first response and is
then resent with the same stable identity. Certification instead requires every
session to finish with zero pending commands; first-seen terminal outcomes and
the deterministic deduplication tests enforce one logical result despite a
physical resend.
While a session is already playing, `GameWarming` pauses its command generator
and schedules same-socket `JoinGame` retries from the server hint. Only a fresh
snapshot plus `CommandOutcomesComplete` resumes commands and triggers stable-ID
resends for any still-unresolved commands. WebSocket-upgrade `429` also uses the ordinary
bounded reconnect loop. Both remain charged to the original game/admission
deadline and are reported; neither creates an exclusion from the one-second
command gate, ten-second admission gate, or zero-gap planned-handoff gate.
The Gate C run requires at least 256 authenticated sessions and 128 simultaneous
duel games throughout the five-minute interval. Separate Gate B population
reports require every idle, lobby, and queued probe to reach its intended state
before scale-in and remain alive until that transition finishes. The open-loop
admission report proves one four-session start wave per second, a one-second
post-ready hold, no more than 64 sessions in flight, and ten-second p99
readiness throughout the scale-in window. Exact task identities must cover the
settled ten-task membership in both the transition and capacity phases.

The runner continuously scrapes Traefik's service-server-up gauge, accepts its
opaque per-task service IDs, and matches settled tasks by exact private-IP
`:8080` URL. It fails on any scrape error or zero-healthy-backend sample.
Settled ECS phase snapshots
require every running task to be healthy. After CloudWatch ingestion settles,
the runner requires complete time-bucket coverage for ECS CPU/memory, Serverless
Valkey bytes/ECPU/read-write latency/connections/network/evictions/throttling,
Traefik-host CPU/network, and resilience
metric series. It also saves and gates a Container Insights Logs Insights result
with CPU/memory samples for every exact ECS task ID in the fresh ten-task
membership snapshot. It fails on a zero-ready sample, recovery fingerprint divergence,
ownership/index mismatch, planned drain failure, any Valkey eviction or throttled
command, or failure to corroborate the measured phase envelopes: 224 game
sessions during natural scale-out; 128 game sessions, 23 durable context
sessions, bounded open-loop admission, and transient make-before-break
candidates during the planned transition; and 272 game sessions during
capacity.
