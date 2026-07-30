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

## Production release and cache lifecycle

Production deployment is manual-only and restricted to the current `main`
commit. Every dispatch, including a dry run with credentialed CDK planning,
must provide the GitHub Actions run ID of a successful main-branch Ephemeral
Development Certification for that exact outer-repository commit. That run
must complete the planned suite, hard-crash suite, verified runtime-stack
cleanup, and verified ingress stop; a run for another commit, branch,
repository, or workflow is rejected before AWS credentials are used.

After deployment, the workflow requires one available Valkey 8 Serverless
cache per region and an exact task URL of
`rediss://HOST:6379/?protocol=resp3&cluster=true`.
The production cache has CloudFormation deletion and replacement policies of
`Retain`. The development Serverless cache is deliberately runtime-only and is
deleted after certification; that deletion does not remove the persistent
development foundations. Deleting a retained production cache is a separate
deliberate operator action, not a consequence of deleting its stack.

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
synths. The development synth uses the same mandatory run-lifecycle contexts as
the certification workflow. It fails when
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
slot compatibility. Only the public development certification against actual
ElastiCache Serverless proves TLS, cluster routing across every slot family, and
provider behavior.

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
Both retain a minimum of one. The application task uses two vCPU and four GiB
so the one-task floor has takeover and burst headroom while target tracking is
still observing load. CPU is targeted at 15%, memory at 80%, and both scale-in
and scale-out cooldowns are 60 seconds. Development and production use the
same policy.

`QueueForMatch` uses the authoritative lobby state as its semantic
acknowledgement. The browser and certification client retain only the single
in-memory queue intent, scoped to the exact lobby and authentication identity,
until `LobbyUpdate` reports `queued`/`matched` or `JoinGame` arrives. The server
rejects queueing on a socket that has not joined an explicit lobby. After a
transport replacement clients restore that lobby first; state `waiting` causes
an idempotent replay, while any acknowledged state clears the intent. Do not
treat a successful WebSocket write as durable admission, and do not add a
separate acknowledgement protocol or persistence service for this.

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
   trim-horizon error. A detected bounded-stream discontinuity is deliberately
   task-fatal so no later readiness marker can bless missing snapshots; ECS
   replaces the unready task and clients use ordinary reconnect. Executor
   command or snapshot-request channel depth is not a gateway replica
   dependency.
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

Historical run records below preserve the cleanup inventories produced by the
then-current fully disposable development topology. Those inventories remain
accurate evidence for those runs, but they are not the cleanup contract for new
runs; the persistent-foundation lifecycle described below is authoritative.

**Gate A — natural scale-out.** Run a fixed 128-session / 64-duel
`every-tick` cohort from the two-vCPU minimum task. It retains one stage, the
20-minute runner, eight-minute target-tracking observation budget, and the
existing one-second command-outcome budget. It does not use synthetic CPU,
force the transition, or adapt load from live metrics. CPU or
memory target tracking must add capacity naturally; failure to trigger or to
preserve command continuity fails this gate. After the added tasks are ready in
ECS, Traefik, and the executor control plane, require at least 60 complete
post-ready seconds under the same outcome and all-partition productivity
budgets. A late transition with less evidence fails instead of certifying one
healthy second. Once the automatic scale-out evidence is complete, let this
runner finish and require its WebSockets and authoritative games to reach zero.
Gate A traffic must not be carried into the reset or forced staircase.

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
proved that the same command-bearing cohort remains healthy while the
production target-tracking policy adds capacity naturally. Gate B must separately prove
that its context and transient admission sockets fit on the final survivor,
which remains ready and resolves every command inside budget.

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
second. The load stage runs for ten minutes so ramp time and nonqualifying
observation seconds cannot masquerade as a five-minute proof; acceptance uses
the longest unchanged per-second qualifying interval and still requires at
least 300 consecutive seconds. This capacity envelope never runs on the
one-task baseline.

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
`server` process and sends that PID SIGKILL directly. One prestarted read-only
observer uses a single partition-local Lua operation to read the exact
old-consumer PEL entry and Redis time atomically. A second long-lived observer
first proves the old membership and assignment/lease state, then atomically
latches the first eligible pre-existing successor and that successor's exact
authoritative-event stream tail. It continues on the same connection until it
captures the first later non-replay scheduled output. Both observers bracket
their calls with local start and completion timestamps, and each selected
observation interval must fit inside its deadline. The
proof is anchored to the selected task's millisecond-precision ECS
`executionStoppedAt`, because ECS Exec output disappears with the container and
`stoppedAt` may lag the actual exit. An old-consumer PEL entry must be observed
within two seconds after that exact stop, and the expired member must disappear
with a pre-existing survivor holding a new fenced lease under a later
assignment version within five seconds. Exact exit 137 remains mandatory. The
deadline comparison assumes the certification runner and AWS ECS clocks are
NTP-synchronized. Each runner-side read is bracketed with a monotonic clock and
uses a conservative wall-clock interval; Valkey server timestamps are recorded
for correlation only and are never compared to the ECS timestamp. The
mutating AWS CLI call has retries disabled and acceptance rejects an explicit
OOM or unhealthy stop reason. The
whole-second floor of `executionStoppedAt` is used only as a conservative
cross-host origin for client timing and the pre-crash whole-second load window.
After ownership is proven, a read-only durable-stream query requires the first
non-replay `CommandScheduledV2` after that exact tail anchor to carry a Valkey
stream timestamp no later than five seconds after the exact ECS stop. Because
the tail, assignment, and lease are read atomically from the same partition
slot, buffered pre-crash events cannot satisfy this gate. The run then requires
affected gateway sessions, fresh snapshots/outcome barriers, zero unresolved
commands, that causal partition output, and restored ten-task ECS/Traefik
health.
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
to 76.4--82.8% at 224. That projection originally retained the CPU 70% /
memory 80% targets; the later bounded-telemetry run below superseded the CPU
target after directly measuring the optimized build. It did not supersede the
one-second gate, one-stage 20-minute runner, eight-minute target-tracking
budget, or frozen cohort. Do not adjust the cohort to make a later run pass.
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
224-session / 112-duel cohort, the one-second gate, or any other acceptance
criterion based on that failed run. It did not justify a scaling-policy change;
the later stable exact-source evidence below does. A fresh full planned run and
a separate authorized SIGKILL run remain mandatory.

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
admission deadline, add a recovery cache, adjust the frozen load, or relax any
acceptance gate. That run did not itself justify changing CPU or memory
targets; the later stable exact-source evidence below does.
The next run must record Serverless connection count and any remaining
recovery-read warnings before considering another optimization.

Cleanup for run `30057487544` succeeded. Independent inventory found no
development stacks, Serverless cache, ECS/EC2/EIP resources, ECR repository,
DynamoDB tables, alarms, log groups, DNS records, or scaling targets; the
imported production VPC remained untouched. Treat this run as diagnostic
evidence only. Fresh complete planned and separately authorized SIGKILL runs
are both required.

Exact-source Serverless run
[`30078864960`](https://github.com/lopatin/snaketron-io/actions/runs/30078864960)
used outer commit `949bda23dc6d40de4117649c95a247b3361005f0` and Snaketron
commit `26d96f553977c9538b8e85c90d710855f2c0cad7`. Gate A reached its
frozen 224-session / 112-duel envelope on one task, natural CPU target tracking
moved `1 -> 2`, and the successor recovered 54 games and replayed 499 commands
while taking partitions 5--9. All 2,882 sessions, 1,441 games, and 2,550,557
commands completed with terminal outcomes; all ten partitions remained
productive, with zero disconnects, reconnects, or measured usable-session gap.
Serverless Valkey reported zero throttling and eviction.

The run still failed the unchanged one-second command-outcome gate. Eleven of
323 complete baseline seconds exceeded the limit, with a 1,501-millisecond
maximum; five of 45 movement seconds exceeded it, with a 1,951-millisecond
maximum. The regional metrics reporter was downloading and deserializing about
25--27 MB of full recovery envelopes every 15 seconds on the one-vCPU task.
Observed scans took up to about 1.9 seconds and aligned with the command
latency bursts. The evidence implicates application telemetry competing with
authoritative execution; it does not show command loss, failover failure, or
Serverless throttling. The fresh unchanged-load rerun must confirm the causal
correction.

The bounded correction keeps the existing recovery format and persistence
path. Metrics obtain exact checkpoint size with `STRLEN` and bounded header and
tail slices with same-key `GETRANGE` commands, retaining index identity,
schema/protocol, and exact checkpoint-age checks while reducing the worst
observed sample payload by more than 400 times. The metric deliberately checks
checkpoint framing and index parity rather than validating the full JSON body;
authoritative recovery still deserializes and validates the complete envelope.
Do not add another cache, metadata record, connection pool, or timeout for this
diagnostic concern, and do not weaken the frozen load or latency gate.

Cleanup for run `30078864960` succeeded. Independent inventory found no active
development resource in CloudFormation, ECS, EC2, networking, Serverless
Valkey, DynamoDB, ECR, Route 53, ACM, CloudWatch, Application Auto Scaling, or
IAM. Production stack timestamps and resources remained unchanged and the
production health endpoint stayed healthy. This run is diagnostic evidence;
fresh complete planned and SIGKILL certification remain required.

Exact-source Serverless run
[`30085417447`](https://github.com/lopatin/snaketron-io/actions/runs/30085417447)
used outer commit `19f7fea443684dc3ab23134e0fe596065e8bf4e4` and Snaketron
commit `65e097ae9af948463e4948c181166f1c11b20aac`. It reached the frozen
224-session target with a peak of 112 concurrent duels and completed all 1,344
sessions, 672 games, and 1,203,217 command outcomes. Every command was
scheduled. During all 485 full seconds from reaching the target through the
autoscaling timeout, every partition was productive, no client disconnected or
reconnected, maximum command-outcome latency was 451 milliseconds, and no
second exceeded 500 milliseconds. For samples with at least 100 active games
on one live task, embedded-metric timestamp-to-log delay changed from a
986-millisecond mean and 1,871-millisecond maximum in the preceding run to a
227-millisecond mean and 500-millisecond maximum. Checkpoint size and age
continued to be reported; correctness-failure gauges, Serverless
`ThrottledCmds`, and `Evictions` remained zero.

The run stopped before movement. Eight consecutive one-minute CloudWatch
averages during the full-load interval were 65.69%, 68.95%, 74.77%, 66.58%,
72.25%, 67.98%, 67.95%, and 71.06%. They contained no three consecutive
periods above the then-configured 70% target; no scale-out action occurred and
the service remained at one task. The old load-to-threshold calibration no
longer held on the corrected build. Lower telemetry delay and the changed CPU
profile are consistent with reduced telemetry overhead, but the run does not
certify rebalance continuity. Keep the 224-session gate fixed and use the same
CPU 60% / memory 80% targets in development and production. Sixty percent
leaves 5.69 points below the weakest measured minute for ordinary variance and
the managed alarm plus task-start delay; 65% leaves only 0.69 points and is not
a robust certification or operating threshold.
Do not change any command, handoff, capacity, or crash criterion. Fresh complete
planned and SIGKILL runs remain required.

Exact-source run
[`30089020521`](https://github.com/lopatin/snaketron-io/actions/runs/30089020521)
used outer commit `dad79987a3e3ac3bab23bb3e8f5dc292c25f658b` and
Snaketron commit `960ca7e62866dbd4a5a37cee7acda6cd37f35d0e`. Gate A
naturally scaled `1 -> 2`; all 2,876 sessions and 1,438 games completed, and
all 2,572,277 submitted commands received terminal outcomes, with zero
disconnect, reconnect, or observed usable gap. Maximum command-outcome latency
was 326 milliseconds in the baseline and 892 milliseconds during movement.
Gate B then passed its service-side
`1 -> 10 -> 1` assertions: exactly nine partitions moved each way, all 1,280
sessions and 640 games completed, all 1,144,974 submitted commands received
terminal outcomes, and 114 of 114 planned game handoffs had zero observed
usable gap. Handoff preparation took at most 1,820 milliseconds while the old
socket remained usable; scale-out and scale-in command outcomes peaked at 429
and 756 milliseconds.

All 480 open-loop admissions also completed without an error or reconnect,
with 311-millisecond p99 and 1,672-millisecond maximum readiness; peak
in-flight admission was 12 sessions, below the unchanged 64-session safety
ceiling. The run failed only the next observed assertion: one synthetic
four-session wave was launched 1,394 milliseconds after its predecessor
instead of within the unchanged 1,100-millisecond cadence allowance. This was
load-generator self-interference, not failover: the inline five-second
infrastructure sample overlapped that launch tick and the timer retained the
delay. Because the runner failed closed there, capacity Gate C, automatic
scale-in, complete metrics gates, and SIGKILL did not run. Per-task evidence
also showed transient post-scale-out CPU skew (roughly 82--89% successor
versus 36--41% incumbent with five leases each); preserve that diagnostic in
the final run.

The narrow runner correction moves the one-at-a-time infrastructure sample off
the launch loop and keeps the launch timer anchored. No cohort or acceptance
threshold changes. Hard-crash mode now requires a zero-WebSocket,
zero-authoritative-game, fully drained one-task baseline, and the workflow runs
both suites and retains both statuses when the first returns a failure.

Cleanup for run `30089020521` succeeded. Independent inventory found no active
development stack, ECS task or service, EC2/EIP/NAT resource, security group,
Serverless or node cache, DynamoDB table, ECR repository, log group, dashboard,
or staging DNS record. The shared production VPC, all production stack
timestamps, and both healthy production services remained unchanged.

Exact-source run
[`30425964773`](https://github.com/lopatin/snaketron-io/actions/runs/30425964773)
used outer commit `fe4652b72aa82c39f9773b3beaec412f01cc8308` and
Snaketron commit `c179d966c0aac6c908e0d5ac8999f7bfadcc503a`. Gate A
naturally scaled `1 -> 2`; all 2,790 sessions, 1,395 games, and 2,450,883
commands completed with exact terminal accounting and zero disconnect,
reconnect, or usable-session gap. The successor recovered 51 games and replayed
380 commands. No Serverless Valkey or DynamoDB throttle and no Valkey eviction
occurred.

This was not a passing run with one tolerable failover outlier. Before ownership
movement, 141 of 330 complete seconds exceeded the one-second command budget,
with a 3,617-millisecond maximum and 48 seconds above two seconds. During
movement, 29 of 46 seconds failed with a 2,715-millisecond maximum. The first
task remained near saturation and the two-task aggregate stayed heavily loaded.
Gate A stopped the planned suite; the crash suite did not inject SIGKILL
because a late target-tracking action from the prior phase violated its
one-task entry check.

Steady load produced roughly 242--244 KiB full checkpoints per game every
second, dominated by the 512-entry exact command-outcome maps. Keep the
one-second cadence and persistence format, but use the aligned 128-entry client
outbox and server exact-result window. Disconnected or resynchronizing clients
create no command identities; at the certified ten-command-per-second profile
the bound also permits 12.8 seconds of continued submission without outcomes.
The contiguous watermark still fences all older contiguous identities. The
load runner now separately gates every complete post-ready second through the
stage finish and requires at least 60 such seconds, so a quiet movement window
or one healthy trailing second cannot hide later backlog. Crash mode suspends
target tracking before establishing and recording an independent `1/1/0`
baseline, then cleanup restores enabled
`1/1/0`. The `PendingCompletionBacklogAlarm` retains the one-minute maximum
across three periods. Regional gauges are emitted only with a real value by one
reporter while other tasks emit zero on the same environment-wide series, so a
minimum statistic would suppress a real backlog after scale-out.

The 128-entry bound also exposes one adversarial case that must be durable:
after 128 sparse results above an unresolved lower gap, immediately ACKing a
later rejection would allow that identity to be reused after a crash. The
executor therefore stores one per-session rejection fence in the existing
recovery checkpoint. A live first rejection is journaled through the normal
outcome-publication path and remains pending until the fence is checkpointed;
bootstrap may checkpoint and ACK without first publishing an incremental
event. Reconnect outcomes carry the fence. Clients clear covered entries,
retain lower gaps, and rotate the command session only when those lower entries
have resolved. This reuses the existing decision journal and checkpoint; it
adds no Redis key or per-command checkpoint.

Cleanup and an independent absence audit found no development resources. All
nine production stack timestamps and resource identities were unchanged, and
production remained healthy. Fresh complete planned and SIGKILL certification
remain required; do not relax the one-second ordinary-operation gate or any
fixed cohort.

Follow-up exact-source run
[`30444237957`](https://github.com/lopatin/snaketron-io/actions/runs/30444237957)
used outer commit `e1dfb875633f80d9528e19ea1c931e0f72ec8bc7` and
Snaketron commit `e70d1185c4cba2aeb3eb44867b28357399afabf1`. It again
proved that 224 sessions were not a valid headroom-preserving trigger: the
single task remained at approximately 100% CPU for six complete minutes.
Every one of 2,846 sessions, 1,423 games, and 2,516,545 commands completed with
exact terminal accounting and no disconnect, reconnect, or usable-session gap,
but 43 baseline seconds exceeded one second, 13 exceeded two seconds, and the
maximum was 3,233 milliseconds. Movement stayed below two seconds at 1,739
milliseconds; post-ready delivery peaked at 1,689 milliseconds. This is still
a failed run, not the accepted sub-two-second transition exception.

Settled post-scale-out Container Insights placed the original task at roughly
823--877 CPU units and the successor at 405--433, with about 7.5% idle CPU per
task. The fixed Gate A trial is therefore recalibrated once to 144 sessions /
72 duels: above Gate B's 128 command-bearing sessions and projected to place a
one-task origin around the desired 70--80% range. This projection is not a
pass. The exact-source AWS run must still trigger CPU/memory target tracking
naturally and meet the unchanged one-second budget; otherwise Gate A fails.
This supersedes the historical 224 freeze without changing the 20-minute
stage, four-session-per-second ramp, CPU/memory targets, protocol, or any
correctness condition.

The crash invocation reached ten healthy tasks but did not inject SIGKILL
because four fresh ECS Exec agents were still `PENDING` at a one-shot setup
check. The runner now polls the exact verified task cohort for at most 120
seconds, while the actual kill remains single-attempt. Cleanup deleted only the
Server, Serverless Valkey, and Monitoring runtime stacks. It retained the
protected Network/EIP/EBS/TLS, ECS, ECR, and DynamoDB foundations and stopped
the ingress instance, which is the current cleanup contract.

Exact-source run
[`30454722583`](https://github.com/lopatin/snaketron-io/actions/runs/30454722583)
used outer commit `8f476b972b000e99d0b4da5de7705f659dcfac6e` and
Snaketron commit `eaba0de0dc89abe6fa00fef13c8cd903b06658e0`. It proved
that the automatic ownership movement itself was healthy: all 48 complete
movement seconds stayed inside the one-second command budget, with a
368-millisecond maximum, exact outcomes, every partition productive, and no
disconnect or reconnect. The 144-session origin was not a valid
headroom-preserving trigger. Its one-task CPU averaged 87.61--90.43% for four
complete minutes before the managed policy acted. Four of 296 baseline seconds
failed the one-second gate, including 2,677- and 2,361-millisecond seconds
before the scale-out action. One of 818 settled post-ready seconds reached
1,271 milliseconds. This is ordinary capacity pressure, not the accepted
sub-two-second transition exception, so Gate A correctly stopped Gates B and C.

The minimum production correction is to scale at the already measured safe
one-task plateau instead of weakening the latency gate. Historical exact-source
run `30046381977` held 128 sessions / 64 duels for eight minutes at
45.18--48.83% CPU with a 488-millisecond maximum, exact command accounting,
and no disconnect, reconnect, or checkpoint failure. Development and
production therefore use a 40% CPU target and Gate A uses that same fixed
128-session cohort. The weakest historical CPU minute remains 5.18 points
above the target, comparable to the margin that previously justified 60%.
`minTasks=1`, one-vCPU task size, the 80% memory policy, all cooldowns, and the
one-second command gate remain unchanged. This is an evidence-backed
headroom policy requiring a fresh current-build run, not a passing result.

The separate crash load completed all 1,376 sessions and 688 games with no
failure. Twenty-two affected clients automatically received fresh snapshots
from other task identities in 2,506--2,706 milliseconds, and ECS recorded the
one selected server container exiting 137. Formal crash acceptance could not
run: the remote shell killed the essential process before its following marker
crossed ECS Exec, then the local client waited for Session Manager's 20-minute
idle timeout. The single crash command now kills the exact server PID directly
while a read-only observer samples the selected partition.
Certification uses ECS `executionStoppedAt` and exact exit 137 instead of ECS
Exec stdout, which is inherently lost during container teardown. This adds no
production endpoint or second crash action. A fresh run must still capture the
immediate PEL, survivor lease, output, and reconnect proof.

This run again reused the exact Network stack, ingress instance, root EBS, EIP,
hostname, and TLS certificate. CloudTrail recorded no development DNS or ACME
change. Cleanup removed only Server, Serverless Valkey, and Monitoring, stopped
the same ingress instance, and retained the four reusable development
foundation stacks.

Exact-source run
[`30475852468`](https://github.com/lopatin/snaketron-io/actions/runs/30475852468)
used outer commit `be4040cafd8ddce72be51eab63ff4de158994e55` and
Snaketron commit `2c3f6c06a725b585c6f161aa7a32cdf98938b547`. Gate A
naturally scaled `1 -> 3`. All 1,664 sessions, 832 games, and 1,476,683
commands completed with exact terminal accounting and zero disconnect or
reconnect. It was still not a passing run: 22 of 332 baseline seconds, two of
46 movement seconds, and 17 later settled seconds exceeded the unchanged
one-second outcome budget. The maximum was 5,852 milliseconds. Gate A
therefore correctly stopped the planned staircase and capacity gate.

Twenty-nine ordinary failures clustered in the first three seconds of a minute.
The elected reporter was serially inspecting every full active recovery record;
its top-of-minute collections took as long as 4,719 milliseconds and coincided
with receipt collapse only on that task's four owned partitions. Separately,
partition 3 failed closed after six fenced event publications timed out, then
restarted locally and recovered six games plus 124 commands without loss. A
pending-completion index read and group-aware trim shared its hot dispatcher;
the evidence does not prove which queued operation initiated that incident.

The narrow correction preserves exact telemetry semantics but fetches bounded
recovery headers and tails in fixed 32-key same-slot batches, bounds the
complete normal collection to two seconds, and emits a separately gated
`RegionalCollectionFailures` metric so a skipped collection cannot look like
zero mismatches. One task-wide maintenance dispatcher now owns only
pending-completion index reads and bounded approximate stream trimming.
Separate at-most-one background completion and trim workers keep both off the
authoritative partition loop and keep DynamoDB completion effects from starving
trim. The 750-millisecond fenced-operation timeout, checkpoint cadence,
partition topology, CPU/memory policies, and acceptance budgets are unchanged.

The separate exit-137 run completed all 788 sessions, 394 games, and 705,108
commands observed before the proof failure. Twenty-six killed-task sessions
recovered through fresh snapshots; 27 balanced disconnect/reconnect attempts
include one retryable replacement socket before final game readiness. Recovery
took 2,311--2,518 milliseconds from client detection. ECS
`executionStoppedAt` showed successor assignment and a new fenced lease in
about 3.3 seconds and all 53 sampled pending entries by 3.88 seconds. The rich
PEL/status command needed just over five seconds to finish one coherent sample,
so the harness failed even though product takeover was inside the five-second
budget. Crash certification now keeps that rich observer only for the
unchanged two-second PEL proof and uses a prestarted lightweight coherent
assignment/lease/membership observer for the unchanged five-second ownership
proof. No production endpoint or coordination mechanism was added.

Cleanup for `30475852468` deleted only the Server, Serverless Valkey, and
Monitoring runtime stacks and stopped ingress. The fixed hostname, DNS, EIP,
certificate-bearing EBS volume, shared production VPC, and reusable
Network/ECS/ECR/DynamoDB foundations remained. Fresh complete Gate A/B/C,
automatic scale-in, and exit-137 evidence remain mandatory.

Exact-source run
[`30486700133`](https://github.com/lopatin/snaketron-io/actions/runs/30486700133)
used outer commit `d2ba3692b4841f6ea0ba1a929c164239c3bc0b8b` and
Snaketron commit `9d176c3be3a4510cfd86f583ca5e2aa915312125`. Gate A
naturally scaled `1 -> 2`. All 1,664
sessions, 832 games, and 1,488,138 commands completed with exact outcomes and
zero disconnects or reconnects. The transition itself passed: all 50 movement
seconds stayed below one second with a 608-millisecond maximum, and all 789
post-ready seconds passed with a 461-millisecond maximum. Five of 323
one-task baseline seconds still failed, reaching 1,701--2,531 milliseconds, so
Gates B and C correctly did not run.

The task averaged 70.91--84.87% CPU and peaked at 90.04% on one vCPU, while
memory remained below 9%. The managed CPU alarm did not act until six minutes
after load began because its one-minute observations arrived late and required
three breaching datapoints. The final failure cluster stalled receipts across
every partition and then drained in the next second. Valkey and DynamoDB
reported zero throttling, Valkey reported zero evictions, and no executor,
lease, fencing, completion, or checkpoint failure occurred. The evidence is
most consistent with one-task compute pressure during target-tracking
observation, not a rebalancing failure.

The regional reporter contributes only about 0.07% of Valkey traffic and
normally finishes in about 100 milliseconds on its separate connection. It is
not a sufficient explanation for Gate A and is not treated as the capacity
fix. Three valid collections did exceed the old 500-millisecond telemetry
deadline by 12--45 milliseconds, so the deadline has two seconds of
operational margin and any `RegionalCollectionFailures` remains a hard
certification failure. The outage tracker uses the actual gap between
successful ownership samples, so a longer regional scan cannot hide a lease
outage. INFO volume was not correlated with the failed seconds; no speculative
metrics concurrency or production log suppression is added.

At that point, development and production used two-vCPU / four-GiB tasks, retained
`minTasks=1`, and targeted CPU at 30%. Diagnostic run `30496453531` tested outer
commit `9c729f8f075a556d3caee6e2a37302f8da0981e6` and nested commit
`a424647639b156abfc8d0731b3fbf51ecbded634`. Its configured 35% managed 3/3
CPU alarm never fired: Gate A timed out, so planned Gates B and C did not run.
The nine consecutive load periods measured 30.33--35.36%. A 30% target would
have supplied seven valid three-period alarm windows, while whole-percent
targets from 31% through 35% would have supplied at most one. Fresh Gate A must
prove the CPU-only scale-out; memory stays at 80%, cooldowns remain 60 seconds,
and the load and one-second acceptance gates are unchanged.

The interrupted Gate A stimulus still supplied clean diagnostic one-task
capacity evidence: all 768 sessions, 384 games, and 687,693 command outcomes
completed exactly, with zero failed sessions and zero evaluated command
seconds above one second. The worst per-second command-outcome latency was 914
milliseconds. This is not formal Gate A or capacity acceptance because the
stage plan did not complete; it isolates the observed failure to the 35%
managed scaling trigger rather than workload instability or command loss.

The independent exit-137 suite also exposed proof issues after successful
product recovery. ECS recorded the selected task exiting 137. A pre-existing
survivor completed fenced partition-6 bootstrap 3.72 seconds after exact stop,
recovering 15 games and replaying 151 commands. All 28 affected clients
received fresh snapshots in 2.512--2.658 seconds from exact stop; all 954
sessions, 477 games, and 855,103 terminal outcomes completed. Another 138
sessions admitted after the stop also completed, and Traefik never had zero
healthy backends.

The rich post-stop status observation began at +168 milliseconds and found
151 exact pending entries under the killed lease, but its multi-read interval
completed at +2.300 seconds. The selector correctly refused to claim the
unchanged two-second PEL proof. A prestarted narrow partition-local atomic
`TIME` / consumer-filtered `XPENDING` observer now supplies that proof without
changing the deadline. The prior load report showed a post-stop receipt bucket
with a 4.2-second upper bound, but receipt time could not exclude a buffered
pre-crash event and is not accepted as causal proof. The corrected harness atomically
anchors the new owner's partition event-stream tail with its assignment and
lease, then immediately saves the first exact later fenced, non-replay
`CommandScheduledV2` stream entry. The bounded, paged read prevents an
unbounded diagnostic query, and immediate capture prevents the approximately
bounded event stream from trimming valid recovery evidence while ECS exposes
the exact stop timestamp. The saved ownership/output pair is accepted only
when the replacement assignment was computed after that exact stop and both
timestamps correlate to the unchanged five-second window. The unrelated-stop
check also snapshots and excludes tasks already stopping before crash evidence
begins.

The independent crash phase of run `30496453531` then formally passed all 12
hard-crash checks. Its one selected ECS task exited 137. The narrow PEL
observation completed 1.161 seconds after exact `executionStoppedAt`; the
survivor's fenced owner-ready sample completed at 4.022 seconds and its first
causal authoritative output appeared at 4.006 seconds. All 32 affected clients
recovered with a formal 4.314-second upper bound. All 1,378 sessions, 689
games, and 1,234,049 command outcomes completed with exact accounting and no
pending commands. Traefik recorded zero zero-backend samples or scrape errors
across 308 observations, and ECS recorded no unrelated task stop.

Cleanup for `30486700133` succeeded and honored the cost boundary: only Server,
Serverless Valkey, and Monitoring were removed; ingress was stopped; and the
fixed hostname, DNS, EIP, certificate-bearing EBS volume, shared production
VPC, and reusable Network/ECS/ECR/DynamoDB foundations were retained. No
certificate was created. Fresh complete Gate A/B/C, automatic scale-in, and
exit-137 evidence remain mandatory.

Exact-source run
[`30503270454`](https://github.com/lopatin/snaketron-io/actions/runs/30503270454)
used outer commit `d31d64f93288aa246bb2038ec11b1adf401d05fc` and
Snaketron commit `463a164bb66bb3e187c4681274bb6edb28ba6e88`. Gate A
naturally scaled `1 -> 2` and passed every fixed check: all 1,664 sessions and
1,477,620 commands completed exactly, with zero failed seconds across 374
baseline, 44 movement, and 747 post-ready seconds. Gate B moved nine partitions
in each direction; all 1,280 game sessions, 480 admissions, and 117 planned
handoffs completed with no reconnect or usable-session gap. Gate C completed
all 1,748 sessions and 874 games and held a final 403-second qualifying streak
against the unchanged 300-second requirement. It retained three earlier
nonqualifying seconds at 1,111 milliseconds, 1,006 milliseconds, and 127 fully
joined duels rather than treating them as part of that continuous streak.

The run is not complete certification. With load at zero, target tracking
successfully stepped from ten tasks to two, approximately one task every two
minutes. The local 20-minute ceiling expired while the last completed
activity's 60-second cooldown still prevented the final action. During that
AWS-only wait, both certification Valkey SSM tunnels reached their inactivity
timeout. The crash suite consequently failed its initial read-only control
snapshot and did not inject a crash.

Automatic scale-in observation now has a 40-minute fail-only ceiling covering
the low-alarm window, bucket alignment, eight subsequent
cooldown/evaluation cycles, and final ECS convergence. It still returns
immediately at desired/running/pending `1 / 1 / 0`, and this ceiling is not a
product scale-in SLO. Once per minute the waiter performs a read-only
executor-status query whose cluster bootstrap PINGs both advertised Serverless
Valkey ports, after first checking both forwarding paths. This keeps the
existing SSM sessions active and fails closed if the control path disappears;
it does not change desired count, assignment, leases, autoscaling policy, or a
user-visible acceptance budget.

Cleanup for `30503270454` removed Server, Serverless Valkey, and Monitoring and
stopped ingress. The same protected Network stack, instance, EIP, DNS record,
certificate-bearing EBS volume, shared VPC, and ECS/ECR/DynamoDB foundations
remained. No deployment-time Network update or certificate creation was
observed. Fresh complete Gate A/B/C, automatic scale-in, and exit-137 evidence
remain mandatory on one exact source.

Run
[`30513482816`](https://github.com/lopatin/snaketron-io/actions/runs/30513482816)
confirmed that target tracking acted inside Gate A's fixed 480-second decision
window even though the new Fargate task needed another 11 seconds to become
ready. Decision evidence and cold-task readiness are now checked separately.
Its independent exit-137 phase passed all 12 checks, with successor ownership
and causal output in about four seconds and exact completion of 1,380
sessions, 690 games, and 1,235,425 commands.

Run
[`30516147896`](https://github.com/lopatin/snaketron-io/actions/runs/30516147896)
then found the unchanged fixed workload at 18.21--22.13% one-minute CPU rather
than the prior identical image's approximately 28.5--35.2%, despite only a
0.12% command-acknowledgement throughput difference. CPU target tracking is
therefore 15%, above the approximately 2% idle level and below the weakest
loaded minute with margin. No load, memory target, cooldown, task range,
decision window, or command gate changed.

That run's active-game crash recovery was clean: all 23 affected clients
recovered in 2.542--2.738 seconds from detection, successor ownership and
causal output resumed in about 4.1 seconds, and 1,232,737 commands resolved
exactly. A lobby whose host sent `QueueForMatch` 162 milliseconds before the
task stopped exposed a separate at-most-once client-boundary bug: the restored
lobby remained `waiting`, so both members timed out. The client now retains
one in-memory queue intent and replays it only when restored authoritative
state remains `waiting`; `queued`, `matched`, or `JoinGame` clears it.

Both runs deleted only their runtime Server, Serverless Valkey, and Monitoring
stacks, stopped the same ingress, and retained identical foundation and TLS
identities. Fresh complete Gate A/B/C, automatic scale-in, and hard-crash
evidence remain mandatory on one exact source.

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

Initial gateway readiness is stricter than merely anchoring those readers. Each
partition replica publishes one boot-unique snapshot request and stays unready
until it consumes the matching completion marker from the same ordered event
stream. The owner appends that marker only after every active actor has
published its requested snapshot at the next ordinary checkpoint; command
dispatch continues while the completion waiters are pending. The replica
retries the same request identity if no executor had subscribed yet. Empty
partitions still receive a marker. Executor membership may become assignment
eligible before this gateway-only proof, which prevents first-task bootstrap
deadlock while Traefik continues to exclude the unready gateway.
The readiness fan-out is bounded to three seconds and the replica retries the
same durable request every two seconds. Timed-out actor waiters are pruned, so a
terminal-materialization outage can keep a new gateway honestly unready without
accumulating permanent workers or weakening existing gateways.

Ordinary cold joins do not repeat that partition-wide fan-out. Their request
names one game, is published at most once per 500 milliseconds while the
gateway polls its replica every 100 milliseconds, and is delivered to the actor
with a nonblocking mailbox send. A full or temporarily absent actor is retried;
it cannot hold the partition command reader or amplify one join across every
game. Partition startup and detected stream-gap repair remain partition-scoped.

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
recovery. Pending-completion index scans and bounded approximate command-stream
trimming use one separate task-wide maintenance dispatcher and run only in
at-most-one background workers per partition; they must never be awaited by the
authoritative partition loop.
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
The task-definition image tag must equal the clean outer-repository checkout
commit and its exact ECR tag lookup must resolve one valid digest. Every running
task must use that image URI and digest; earlier commit tags may legitimately
alias the same content digest. Both the outer checkout and Snaketron submodule
must be clean, and the submodule HEAD must equal the outer commit's gitlink. A
missing tag, identifier, metric endpoint, account confirmation, or exact source
binding fails before mutation.

The runner changes only that verified non-production ECS service. Cleanup
retries restoration, waits for the original desired/running count, restores the
original enabled autoscaling state, and writes cleanup.json; inability to verify
restoration fails the run. It also refuses known production hosts, production
environment tags, and prod-labeled ECS identifiers:

The runner passes the load tool's generic production-host confirmation because
that tool conservatively protects every snaketron.io subdomain, including the
fixed `dev.snaketron.io` development hostname. The stricter account/resource
identity gate above runs first; this flag is not permission to target a
production-tagged deployment.

```bash
export SNAKETRON_STAGING_CONFIRM=RUN_SNAKETRON_STAGING_CHAOS \
SNAKETRON_STAGING_TARGET=https://dev.snaketron.io \
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

The “Ephemeral Development Certification” workflow reuses the fixed public
`dev.snaketron.io` endpoint and its protected development foundations. The
Network foundation imports the production Network stack's VPC read-only and
owns only development security groups, one EIP and A record, and the Traefik
instance. Its root EBS volume retains Traefik's ACME account and TLS certificate
state while the instance is stopped. The reusable ECS cluster, ECR repository,
and DynamoDB tables also remain provisioned between runs. None of these
foundations carries a run expiry.

For a certification run, the workflow starts that same ingress instance,
creates only the Serverless Valkey, Server, and Monitoring runtime stacks, opens
three SSM sessions (Valkey 6379, Valkey 6380, and Traefik metrics), runs the
planned and hard-crash commands in that order, and uploads both evidence
directories. Cleanup deletes and verifies the absence of those three
run-identified runtime stacks, then stops and verifies the persistent ingress.
It preserves the EIP, `dev.snaketron.io` record, EBS/TLS state, security groups,
ECS/ECR/DynamoDB foundations, and Traefik log groups. It must also prove that
the shared production VPC still exists and must never create, replace, or delete
that VPC's routes, endpoints, or flow logs.

Normal ACME renewal for the fixed hostname can still occur when necessary, but
certification runs do not derive new hostnames or request one certificate per
deployment. No workflow-attempt certificate quota is part of this lifecycle.

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
report schema 11 includes each session's launch
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

The automatic scale-in waiter returns immediately on success but permits up to
40 minutes before failing. This is a conservative certification observation
ceiling, not a service SLO: the managed low alarm may need fifteen one-minute
datapoints, after which a ten-to-one contraction can require eight more
cooldown/evaluation cycles and final ECS convergence. The zero-load waiter
keeps both SSM Valkey tunnels active with a once-per-minute read-only cluster
control probe; inability to read the control path fails certification.

Report schema 11 records coordinator-observed, server-confirmed peak
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
physical resend. A sparse-window rejection fence is protective behavior for an
invalid or pathological command session, not healthy load-test throughput; any
such fence in Gate A, B, or C fails that certification session immediately
before covered commands can be counted as successful outcomes.
After an authoritative terminal snapshot, a
`CommandOutcomesComplete.terminal_rejection_reason` from a current terminal
recovery envelope rejects any same-game identities that remain pending after
exact outcomes. The report records these as
`TerminalBarrierRejectedOutcome`; their latency ends at the later of the
barrier's dedicated-reader receipt and the original command send. A reasonless
barrier—used when recovery state is absent, expired, malformed, or
nonterminal—never clears pending identities and therefore fails closed. Treat
any terminal default observed before the terminal snapshot as a protocol
failure.
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
command, or failure to corroborate the measured phase envelopes: 128 game
sessions during natural scale-out; 128 game sessions, 23 durable context
sessions, bounded open-loop admission, and transient make-before-break
candidates during the planned transition; and 272 game sessions during
capacity.
