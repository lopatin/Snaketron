# PRD: Seamless ECS Autoscaling and Crash Recovery

| Field | Value |
| --- | --- |
| Status | Direct-only implementation acceptance draft |
| Product | Snaketron regional game service |
| Owners | Engineering / Product |
| Last updated | 2026-07-29 |
| Scope | Executor ownership, task lifecycle, WebSocket continuity, matchmaking safety, readiness, and autoscaling |

## 1. Executive summary

Snaketron must treat abrupt ECS task loss as the normal game-recovery path. Executor ownership must move independently of WebSocket placement, and correctness must never depend on a task receiving SIGTERM or waiting for its games to finish.

> With Valkey available and at least one surviving ready task, executor partitions must automatically recover on another task without losing acknowledged player commands or applying authoritative effects more than once. Planned scale-up must not reconnect WebSockets. Planned scale-down must use make-before-break WebSocket handoff so supported clients retain a usable connection throughout the handoff. No shutdown path may wait for a game to finish.

The minimum correct design is:

- short-lived task membership and a versioned, explicit partition assignment map in regional ElastiCache Serverless for Valkey;
- one uniquely tokened, fenced lease for the active authority of each partition;
- executor-only Redis consumer groups for durable command takeover;
- the existing per-game snapshots, extended into recovery checkpoints with command cursors and deduplication state;
- atomic matchmaking claims and idempotent completion side effects;
- truthful readiness and automatic Traefik health routing;
- client command resend plus make-before-break WebSocket handoff for planned task removal.

This PRD deliberately does not introduce a separate gateway service, a new consensus system, whole-partition snapshots, a generic WebSocket event log, a self-managed cache cluster, or custom autoscaling signals. ElastiCache Serverless itself is TLS-only and cluster-mode-only; the application must therefore be Redis-Cluster-aware. CDK pins Serverless Valkey major 8 for its faster managed burst expansion, without adding a paid ECPU minimum or a correctness-affecting maximum.

## 2. Product problem

The superseded executor path exposed the following failure modes.

| Pre-project behavior | Historical repository evidence | User or service risk |
| --- | --- | --- |
| Every task competes for every partition lease. There is no explicit placement plan. | `server/src/game_server.rs` starts a `ClusterSingleton` for all ten partitions on every task. | Scale-up does not reliably rebalance work, and scale-down relies on lease expiry. |
| Lease ownership is the reusable server ID and writes are not fenced by a unique acquisition. | `server/src/cluster_singleton.rs` renews by `server_id`; `GameBus` writes do not validate authority. | A paused or detached old executor can resume and publish after a replacement acquires the lease. |
| A new executor subscribes at the command-stream tail. | `GameBus::subscribe_to_partition` resolves the current tail before `run_game_executor` starts. | Commands written while no executor is subscribed can be skipped permanently. |
| Command streams are trimmed with approximate `MAXLEN 8192`. | `server/src/game_bus.rs`. | A command required for takeover can be trimmed, including a future pending consumer-group entry. |
| Recovery snapshots contain `GameState`, but not the executor command cursor, dedupe watermark, or server command counter. | `GameBus::store_snapshot` and `GameEngine`. | Replayed commands can be lost, assigned inconsistent IDs, or applied more than once. |
| Partition recovery scans every `game:snapshot:*` key and reads snapshots serially. | `load_stored_snapshots` in `server/src/game_executor.rs`. | Recovery time grows with all regional games instead of the failed partition's active games. |
| Shutdown listens only for Ctrl+C, cancels all work immediately, and waits on handles serially. | `server/src/main.rs` and `GameServer::shutdown`. | ECS SIGTERM does not initiate the intended drain, and total shutdown time can exceed a bounded task deadline. |
| Health endpoints report success without checking readiness; replica readiness is hard-coded true. | `api/regions.rs` and `ReplicationManager::is_ready`. | Traefik can route new users to a warming, broken, or draining task. |
| The browser reconnects after a fixed two seconds and marks authentication complete immediately after sending the token. | `client/web/contexts/WebSocketContext.tsx`. | Recovery is slower than necessary and may issue game/lobby requests before authentication actually succeeds. |
| Transport cleanup explicitly leaves a lobby. | WebSocket cleanup calls `LobbyJoinHandle::close`. | A task crash or planned socket handoff can remove lobby presence or delete a solo lobby. |
| Match creation and queue removal are separate operations. | `matchmaking.rs` creates the game before removing lobbies from all queues. | Concurrent workers or a crash can double-match a lobby or create a partial match. |
| XP and MMR persistence use additive updates after completion. | `xp_persistence.rs`, `mmr_persistence.rs`, and `game_executor.rs`. | A replayed completion can apply durable rewards more than once. |
| Traefik uses sticky cookies and a health endpoint that is always healthy. | `cdk/lib/fargate-stack.ts`. | A reconnect can be biased back to a draining task and route withdrawal is not truthful. |

The existing gateway/executor decoupling is correct and must be preserved: WebSocket handlers already publish commands by partition to Valkey, and every task already maintains replicas for all partitions. Authoritative execution does not need to run on the task holding a player's WebSocket.

The legacy `specs/HighAvailability.md` describes a superseded Raft architecture. This PRD is the source of truth for the Redis/Valkey-based autoscaling design.

## 3. Users and desired outcomes

### 3.1 Active player

- A planned scale-up is invisible.
- A planned scale-down does not disable controls, show a disconnected/stale overlay, or lose game progress.
- If a task crashes, the game reconnects and resumes automatically without a page reload or user action.
- A player command is never silently lost after the client has been told it was accepted.

### 3.2 New or matchmaking player

- Planned task changes do not make the regional service unavailable while another ready task exists.
- A missed transient `MatchFound` notification does not strand the player; reconnect discovers the committed match.
- A lobby is not destroyed merely because its WebSocket transport moved or failed.

### 3.3 Operator

- ECS may stop or kill any task without selecting individual games or waiting for them to complete.
- Scale-out produces a balanced executor placement; scale-in moves only work that must move.
- Failover correctness is observable through assignment versions, lease tokens, command pending state, checkpoint age, and recovery metrics.
- Traefik discovers and removes tasks automatically; no manual backend update is required.

## 4. Guarantee boundary

“Zero disruption” has different meanings for planned and abrupt events:

| Scenario | Required guarantee |
| --- | --- |
| Planned scale-up | No WebSocket reconnect, no input interruption, no stale/disconnected UI, and no lost or duplicate authoritative effect. Executor movement is invisible. |
| Planned scale-down with another ready task | Supported clients maintain at least one authenticated, game-ready socket throughout make-before-break handoff. Lobby/game context and pending commands survive. New users retain service availability. |
| Abrupt task crash with another ready task | Automatic WebSocket reconnect and game recovery without user action. A brief reconnect or stale indication is allowed because the transport already failed. Acknowledged state and commands survive. |
| Abrupt crash at `minTasks=1` | An availability gap is allowed until ECS starts a replacement. State resumes automatically if Valkey and a retained recovery checkpoint remain available. |
| Regional Serverless Valkey outage or data loss | Availability and state recovery during the outage are outside this release's guarantee. Restoration must not intentionally bypass fencing or idempotency. |
| Single Traefik/NAT failure | Availability is outside this release's guarantee. |

Literal uninterrupted transport cannot be promised after a hard gateway crash. The zero-gap objective applies to the cooperative, planned path.

For this PRD:

- A **supported client** runs the deployed drain/command protocol, can reach the regional endpoint, has JavaScript execution active, and keeps its old transport healthy until the planned handoff completes. A sleeping, offline, or suspended browser falls back to normal reconnect when it wakes.
- Certification has three separate load gates and must not infer one from
  another:
  - the **natural scale-out gate** runs 128 authenticated game sessions / 64
    duels with `every-tick` commands from the one-task minimum until CPU or
    memory target tracking adds capacity while command continuity remains
    inside budget;
  - the **planned-transition gate** starts from a clean one-task baseline and
    runs 128 game sessions / 64 duels with `every-tick` commands covering all
    fifty partitions through forced `1 -> 10 -> 1`, plus 10 idle, 10 lobby, and
    three unmatched matchmaking probes. During scale-in it also runs bounded
    open-loop idle admission at four starts per second, with a 64-session
    in-flight safety ceiling and a one-second post-ready hold. The high-load
    scale-out clients must be gone before this gate begins; and
  - the **ten-task capacity gate** configures 272 game sessions / 136 duels and
    requires at least 256 authenticated game sessions / 128 concurrent duels,
    after ramping at four new sessions per second, with `every-tick` commands
    across fifty partitions for at least five continuous minutes.
  Make-before-break candidate sockets are additional transient traffic. Every
  gate reports checkpoint write rate/size, pending backlog, per-task sockets,
  CPU/memory, and Valkey/ingress saturation. Raising a required minimum requires
  another certification run.
- A command is **semantically acknowledged** when the client receives its matching terminal `CommandScheduledV2`, `CommandRejected`, or resolved recovery outcome. Gateway `XADD` success alone is not a player-visible acknowledgement.

## 5. Goals

1. Make crash recovery authoritative: SIGTERM accelerates recovery but is never required for it.
2. Rebalance all fifty executor partitions automatically on task arrival and departure.
3. Prevent any stale executor from committing authoritative output after its lease term ends.
4. Make executor commands durable across periods with no active executor.
5. Resume each active game from a recent per-game checkpoint without scanning all game keys.
6. Preserve logical exactly-once command effects over at-least-once delivery.
7. Preserve gateway/executor independence and avoid reconnecting sockets for executor-only movement.
8. Provide zero-usable-session-gap WebSocket handoff on planned task removal for supported clients.
9. Make matchmaking commits atomic and recoverable without Pub/Sub.
10. Route only to ready tasks and use CPU/memory target tracking without adding a custom game-specific scaling signal.

## 6. Non-goals

- Self-managed Valkey nodes, replication groups, shards, or failover policy; those are delegated to ElastiCache Serverless.
- Redundant managed ingress, NAT redesign, or regional disaster recovery.
- Raising the service floor above `minTasks=1`.
- Game-count, queue-depth, or other custom autoscaling signals.
- A separate ECS service or binary for gateways and executors.
- Moving existing WebSockets during scale-up.
- Waiting for games to finish during shutdown.
- Whole-partition snapshots, snapshot deltas, or a new checkpoint service.
- A generic durable outbound WebSocket session/event replay system.
- Persistence of unacknowledged input across a browser refresh, tab crash, or device change.
- A new consensus system, Kafka, service mesh, or workflow engine.
- A matchmaking singleton as a correctness dependency.
- Manual Traefik updates in response to node events.
- DynamoDB capacity redesign in this phase.
- Continuous availability when the regional Serverless Valkey dependency or single ingress dependency is unavailable.

## 7. Core invariants

These are safety properties, not performance targets. No observed violation is acceptable.

1. A partition has at most one lease token whose authoritative writes can commit.
2. Desired placement and active authority are separate:
   - the assignment map selects who may next acquire and renew;
   - the exact, unexpired lease token determines who may still write.
3. Every partition has one desired owner whenever at least one eligible task exists.
4. A stale lease token cannot append an event, commit a checkpoint, acknowledge a command, or finalize a game.
5. Redis command transport is at least once; each stable client command ID has at most one logical authoritative effect.
6. A client-visible accepted command remains recoverable through either a checkpoint or an unacknowledged durable command-stream entry.
7. For a successfully finalized game while DynamoDB is available, every required completion, XP, MMR, and similar durable effect eventually applies exactly once per `game / user / effect` idempotency key.
8. A lobby or player can belong to at most one committed match.
9. A committed match can be discovered without receiving its Pub/Sub notification.
10. Starting from the same recovery envelope, ordered logical command inputs, and target logical tick, the authoritative `GameState` fingerprint matches the uninterrupted deterministic control. Transport IDs, lease metadata, and duplicate physical deliveries are excluded from the oracle.
11. Planned scaling never waits for a game to finish.

## 8. Target architecture

~~~mermaid
flowchart LR
    C["Browser client"] <-->|"WebSocket"| G["Any ready ECS task / gateway"]
    G -->|"XADD player command"| CS["Partition command stream"]
    A["Assignment coordinator singleton"] -->|"Versioned desired owner map"| V["Regional Serverless Valkey"]
    V --> A
    V --> L["Fenced partition lease"]
    CS -->|"Consumer group / pending takeover"| E["Assigned partition executor"]
    L --> E
    E -->|"Fenced events and checkpoints"| V
    V -->|"Partition event stream"| R["Replica readers on every ready task"]
    R --> G
    E -->|"Idempotent completion effects"| D["DynamoDB"]
~~~

The assignment coordinator is control plane only. Existing assignments and active leases continue if the coordinator is temporarily absent. Serverless Valkey remains the regional coordination and recovery dependency.

## 9. Functional requirements

### R1 — Task identity, lifecycle, and membership

1. Every process must have a unique boot identity. It must not be only a reusable server ID, hostname, or ECS service name.
2. Each task must publish regional membership in Valkey with:
   - boot identity and ECS task identity when available;
   - the exact current executor protocol version, used only to fail closed on a wrong deployment;
   - lifecycle state: `WARMING`, `ACTIVE`, or `DRAINING`;
   - an expiry refreshed on a short heartbeat.
3. Membership heartbeat is one second and expiry is four seconds. Changes require staging evidence that the five-second crash-takeover objective still holds.
4. Executor membership may enter `ACTIVE` after the listener, assignment
   watcher, and recent Valkey-operation predicates pass. It must not wait for
   gateway replica hydration: a first or replacement task may need to own an
   executor before it can answer the snapshot barrier that makes its gateway
   routable.
5. Only `ACTIVE` executor members are eligible for new desired assignments.
   Public `/health/ready` remains a stricter, independent gateway predicate.
6. On SIGTERM, executor membership must synchronously enter `DRAINING` before local readiness is demoted. The task retains its current executor work until the partition-local assignment views move, then performs bounded handoff; it never waits for a game to finish.
7. SIGINT/Ctrl+C must use the same path for local development.
8. Shutdown must have one global deadline. It must not apply a new timeout serially to every background handle.
9. If the deadline expires, the process exits and correctness falls back to lease expiry, consumer-group takeover, and checkpoints.
10. Killing an owner with SIGKILL, with no membership cleanup and no lease release, must still result in automatic reassignment and recovery.

### R2 — Explicit desired partition placement

1. A regional assignment coordinator must persist one atomic, versioned assignment document in Valkey.
2. The persisted contract is an explicit `partition -> desired task boot ID` map plus the membership inputs and assignment version needed for diagnostics. Serialized internals of a `hashring` crate are not a persistence contract.
3. The allocator must be deterministic, keep eligible task owner counts within one partition, and minimize movement:
   - scale-up moves only enough partitions to balance the new task;
   - scale-down moves only partitions assigned to departing tasks;
   - an unchanged membership set produces no movement.
   - formally, after eligibility and balance are satisfied, minimize the assignment map's Hamming distance from the preceding assignment.
4. With fifty fixed partitions, use the direct balanced/minimal-movement allocator. Do not add or persist a consistent-hash ring for this release; it would not replace the explicit assignment contract or its balance and movement tests.
5. The coordinator must coalesce ordinary staggered joins and planned drains behind one four-second in-memory quiet window. This wait is allowed only while every current owner is live, protocol-compatible, and `ACTIVE` or `DRAINING`; a missing, expired, `WARMING`, or incompatible owner bypasses it immediately. A coordinator-term change resets the timer, and the current assignment's partition views are repaired while a candidate is held. Do not persist this timer or add an ECS dependency or two-phase assignment protocol.
6. The coordinator itself must use a unique tokened lease. Assignment writes must compare the exact coordinator token and expected assignment version atomically.
7. Readers must observe either the complete old assignment or the complete new assignment, never a partial map.
8. If the coordinator is unavailable, the last assignment remains valid and existing partition authorities continue.
9. A task may acquire a free partition lease only if it is the desired owner in the current assignment.
10. An incumbent stops renewing when it is no longer the desired owner. Its existing unexpired lease token remains the active authority until it is compare-deleted or expires.
11. Assignment change alone must not invalidate a final fenced checkpoint. This desired-versus-active distinction is the cooperative handoff; no separate transfer state machine is required.
12. Every Valkey key must be constructed through `RedisKeys`. Cluster hash tags define distinct atomicity categories: regional membership, regional canonical assignment, global matchmaking, active-server metrics, and one separate family for each of the fifty executor partitions. All keys in one Lua script, transaction, multi-key command, or pipeline batch must share one tag. Executor families must remain distinct so Serverless can distribute authoritative traffic across slots.
13. The canonical assignment document lives in the assignment slot. After a successful compare-and-set, the coordinator projects the complete document into each partition slot using a monotonic per-partition view. Lease acquire/renew reads that local view. A crash during projection may delay movement until reconciliation, but must never authorize two generations or roll a view backward.

### R3 — Fenced partition authority

1. Every successful partition lease acquisition must receive a never-reused acquisition token. Use a process boot UUID plus an acquisition UUID. Do not add a Redis monotonic epoch for this release; exact-token equality against the one logical Serverless cache is sufficient.
2. The lease value must include the task boot identity and acquisition token.
3. Acquire must atomically verify current desired ownership and lease availability.
4. Renew must atomically verify current desired ownership and the exact acquisition token.
5. Graceful release must compare-delete the exact acquisition token.
6. Every executor-owned Valkey mutation must validate the exact live lease token in the same Valkey operation as the mutation. This includes:
   - event-stream appends;
   - recovery checkpoint writes;
   - active-game index changes;
   - executor-originated status messages;
   - command `XACK`;
   - finalization markers.
7. Gateway-originated player-command `XADD` is not executor-owned and must remain independent of the executor lease.
8. Fencing checks must be centralized in a small set of lease-aware `GameBus` scripts or APIs. Callers must not implement a non-atomic “check, then write.”
9. Lease renewal and fencing operations must use their dedicated 750 ms deadline, which remains shorter than the three-second lease TTL.
10. A fenced-write rejection must prevent the mutation. Rejection on an authoritative actor or consumer path immediately cancels that executor; an already-cancelled background retry may only stop and leave its durable work for the successor.
11. Fencing addresses a paused, partitioned, timed-out, or detached application process that resumes late. It is not primarily a defense against a cache split brain; ElastiCache presents one managed logical cache endpoint.
12. ElastiCache Serverless fixes `maxmemory-policy=volatile-lru`; the application cannot select `noeviction`. Do not configure a data-storage maximum or ECPU maximum: those ceilings can respectively cause eviction/write failure or throttling during a scale event. `Evictions` and `ThrottledCmds` must remain zero in certification and alarm in production. Correctness still comes from durable protocol state and fail-closed error handling, not from assuming eviction is impossible.

### R4 — Durable executor command consumption

1. Keep the existing Redis partition command stream, but add one stable executor consumer group per partition.
2. Consumer groups apply only to the authoritative executor command path. Replica event readers and snapshot-request readers may keep ordinary `XREAD` fan-out.
3. Each consumer name must identify one lease acquisition token, not merely a task.
4. After acquiring authority, a successor must:
   1. load the partition's active-game checkpoints;
   2. exhaust pending entries from every prior acquisition in stream-ID order, using `XAUTOCLAIM` batches of at most 512 with zero additional idle delay once exclusive lease authority is established;
   3. read undelivered group entries with `XREADGROUP`;
   4. process recovered commands in stream order;
   5. only then advance recovered game clocks to current wall time.
5. A command may be `XACK`ed only when its outcome is recoverable:
   - accepted input: its scheduled or already-applied result and dedupe identity are covered by a checkpoint;
   - rejected input: a terminal rejection is durable;
   - `GameCreated`: the initial game checkpoint and active-game index exist;
   - completion/status work: the durable finalization state exists.
6. `XACK` is an internal transport retirement operation. It does not replace the client-visible semantic result.
7. ACKs must be batched at checkpoint boundaries; do not add one Valkey round trip per message.
8. `XREADGROUP` delivery and `XAUTOCLAIM` must validate the exact lease token atomically in the same Valkey operation that assigns pending ownership. Because a blocking group read cannot be enclosed by the fencing script, use bounded nonblocking fenced reads plus a 50 ms cancellable local idle. Token loss cancels the reader immediately.
9. Command streams must not use publish-time approximate `MAXLEN` trimming.
10. Command-stream cleanup must use a group-aware retention policy that never trims a pending or otherwise recoverable entry. Stuck pending age must be observable and alerted.
11. `XACK` removes an entry from the pending-entry list; it does not by itself delete the stream entry.
12. Recovery backlog processing must use batches of at most 512 while preserving per-partition stream order.
13. Every delivered executor item must retain its Redis stream ID through dispatch and checkpointing. A malformed/poison entry needs a durable quarantine or terminal disposition before it can be ACKed.
14. Before a task becomes ready, independently bootstrap exactly one
    `redis-rs` cluster connection for each of the fifty fixed executor partitions.
    Every latency-sensitive, partition-scoped `GameBus` command, event,
    consumer-group, snapshot-anchor, and fenced mutation must use the
    deterministic lane for that partition. Do not clone one connection across
    partitions: clones share the same bounded client dispatcher and underlying
    per-node connection set.
15. Keep `PartitionLeaseStore` acquire/renew/release, membership, assignment,
    matchmaking, and readiness on the low-volume global control connection.
    Partition-hot fenced scripts still validate the lease key atomically from
    their partition lane.
16. Before a task becomes ready, independently bootstrap exactly one
    recovery-read `redis-rs` cluster connection for each of the fifty fixed
    partitions. Takeover journal/envelope loads, stored-snapshot and
    recovery-failure loads, reconnect outcome reads, and completion-record
    loads use the deterministic recovery lane for their partition. Regional
    resilience metrics use one additional independently bootstrapped,
    best-effort dispatcher so telemetry cannot queue ahead of recovery.
    Full-state periodic checkpoints and terminal completion commits retain
    exactly one independently bootstrapped checkpoint-write dispatcher per
    task. RESP3 Pub/Sub and stream fan-out readers retain their own dedicated
    connections.
17. The fixed topology is fifty partition-hot lanes, fifty partition-scoped
    recovery-read lanes, one global control dispatcher, one checkpoint-write
    dispatcher, one metrics dispatcher, and the already separate Pub/Sub and
    stream-reader connections. Do not add a dynamic or per-game pool, a
    priority scheduler, longer client deadlines, a recovery cache, another
    persistence format, or retries that conceal queueing.

### R5 — Command acknowledgement, idempotency, and resend

1. Use `CommandScheduledV2` as the positive semantic acknowledgement. Do not add a redundant gateway-level `CommandAccepted` event.
2. Add a terminal `CommandRejected` result containing the stable client command identity and reason.
3. Each player command must use the current `GameCommandV2` envelope with a stable identity scoped by game, authenticated user, client game-session ID, and monotonic session sequence. No legacy command envelope is accepted on the WebSocket path.
4. The browser must keep the command sequence and an in-memory pending outbox outside the WASM engine instance so snapshot-driven engine reconstruction does not reset command identity. Cap it at 128 unresolved commands per client game session. It must also prevent the next sequence from moving more than 128 positions above the earliest unresolved sequence; otherwise out-of-order terminal results could free client slots while overflowing the server's bounded sparse-result map. A disconnected, unauthenticated, snapshot-waiting, or unsynchronized browser must not create a new command identity. At the certified ten-command-per-second profile, the bound also covers 12.8 seconds of continued submission without outcomes. At either cap, do not create or send another command identity until the blocking entry resolves.
5. Only one WebSocket generation may send game commands during a planned dual-socket overlap.
6. After reconnect, the client must wait for authentication, game rejoin, and fresh resolved-command state before resending unresolved commands in original order.
7. The server must deduplicate by stable client command identity before scheduling. A duplicate must return the recorded `CommandScheduledV2` or `CommandRejected` outcome. Repeated physical delivery must not repeat the logical game effect.
8. Dedupe state must contain:
   - the highest contiguous terminally resolved sequence for each client game session;
   - a bounded sparse result map above that point when outcomes have gaps or arrive out of order.
9. A rejected or never-received sequence must not be represented as accepted merely because a higher sequence resolved. The contiguous watermark never advances across an unresolved gap.
10. Retain at most 128 exact outcomes per client game session and at most 64 client game sessions per game. Keep this bound equal to the browser outbox bound. The contiguous watermark remains in every recovery checkpoint for the checkpoint lifetime and permanently fences older resolved identities, so pruning an exact result does not permit its logical effect to run again.
11. Recovery checkpoints must contain the resolved watermark and sparse command outcomes. After a recovery snapshot, the server sends that user's `CommandOutcomes` records followed by an explicit `CommandOutcomesComplete` barrier before the client may resend unresolved commands.
12. A client may remove an outbox entry only after matching
    `CommandScheduledV2`, matching `CommandRejected`, matching
    `CommandOutcomes`, an explicit terminal default disposition described
    below, or a definitive `GameLoadFailed`. A terminal game event first parks
    retries. The server then sends the durable exact per-session outcomes
    (including any rejection fence) followed by
    `CommandOutcomesComplete`. It may put a nonempty
    `terminal_rejection_reason` on that barrier only after loading a current
    recovery envelope whose game state is terminal. After the client has
    already processed a terminal snapshot for that same game, it applies the
    reason as a rejection to every still-pending identity for the game. Exact
    outcomes take precedence. This closes commands that crossed buffered
    terminal delivery without inventing a scheduled result. The client retains
    an empty terminal tombstone until explicit leave/navigation so input cannot
    open a new command session while UI state catches up. An authorized
    completed-game reload with a missing, expired, malformed, or nonterminal
    recovery envelope sends a reasonless barrier. It remains readable, but any
    pending identity stays parked and fails closed rather than being
    dispositioned from an unproven terminal frontier.
13. Before publishing a client-visible `CommandScheduledV2` or `CommandRejected`
    that is not yet checkpointed, the executor must atomically and under its
    live fence write the exact outcome, authoritative schedule/counter, and
    event watermark to a partition decision journal keyed by the pending
    command-stream ID, then append the outcome event. The journal entry is
    deleted only by the same fenced transaction that ACKs that stream ID after
    a checkpoint or another terminal disposition makes the result recoverable.
    This uses the existing outcome-publication round trip;
    it must not introduce a full checkpoint per command.
14. The delivery contract is at-least-once physical delivery with exactly-once logical effect, not literal exactly-once message publication.
15. If the bounded sparse-result map is already full when a new noncontiguous
    identity arrives, the executor must install one checkpointed terminal
    rejection fence for that client game session, starting at the offending
    sequence. If that first rejection is published before a checkpoint, it uses
    the ordinary decision-journal and positive-stream-sequence path and its
    command remains pending until a checkpoint containing the fence ACKs it.
    Bootstrap may instead install the fence without publishing and atomically
    checkpoint plus ACK it before exposing recovery state. The same fence is
    restored after takeover and included in reconnect `CommandOutcomes`; an
    exact outcome or the contiguous watermark takes precedence. The client
    clears covered pending entries, creates no later identity in the fenced
    session, preserves any unresolved lower gap, and rotates to a fresh session
    only after those lower entries resolve. Do not add another Redis key,
    per-command checkpoint, or rejection persistence mechanism for this edge
    case.
16. The gateway must test the partition completion key and append
    `GameCommandSubmittedV2` in one same-slot Lua operation. Once completion
    exists, a late command is not appended to the executor stream; the proven
    terminal default disposition closes its client identity. The script must
    reject a payload whose typed game/user identity does not match the
    authenticated command envelope.
17. `terminal-command-cutoff-v1` is a required lifecycle capability. A server
    or client missing it is incompatible with this release; do not add a
    fallback protocol.

### R6 — Per-game checkpoints and recovery

1. Keep full per-game snapshots. Do not add a whole-partition state blob or delta snapshot scheme in this phase.
2. Store a versioned recovery envelope containing at least:
   - schema version;
   - game ID and partition ID;
   - full authoritative `GameState`, including RNG, arena, score, status, and scheduled command queue;
   - last incorporated command-stream ID;
   - resolved client-command watermarks and sparse outcomes;
   - next server command sequence/counter;
   - authoritative event stream sequence or revision;
   - checkpoint timestamp and source lease token for diagnostics.
3. Active games must initiate checkpoints on a one-second wall-clock cadence, independent of custom game tick duration. Persisted checkpoint age must remain below `SNAKETRON_MAX_CHECKPOINT_AGE_MS` (default ten seconds) or the actor fails closed.
4. Checkpoints must also be written at game creation, successor activation after recovery, and completion. A planned handoff must not add a redundant incumbent checkpoint; it uses the same last-checkpoint plus PEL/decision-journal recovery source as an abrupt crash.
5. Maintain a `partition -> active game IDs` index. Recovery must query this index and fetch checkpoints in a pipeline/batch rather than scan all `game:snapshot:*` keys.
6. The authoritative recovery source is the versioned checkpoint. A local replica may accelerate recovery only if it carries the same cursor and dedupe metadata; an ordinary state-only replica must not override the checkpoint.
7. A successor must batch-load the partition decision journal under its new
   fence, attach entries to reclaimed commands by exact stream ID, and preserve
   each game's complete source-stream projection while allowing independent
   games to be interleaved. It skips commands already covered by the
   checkpoint; for an uncovered journaled command it restores the exact
   recorded schedule, server-command counter, resolved outcome, and strictly
   advancing event watermark without reauthorizing or republishing the
   incremental outcome. It then catches the deterministic engine up to wall
   time and publishes one fresh recovery snapshot with resolved-command state
   before normal deltas resume. An orphaned, mismatched, or non-monotonic
   journal entry fails recovery closed.
8. Recovery checkpoint retention defaults to 30 minutes and is configurable through `SNAKETRON_RECOVERY_RETENTION_MS`; it must exceed the measured ECS replacement p99 plus margin.
9. Completed games must have explicit cleanup after their durable completion grace period.
10. If replacement occurs after the documented recovery retention, the game must produce an explicit unrecoverable outcome. It must not silently fabricate or restart state.
11. Checkpoint size and write volume must be measured under `1 -> 10 -> 1` load. Delta encoding is considered only if those measurements show a real capacity problem.
12. A cold-join or ordinary on-demand repair request carries the exact game ID
    and is only a best-effort hint to that actor. A gateway polls its local
    replica every 100 milliseconds but publishes the targeted request at most
    once per 500 milliseconds through the bounded warm-up window. The executor
    uses a nonblocking actor mailbox send; a full mailbox, a request racing game
    creation, or an ownership gap is benign because the gateway retries and a
    successor publishes its recovery re-anchor. This path must not checkpoint
    inline, wait for an actor reply, or fan out to unrelated games.

    Partition startup/readiness and detected partition-gap repair remain
    explicitly partition-scoped requests. An initial readiness request retains
    completion waiters in every active actor while commands continue, and a
    background executor worker appends a boot-unique completion marker to the
    same ordered partition event stream only after every active actor has
    published. The gateway retries a missed request and becomes ready only
    after consuming its exact marker, which also handles an empty partition
    without a timer or synthetic game. Targeted cold-join hints never satisfy
    this readiness proof.
13. The authoritative executor partition reader must round-robin the complete
    projection for each addressable game, keep at most one unresolved delivery
    per game, and use a bounded fan-out. A later command, status, creation,
    cursor, decision, checkpoint, ACK, or actor-membership action for one game
    must first settle that game's earlier accepted delivery; it must not wait
    for unrelated games. Global drains are limited to Poison boundaries,
    ownership handoff, the bounded fan-out window, failure, and batch
    completion. This removes cross-game head-of-line blocking without
    introducing a second scheduler or relaxing per-game ordering and recovery
    semantics.
14. Gateway replicas must use a resumable event-only partition reader. They
    must not read or drain the executor command stream or snapshot-request
    stream: pressure on either unrelated stream must not block authoritative
    game events from reaching connected sockets. The reader anchors before
    subscription returns, reconnects from its last event-stream ID, and
    verifies that a nonzero cursor has not crossed the bounded stream's trim
    horizon on initial connection, reconnect, full backlog batches, and local
    channel backpressure. A detected transport discontinuity fails the critical
    replica worker and therefore the task; a surviving completion marker must
    never certify snapshots that were trimmed before delivery. Existing
    per-game sequence-gap and join-side snapshot repair remain responsible for
    recoverable gaps beyond this bus.
15. After broadcasting a full terminal snapshot, a gateway replica may evict
    that game immediately. The fenced completion script stores the immutable
    completion record, final recovery envelope, stored snapshot, pending-effect
    index, and terminal publications in one atomic partition-slot operation
    before the terminal snapshot can be observed. The existing command-stream
    terminal status remains available for executor cleanup, but gateways
    neither consume it nor wait for it as an eviction/liveness dependency.

### R7 — Planned partition handoff and task shutdown

1. Planned scale-up and scale-down must use the same crash-safe primitives as abrupt recovery.
2. When desired ownership changes, the incumbent must stop fetching new group work and ordinary assignment-conditioned renewal. An exact-token keepalive may preserve its authority only until the game barriers complete and that same token is compare-deleted.
3. While its current token is still valid, the incumbent must place a barrier into every active game loop. Each loop must process already queued commands, stop tick advancement, persist its exact last-published event watermark with the partition lease fence, acknowledge quiescence, and perform no later authoritative mutation under that token. This marker is not a state checkpoint.
4. After every game loop reaches the barrier and persists its marker, the incumbent must compare-delete its exact partition lease without writing another full-state checkpoint. Commands newer than the last periodic checkpoint remain in the PEL with any published outcome in the decision journal. The successor replays them, raises its event watermark to at least the marker, checkpoints/ACKs the recovered state while deleting the marker, and publishes a recovery snapshot strictly beyond the incumbent's visible stream.
5. If any marker write or loop barrier fails before the handoff deadline, stop the old executor and fall back to normal lease-expiry recovery; do not release the lease, extend shutdown, publish an unfenced partial handoff, or advertise a planned client drain.
6. The successor may acquire only after the old lease is deleted or expires.
7. A crash at any handoff step must require no cleanup: the successor claims pending entries and resumes from the last successful checkpoint.
8. Partition transfer must not wait for a game to complete.
9. Partition ownership movement alone must not close or move a WebSocket.
10. Configure one 60-second ECS container stop timeout and one 45-second application drain deadline. Any change must preserve a safety margin and pass the planned-drain suite.
11. Executor handoff must start immediately after the task becomes unready and `DRAINING`, before the route-withdrawal wait. The task continues serving existing WebSockets, but may send the WebSocket drain notice only after every executor handoff succeeds. An empty local executor registry is not proof of success: authority loss, worker failure, cancellation, or cleanup must latch failure before removing a handle and suppress the planned drain for the remainder of that process boot.

### R8 — WebSocket recovery and planned make-before-break

1. Preserve the single-process gateway/executor deployment. A gateway can serve any game through regional Valkey streams and its local replicas.
2. Scale-up must not move existing WebSockets.
3. Hard-crash reconnect behavior must be:
   - an immediate first retry;
   - short jittered exponential backoff after failure;
   - automatic reauthentication, lobby/game restoration, and fresh snapshot;
   - no page reload or user action.
4. Use an explicit server `Authenticated` response containing task boot identity and the current required capabilities. The client must not mark a socket authenticated merely because it sent a token or a timeout elapsed. All advertised requirements are mandatory, including `command-outcome-barrier-v1` and `terminal-command-cutoff-v1`; there is no version negotiation or fallback mode.
5. Every socket must have a monotonically changing local generation. Callbacks from an older generation must not close, reconnect, overwrite state, or clear readiness for a newer socket.
6. Transport closure must be distinct from explicit `LeaveLobby`. Unexpected or planned transport loss stops that connection's heartbeat and lets its short presence lease expire; it must not immediately delete durable lobby or matchmaking state.
7. Cleanup from an old socket must compare its session/generation and must not erase presence created by the replacement socket.
8. Maintain durable `user -> active game / committed match` resolution. Pub/Sub `MatchFound` remains a best-effort hint.
9. For planned task drain:
   1. mark the task unready so Traefik begins route withdrawal while keeping old sockets open;
   2. during the bounded route-convergence window, finish in-flight and safe stateless HTTP work but reject every new WebSocket upgrade with a retryable `503`; supported clients retry without surfacing a terminal error;
   3. after every executor partition has completed its marker-backed handoff, send one drain message containing task identity and deadline over every existing socket; if executor handoff failed, send no drain message and let socket closure use ordinary crash recovery;
   4. the client opens a second socket through the same regional URL, not a server-specific URL;
   5. the second socket authenticates and restores lobby/game context;
   6. for a game, the second socket receives a current snapshot, resolved-command outcomes, and `CommandOutcomesComplete`, then buffers subsequent events;
   7. after the candidate is ready, the client sends a uniquely tagged application Ping on the old socket and receives its matching Pong. WebSocket ordering freezes the old game-stream watermark observed through that Pong; the marker-backed successor guarantees a recovery snapshot beyond that fixed frontier;
   8. the old socket remains the visible event stream and sole command owner while the candidate catches the frozen frontier and receives `CommandOutcomesComplete` paired with its latest snapshot. At promotion, retain old state already applied beyond the frontier and suppress the candidate's prefix at or below that applied watermark, including covered frames that arrive after the atomic socket swap. Keep this suppression floor until the promoted stream advances beyond it; a terminal snapshot remains immediately authoritative;
   9. the client atomically switches command ownership to the new socket generation and only then closes the old socket. If the old transport closes or the shared deadline fires after the candidate is fully ready but before the Pong, retain and promote that candidate as crash recovery while recording a planned-handoff failure.
10. During overlap, only one socket sends player commands. Event delivery may overlap, but stable event revisions and socket-generation filtering must make duplicates harmless.
11. Use one drain message containing only the departing gateway task identity and deadline; it must never direct clients to an executor host because executor and gateway placement are independent.
12. Generic durable outbound WebSocket replay is not required; reauthentication, rejoin, and a fresh snapshot are sufficient.
13. Treat lobby-roster Pub/Sub as an invalidation hint, not authoritative
    delivery. A socket entering a lobby must immediately read the durable
    roster, every hint must trigger another durable read, and the next
    successful read on the one-second periodic cadence repairs a missed hint
    without reconnecting or changing the client protocol. A socket must not
    resend an unchanged client-visible snapshot merely because member
    heartbeat timestamps changed.
14. A socket already in a lobby must receive `AccessDenied` for `CreateLobby`
    or `JoinLobby`. It must send `LeaveLobby` successfully before entering a
    different lobby; denial leaves the current lobby presence and every
    lobby-scoped subscription unchanged.

### R9 — Crash-safe, concurrency-safe matchmaking

1. Keep matchmaking selection and scoring in Rust.
2. Admit a lobby through one atomic Valkey Lua operation that:
   - verifies the lobby metadata still exists and no lobby member or lobby is already matched;
   - creates one immutable queue identity for the lobby and one exact queued-lobby claim for each member;
   - rejects a conflicting lobby or user claim, so one user cannot be queued through two tabs or two lobbies;
   - inserts every queue and MMR-index member and sets the lobby state to `queued` in the same operation.
   A retry of the same physical request is idempotent. A later request while the lobby is already queued preserves the first admitted preferences; changing them requires cancel and requeue. `QueueForMatch` is rejected unless that socket has joined an explicit lobby; every browser entry point creates or restores that lobby before queueing, so there is no unaddressable server-generated lobby between the client and durable admission. A client retains at most one in-memory queue intent, scoped to the exact lobby and authenticated identity, until an authoritative `LobbyUpdate` reports `queued` or `matched`, or `JoinGame` reports the committed game. A successful WebSocket write is not admission acknowledgement. After reconnect it restores the lobby first and replays the same intent only if authoritative state is still `waiting`. Browser refresh, tab loss, and device transfer remain outside the persistence guarantee.
3. Cancellation must compare and remove the exact admitted lobby identity, its queue/MMR members, and its per-user claims in one operation. It sets an existing lobby's state back to `waiting` only when no active-game mapping won the race. Repeated cancellation is idempotent.
4. Commit a selected match through one atomic Valkey Lua operation in the matchmaking hash slot that:
   - verifies every selected lobby/queue entry is still eligible;
   - removes the selected lobbies from every relevant queue and MMR index;
   - records the active match;
   - records user/lobby-to-active-game mappings;
   - changes existing lobby metadata to `matched`;
   - writes one durable `GameCreated` outbox record containing the complete initial event;
   - publishes each connected-lobby `MatchFound` hint only after those durable writes, before the same script returns.
5. Allocate the durable DynamoDB game ID before the Valkey commit. Unused IDs after a failed claim are acceptable.
6. Exactly one concurrent claim may succeed. Losing workers must leave the winning match intact and must not partially remove other queue state.
7. Every task may scan the small matchmaking outbox. Delivery into the destination partition slot must atomically compare/create a per-game delivery marker and append `GameCreated`; retries after an ambiguous response return the original delivery rather than append a duplicate. Remove the source outbox field only by compare-and-delete after destination success. No singleton is required.
8. Outbox scanning must route validated records through exactly one bounded,
   sequential delivery worker per fixed executor partition. Routing is
   nonblocking: each scan page becomes at most one complete batch per
   partition, and a lane holds at most one queued batch behind its active
   batch. A full lane leaves the batch's records in the authoritative Redis
   hash for a later scan. The worker must continue through the batch after a
   record-specific delivery error. A slow partition's publish, compare-delete,
   or marker expiry must not delay another partition, and an unexpected
   delivery-worker exit is task-fatal. Do not add an unbounded spawn-per-record
   path or a generic worker pool.
9. `GameCreated` carries the full initial state. The executor remains the only checkpoint writer: it creates the initial recovery checkpoint and active-game index entry before ACKing `GameCreated`.
10. `MatchFound` remains a hint. A connected lobby listener must subscribe first, then read the durable lobby-to-game mapping; every hint triggers another authoritative mapping read, and a five-second fallback reconciliation covers a missed hint or subscription reconnect. Deduplicate forwarded `JoinGame` messages by game ID. Disconnected recipients resolve the same durable mapping during authentication.
11. The atomic admission predicates and atomic eligibility/commit are the matchmaking fences. A separate matchmaking ownership epoch or general saga framework is not required; the one outbox is the narrow cross-slot bridge imposed by Redis Cluster.

### R10 — Truthful liveness, readiness, and routing

1. Expose separate endpoints:
   - `/health/live`: the process and HTTP runtime are functioning;
   - `/health/ready`: the task may receive new regional traffic.
2. Start unready.
3. Readiness requires:
   - the HTTP/WebSocket listener is bound;
   - a recent bounded Valkey operation succeeded;
   - all partition replica stream readers are subscribed and alive, and each
     has consumed its boot-unique initial snapshot-completion marker after all
     preceding active-game snapshots;
   - membership and assignment watchers are alive;
   - other critical local workers are alive;
   - the task is not draining.
4. Readiness must not require owning any executor partition.
5. A critical background worker exiting unexpectedly must fail the process so ECS restarts it. Do not add a general in-process supervisor.
6. A transient regional Valkey failure makes tasks unready but must not make ECS liveness fail and create a replacement storm.
7. Executor-placement eligibility and public gateway readiness are distinct.
   A task may execute partitions while its gateway remains unready. After all
   ten initial snapshot barriers pass, a later requested game that is still
   cold uses the bounded on-demand snapshot request/load and returns a
   retryable warming response rather than reporting a false missing game.
8. Traefik backend health must use `/health/ready`; ECS container health must use `/health/live`.
9. Keep `/api/health` as the lightweight client-side regional latency probe; it must not be the Traefik readiness signal.
10. Remove the Traefik sticky-session cookie. Affinity is not required and can route reconnects back toward a draining backend.
11. Keep automatic ECS discovery. Planned drain waits eight seconds after becoming unready before assuming new upgrade attempts have stopped reaching that backend; the server still rejects any late upgrade with retryable `503`.
12. Traefik active backend health uses a two-second interval and one-second timeout.
13. ECS provider discovery polls every five seconds. No node-event webhook or manual Traefik update is required.
14. ECS container liveness checks run every five seconds with a startup grace period.
15. New tasks become routable automatically after ECS discovery and readiness pass.
16. Traefik's `ping` endpoint must remain enabled because its container health command is `traefik healthcheck --ping`.
17. Retain the per-source-IP WebSocket rate average of 50 upgrades per second
    and the separate persistent-socket cap, but set the upgrade burst to 512 so
    the certified same-IP make-before-break cohort is not rejected during
    scale-in. A client or certification session that receives upgrade `429`
    retries through the ordinary reconnect path inside its existing admission
    deadline. This is burst capacity, not a higher sustained rate or a relaxed
    readiness deadline. The certification load client bounds each physical
    WebSocket connection attempt to two seconds so one dead route cannot consume
    the unchanged ten-second end-to-end admission deadline; retries share, and
    never extend, that original deadline.

### R11 — Idempotent finalization and external effects

1. Game completion must have a stable completion revision/idempotency key.
2. Before any DynamoDB reward or ranking write, the executor must atomically commit an immutable completion record and final recovery checkpoint through its live fenced token.
3. External effects must derive from that committed completion record, not from mutable in-memory state held by the executor.
4. Completion status, XP, MMR, rankings, and any future reward effect must be idempotent per `game / user / effect type`.
5. A stale executor must not be able to create a distinct second completion revision.
6. DynamoDB additive updates must be protected by a conditional idempotency record in the same transactional boundary as the update, or by an equivalent atomic design.
7. Retrying after an ambiguous DynamoDB response must converge to the same result.
8. Recovery may redeliver completion work; repeated delivery must be observable as a prevented duplicate, not a repeated reward.
9. The completion record must retain recoverable pending-effect status and be retried until every required idempotent DynamoDB effect is confirmed. It may be cleaned up only after confirmation and the configured completion grace period.
10. Because completion state is partition-local while matchmaking mappings share the matchmaking slot, the fenced completion commit must retain the pending-completion record until a separate idempotent matchmaking cleanup succeeds. That cleanup must:
    - remove only that game's active-match record;
    - remove player, spectator, and lobby active-game mappings only when their current value still equals the completed game ID;
    - never delete a mapping that has already advanced to a newer game.
11. The partition-local fenced completion commit must durably publish one full
    terminal snapshot and one terminal status notification in the same atomic
    script that stores the immutable completion record, final recovery
    envelope, stored snapshot, and pending-effect index. Retrying after a
    timeout or crash must repair a missing notification without publishing
    duplicates during the completion grace period. Because readers cannot
    observe the terminal snapshot before those durable writes commit, a gateway
    replica broadcasts that snapshot and then evicts its local game without
    waiting for a command-stream marker.
12. Durable effects must enforce their dependency order in the storage transaction: no XP, MMR, ranking, or high-score effect may commit before the completed-game record, and a ranking projection may not commit before its matching MMR effect.
13. A successor executor must be able to finalize a game created or previously executed by another task. Completion identity must be the durable game ID and immutable completion revision; it must not require the finalizing task's server ID to match the original executor.

### R12 — Autoscaling behavior and capacity constraints

1. Use the same target-tracking policies in development and production:
   - CPU target: 15%;
   - memory target: 80%;
   - 60-second scale-in and scale-out cooldowns.
   The CPU target remains above measured idle utilization while leaving margin
   below the weakest loaded minute observed for the fixed Gate A work across
   otherwise identical Fargate placements. This preserves headroom for command
   processing and partition recovery while the managed alarm evaluates and a
   replacement task starts.
2. Retain `minTasks=1` and allow 25 tasks in both development and production. The release-blocking `1 -> 10 -> 1` staircase still runs outside production, while fifty partitions divide evenly across both its ten-task waypoint and the 25-task autoscaling ceiling. The staircase uses the fixed 128-session / 64-duel one-task-capacity-valid transition cohort; the separate 128-session natural scale-out load and the complete capacity envelope must both be removed before a forced scale-in to one task. The minimum application task is two vCPU and four GiB, the smallest valid Fargate memory pairing for two vCPU; target tracking cannot protect a saturated one-task floor during the managed alarm's observation delay.
3. The autoscaler must never select zero desired tasks.
4. Validate forced `1 -> 10 -> 1` with 128 active game sessions / 64 duels
   producing `every-tick` commands on every partition, 10 idle sessions, 10
   lobbies, three unmatched matchmaking sessions, and bounded open-loop idle
   admission at four starts per second during scale-in. Each successful
   admission holds for one second after becoming ready and the probe enforces a
   64-session in-flight safety ceiling. Sessions must be launched throughout
   the transition, not accumulated into an artificial destination load.
   Separately prove the
   128-session natural scale-out gate, and hold the 256-session/128-duel
   capacity envelope only after ten tasks are healthy and ready.
   Gate B's maximum is 215 sockets and only 128 are command-bearing. Gate A
   proves that the same command-bearing cohort remains healthy while the
   production CPU/memory policy adds capacity naturally. Gate B separately
   proves that its context and transient admission sockets fit on the final
   one-task destination. The minimum application task is two vCPU and four GiB,
   the smallest valid Fargate memory pairing for two vCPU. Current exact-source
   evidence measured about 0.36--0.72 vCPU at this cohort across otherwise
   identical Fargate placements; one vCPU did not preserve the one-second
   latency budget during the managed policy's observation lag.
5. No custom game-specific autoscaling metric is added in this phase.
6. Every task currently replicates every partition, so task-local replica memory may not fall on scale-out. Scaling tests must prove memory behavior is acceptable; otherwise the replication model or memory policy needs a separate decision.
7. Existing WebSockets do not redistribute on scale-up, so service-average CPU can hide a hot gateway task. Record per-task CPU, memory, connections, and event-forwarding load during validation.
8. Keep the partition count fixed at fifty; do not increase it again or add adaptive splitting without load evidence.
9. Load tests must include Serverless Valkey read/write latency, ECPU, bytes, connections, network traffic, `ThrottledCmds`, and `Evictions`, plus the shared regional NAT/Traefik host's CPU, network, connection success, and admission latency/error evidence.
   Connection-tracking occupancy is an optional capacity diagnostic when the
   host exposes it; it is not an autoscaling-correctness or release gate.
   Redesigning those dependencies remains out of scope.
10. A planned scale-in load must fit on the one-task destination before the
    transition begins. Simultaneous lease-probe, heartbeat, checkpoint, and
    event-write timeouts while that destination is CPU-starved invalidate the
    capacity precondition; they are not evidence of a partition-handoff race.
    Fencing remains fail-closed: a write that cannot prove the current lease
    must be rejected, executor drain must fail, and the client must not be told
    that a cooperative handoff succeeded.

## 10. Logical Valkey data model

Exact suffixes are implementation details, but the brace-delimited hash-tag families are part of the Serverless compatibility contract. Keys outside an atomic multi-key operation may remain independently slotted.

| Logical record | Suggested shape | Purpose |
| --- | --- | --- |
| Task membership | Keys tagged `{snaketron:members:<region>}` | Detect active, warming, draining, and crashed tasks atomically. |
| Coordinator lease + canonical assignment | Keys tagged `{snaketron:assignment:<region>}` | Elect one writer and persist one explicit versioned owner map. |
| Per-partition assignment view | Key tagged `{snaketron:exec:<p>}` containing the complete canonical document/version | Let partition lease scripts verify desired ownership without a cross-slot read. |
| Partition lease, streams, active-game index, recovery and completion records | Keys tagged `{snaketron:exec:<p>}` | Keep every fenced executor transaction single-slot while spreading fifty partitions across Serverless slots. |
| Matchmaking queues, mappings, active matches, notification channels, and `GameCreated` outbox | Keys/channels tagged `{snaketron:mm}` | Keep admission/cancel/match claims and their in-script notifications in one hash slot. |
| `GameCreated` delivery marker | Key tagged `{snaketron:exec:<p>}` beside the destination command stream | Make cross-slot outbox delivery idempotent. |
| Active-server metrics + expiry index | Hash and sorted set tagged `{snaketron:server-metrics}` | Refresh and prune per-task region/user counts atomically without a cluster-wide key scan. |
| Effect idempotency | DynamoDB item keyed by game, user, and effect | Prevent duplicate completion rewards. |

Illustrative recovery envelope:

~~~json
{
  "schema_version": 2,
  "game_id": 123,
  "partition_id": 3,
  "game_state": "...full GameState...",
  "command_cursor": "1721490000000-4",
  "resolved_client_commands": {
    "client-game-session-uuid": {
      "contiguous_through": 39,
      "sparse_outcomes": {
        "41": "scheduled"
      }
    }
  },
  "next_server_command_sequence": 88,
  "event_stream_sequence": 9201,
  "checkpointed_at_ms": 1784678400000,
  "source_lease_token": "diagnostic-only-token"
}
~~~

The stored source token is diagnostic only. On recovery, the successor's newly acquired live token controls all new writes.

## 11. Required workflows

### 11.1 Planned scale-up

1. ECS starts a new task.
2. After listener, assignment-watcher, and recent-Valkey checks pass, its
   executor membership becomes `ACTIVE`; the assignment coordinator may use it
   while its public gateway remains unready.
3. The coordinator computes a minimally moved balanced desired map.
4. Incumbents for moved partitions stop renewing, checkpoint, ACK covered commands, and compare-delete their leases.
5. The new task acquires those leases, claims pending commands, restores checkpoints, catches up, and publishes fresh snapshots.
6. Only after all gateway replicas consume their matching initial snapshot
   barriers does `/health/ready` pass and Traefik route new connections to it.
7. Existing WebSockets remain on their original tasks throughout.

### 11.2 Planned scale-down

1. ECS sends SIGTERM.
2. The task becomes unready, advertises `DRAINING`, and starts the one global deadline.
3. The coordinator excludes it from desired placement.
4. Its executors cooperatively checkpoint/release; successors recover through the normal crash-safe path.
5. Existing gateways continue serving their sockets while Traefik withdraws the backend.
6. Clients complete dual-socket authentication, rejoin, snapshot catch-up, and atomic generation switch.
7. The old sockets close and the task exits. No game completion is awaited.
8. If the task is killed at any step, leases expire, pending commands are claimed, presence leases expire, and clients use hard-crash reconnect.

### 11.3 Abrupt owner crash

1. The task stops without cleanup.
2. Its membership and partition leases expire.
3. The coordinator assigns its partitions to surviving `ACTIVE` tasks.
4. Each successor acquires a new unique lease token.
5. It loads the partition active-game index and recovery checkpoints.
6. It claims pending commands, reads new commands, deduplicates entries covered by checkpoints, and replays backlog in order.
7. It advances games to wall time, publishes fresh snapshots, and resumes normal execution.
8. Any late write from the old process is rejected by the new token.

### 11.4 Ambiguous player command

1. The client places the stable command ID in its outbox before sending.
2. The gateway appends it to the partition command stream.
3. The executor processes it at least once and schedules it at most once.
4. The client clears individual entries on `CommandScheduledV2`,
   `CommandRejected`, or matching `CommandOutcomes`. A terminal game event
   parks the outbox. After exact durable outcomes, a
   `CommandOutcomesComplete` carrying the proven terminal default rejects any
   identities still pending for that game; a reasonless barrier leaves them
   pending and fails closed. The barrier is invalid unless the client has
   already observed an authoritative terminal snapshot for that same game.
   Terminal state is irreversible, so reconnect does not discard this proof or
   require generation-scoped terminal bookkeeping. Even after every entry
   resolves, an empty terminal tombstone blocks new commands until explicit
   leave/navigation. A definitive `GameLoadFailed` may also clear it.
5. If the gateway/socket fails at any point, the client resends the same identity after recovery readiness.

## 12. Failure semantics

| Failure | Required behavior |
| --- | --- |
| Assignment coordinator crash | The last atomic assignment remains; another eligible `ACTIVE` task acquires the coordinator lease and reconciles membership. |
| Executor pause longer than lease | Successor acquires a new token; every late mutation and ACK from the old token fails. |
| Task SIGKILL | Membership/leases expire; commands remain new or pending; checkpoints restore games; clients reconnect. |
| Failure after command delivery but before schedule | Successor claims the pending entry and processes it. |
| Failure after schedule but before checkpoint | The command and its exact write-ahead decision remain pending. The successor restores the recorded schedule, counter, outcome, and event watermark from the prior checkpoint and produces one logical state effect without another incremental confirmation. |
| Failure after checkpoint but before visible confirmation | Successor skips reapplication; resolved snapshot state eventually clears the client outbox. |
| Failure after visible confirmation but before `XACK` | If checkpointed, the successor skips the command; otherwise it loads the decision keyed by the pending stream ID and restores that exact result. In both cases no duplicate logical effect or incremental schedule is produced before the recovery snapshot reanchors the client. |
| Sparse-result overflow after its rejection is published but before the fence is checkpointed | The command and journaled fence decision remain pending. A successor restores and checkpoints the identical fence without applying the command or duplicating the incremental event; recovery outcomes expose it to reconnecting clients. |
| Sparse-result rejection fence checkpointed before its event is observed | Reconnect `CommandOutcomes` carries the fence, clears every covered pending identity, preserves lower unresolved gaps, and prevents reuse of the rejected identities. |
| Checkpoint write failure | Do not ACK covered entries. Emit positive confirmation only while the original entry remains durably recoverable, retry, and expose unhealthy checkpoint age. Step down only if lease/fencing validity cannot be established or an explicit fail-closed age budget is exceeded. |
| Matchmaker crash before atomic commit | Entrants remain queued; an allocated game ID may be unused. |
| Matchmaker crash after atomic commit but before outbox delivery or Pub/Sub | Durable mappings and the `GameCreated` outbox record remain. Any task idempotently delivers it into the partition stream; reconnect discovers the match without Pub/Sub. |
| Valkey unavailable to all tasks | Readiness becomes false; liveness remains true; availability is not promised. On restoration, token and pending state reconcile without bypassing the durable consumer-group path. |
| Sole task crash | Region is unavailable until replacement. State resumes only inside the documented checkpoint-retention window. |
| Replacement after checkpoint retention | Explicit unrecoverable result; no silent game restart. |

## 13. Non-functional targets

Timing is an operational objective, never a substitute for fencing or durability.

| Measure | Initial release target |
| --- | --- |
| Planned partition handoff | Under the transition envelope, continuously submitted commands all reach a terminal outcome within one second of their original send, deterministic fingerprints match, and the predictive client never freezes or activates the three-second stale overlay. |
| Planned WebSocket drain | Zero measured interval without either old or replacement authenticated, game-ready socket for supported clients; completion within the 20-second client handoff window and 45-second application deadline. |
| Crash takeover with another ready task and healthy Valkey | p99 first fresh authoritative output within five seconds. |
| Hard gateway crash with survivor and healthy ingress | Automatic authenticated game resume p99 within ten seconds; uninterrupted transport is not promised. |
| Ready capacity | A started task is counted as added capacity only after it appears healthy in Traefik; no zero-ready interval occurs. User-visible timing is measured by planned new-user availability rather than an internal readiness-transition timestamp. |
| Checkpoint freshness | The one-second wall-clock cadence is a deterministic code invariant; under the certification load, the maximum persisted age across the active-game index remains below the configured ten-second fail-closed limit. |
| Assignment balance | Owner counts differ by at most one partition. |
| Assignment movement | Minimum assignment-map Hamming distance after excluding ineligible owners and satisfying the balance rule. |
| Planned new-user availability | p99 reaches a ready backend within ten seconds and sees no terminal connection error. Transient internal `503` retries are allowed and measured. |
| Correctness invariants | Zero violations across deterministic, chaos, and load suites. |

The supported staging evidence consists of three independent gates. The
128-session / 64-duel headroom gate proves natural CPU or memory scale-out.
After those clients and games are gone, the planned `1 -> 10 -> 1` gate uses
128 game sessions / 64 duels with `every-tick` commands on all fifty partitions,
23 fixed context probes, and bounded open-loop idle admission at four starts
per second with a 64-session in-flight ceiling and one-second post-ready hold.
The ten-task capacity gate separately holds at least
256 authenticated sessions / 128 duels, from a configured 272-session / 136-duel
cohort, for at least five minutes. This phase separation is not a reduction of
the capacity envelope; it prevents an overloaded one-task destination from
being misdiagnosed as a handoff failure. The five- and ten-second recovery
objectives must pass inside the full ten-task capacity envelope. Timing targets
may be changed only by an explicit product decision; correctness properties,
including fail-closed fencing, may not be relaxed.

## 14. Observability and alerts

Use bounded-cardinality partition/task labels. Do not label production metrics by arbitrary user ID.

Required metrics:

- task lifecycle, membership age, ready/live/draining task counts, and local readiness;
- assignment version/age/imbalance plus desired-owner and active-lease mismatch/deficit;
- partition unowned duration and fenced-write rejection count;
- pending command count/oldest age, claims, ACKs, resends, deduplications, rejections, pending completions, and quarantines;
- checkpoint age/size/failures and active-game index parity;
- recovered games, replay count, and deterministic fingerprint divergence;
- active WebSockets and planned-drain failures;
- load-test reconnect, authentication, rejoin, snapshot, per-command terminal-outcome latency, command-outcome barrier, usable-session-gap, and socket-generation evidence, combined with real-browser Playwright stale-overlay evidence;
- match claim conflicts and prevented duplicate completion effects;
- ECS CPU/memory and staging evidence for Serverless Valkey latency, ECPU, bytes, connections, network traffic, throttling, evictions, and functional shared
  Traefik/NAT capacity through connection success, admission latency/errors,
  CPU, and network. Connection-tracking occupancy is optional when available.

Critical alerts:

- zero ready tasks;
- a partition unowned beyond the crash-takeover objective;
- any unexpected fenced-write rejection;
- assignment stuck with eligible tasks or imbalance;
- oldest pending command or checkpoint age approaching the recovery budget;
- active-game index/checkpoint mismatch;
- fingerprint divergence after recovery;
- any planned-drain failure;
- any Serverless Valkey eviction or throttled command, or sustained service-side latency inconsistent with the command budget.

## 15. Acceptance and chaos test matrix

Each test must assert the concrete identifiers relevant to its invariant: game and command IDs for execution tests, assignment versions and lease tokens for ownership tests, and socket generations for handoff tests. Passing because logs contain no errors is insufficient.
The fixed Gate A, B, and C clients must observe zero sparse-window rejection
fences. A fence is valid protective protocol behavior for a pathological
session, but it fails load certification rather than counting its covered
rejections as healthy command throughput.
Command-outcome latency starts immediately before the original awaited socket
write and ends when a dedicated, continuously polled socket reader receives
the terminal-outcome frame, before lossless delivery to the synthetic game
driver. Reader-receipt-to-driver lag is reported separately as load-generator
diagnostic evidence. No frame may be discarded because the handoff for an
active socket reaches a capacity limit. Retiring a socket may discard its
private unread queue; exact pending-command accounting and the durable outcome
barrier must then recover every outcome or fail certification. The real-browser
suite remains the authority for JavaScript rendering and stale-overlay behavior.
For a pending command closed by a proven terminal-default barrier, the
observation time is the later of that barrier's dedicated-reader receipt and
the command's original send. This covers the legitimate cross-task ordering in
which the reader has already received the terminal frontier while the driver is
finishing a command send; it never produces a negative latency and does not
permit a reasonless or pre-terminal barrier to resolve anything.
Command-outcome certification accepts report schema 11 or newer only when
`metadata.command_outcome_latency_basis` is exactly
`original-send-to-dedicated-reader-frame-receipt`.

| Test | Pass criteria |
| --- | --- |
| Scale `1 -> 10` under the 128-session / 64-duel planned-transition load while games receive commands on all fifty partitions | The independent 128-session natural scale-out cohort has already exited and the service has returned to a healthy one-task baseline. Exactly 45 partitions move between the settled endpoint assignments, owner counts become five each, assignment versions advance monotonically, no active WebSocket hard-reconnect occurs, every full transition second resolves exactly its submitted commands with no terminal outcome taking more than one second from original send, and fingerprints match. The real-browser planned-drain suite and staging protocol evidence jointly prove that no stale overlay occurs. |
| Scale `10 -> 1` under the same 128-session / 64-duel transition load with 10 idle, 10 lobby, three unmatched matchmaking, and open-loop admission clients | Every partition has active command work before movement. Exactly 45 partitions move between the settled endpoint assignments and versions advance monotonically; every observed drain handoff has zero usable-session gap and one command owner; every full transition second resolves exactly its submitted commands with no terminal outcome taking more than one second; no active socket hard-reconnects. The admission probe starts four sessions per second throughout the scale-in window, holds each successful session for one second after it becomes ready, and never exceeds its 64-session in-flight safety ceiling; each reaches a ready backend, with p99 initial WebSocket authentication within ten seconds and no terminal error. The one-task destination remains ready and services lease renewal, membership heartbeat, checkpoint, and event traffic without starvation; no game completion is awaited. |
| Kill after command `XADD`, before group delivery | Successor reads it as new work and applies one logical result. |
| Kill after delivery into pending, before schedule | `XAUTOCLAIM` recovers it and applies one logical result. |
| Kill after schedule, before checkpoint | Replay does not lose or double-apply the command. |
| Kill after checkpoint, before `CommandScheduledV2` publication | Replayed `CommandOutcomes` clears the outbox without reapplying the command. |
| Kill after `CommandScheduledV2`, before `XACK` | Successor reclaims and retires or replays as required, but one logical effect and no pre-reanchor duplicate incremental schedule reaches consumers. |
| Fill one client session's sparse exact-result window, then crash before the overflow rejection is checkpointed | The overflow command remains in the consumer-group pending list with its exact journaled decision; takeover installs the same rejection fence, emits no conflicting incremental outcome, and does not change engine state or the server command counter. |
| Crash after checkpointing a sparse-window rejection fence but before its event reaches the client | Recovery outcomes expose the same fence; the client clears covered entries, keeps lower gaps pending, never resends a covered identity, and rotates only after every lower pending identity resolves. |
| Reject sequence N, accept N+1, and lose both terminal events | Reconnect does not treat the higher sequence as proof that N was accepted; resolved watermark/sparse outcomes clear each entry according to its own result. |
| Complete a game while one authenticated command crosses buffered terminal delivery | The completion commit and command append are atomically ordered. An append that wins receives its exact durable result; an append that loses is absent from the command stream and is rejected by the ordered terminal-default barrier. The client and certification driver first observe the terminal snapshot, finish with zero pending identities, and retain the empty terminal tombstone. |
| Reload a durably completed game while recovery state is missing or still contains the preceding nonterminal checkpoint | The durable terminal snapshot remains readable, but `CommandOutcomesComplete` has no terminal default and cannot clear a pending identity. A preceding nonterminal recovery bridge does not authorize terminal disposition. |
| Cold-join one game while its partition contains many unrelated actors and one actor mailbox is full | Requests name only the joined game and are published no faster than once per 500 milliseconds per joining socket. The executor does not await an actor reply or fan out to unrelated actors; a full or temporarily missing target is retried without blocking partition command delivery. Startup readiness and partition-gap requests remain full-partition proofs. |
| Hold 128 client commands unresolved, then submit one more | The first 128 identities remain intact and resendable; the client does not allocate or send identity 129 until one entry resolves. |
| Leave client sequence 1 unresolved while sequences 2 through 129 resolve, then submit sequence 130 | The client retains sequence 1, does not allocate sequence 130, and therefore cannot overflow the server's 128-entry sparse-result window. After sequence 1 resolves, the next allocated identity is exactly sequence 130. |
| Pause owner A beyond its lease, let B acquire, then resume A | Every event, checkpoint, finalization, active-index mutation, and ACK from A is rejected. |
| Let stale consumer A read or claim after B acquires exclusive authority | A's atomically fenced read/claim is rejected without changing the PEL or last-delivered ID; B receives the exact entry in stream order and A cannot dispatch a committed mutation. |
| Crash coordinator during assignment write | Readers observe a complete old or new document; recovery reconciles monotonic versions. |
| Kill a task that owns both the coordinator lease and partitions | A survivor reacquires coordination, publishes a complete assignment, claims pending commands, and resumes authoritative output inside the crash objective. |
| SIGKILL one ECS task during the fixed non-production certification load while another task is ready | The task receives no graceful cleanup; its membership and leases expire; a survivor recovers its naturally observed pending backlog and resumes fresh authoritative output within five seconds; affected gateway sessions automatically authenticate, rejoin, and receive a fresh snapshot within ten seconds; commands have one logical outcome; and ECS restores healthy capacity. This is the only distinct external crash action required. |
| Change eligible membership `1 -> 4 -> 2 -> 10 -> 1` at 500 ms intervals while prior owners remain live | The safe changes coalesce into one version after the final quiet window; all fifty partitions then have one matching desired/live owner, owner counts differ by at most one, and no stale assignment overwrites a newer one. Removing or warming a current owner during the pending window preempts it and reconciles immediately. |
| Recover RNG-dependent games, queued commands, and custom slow ticks | Recovery envelope fields restore the same logical fingerprint and wall-clock checkpoint cadence. |
| Recover with 10,000 unrelated snapshot keys in Valkey | Recovery reads only the indexed games for the acquired partition; unrelated key count does not change the fetched envelope count. |
| Leave a command pending beyond 8,192 later appends | Safe trimming retains and reclaims the pending command. |
| Fail checkpoint writes for nine seconds, then for eleven seconds, with the ten-second age budget | In the nine-second case commands remain pending and checkpointing recovers; in the eleven-second case the actor fails closed at the budget without falsely retiring work. |
| Block one game actor while a later command targets another game in the same partition batch | The unrelated game receives its command before the blocked actor is released; each game retains one unresolved delivery at most, cursors never regress, and a handoff drains only the accepted prefix while leaving the untouched suffix recoverable in the PEL. |
| Flood the executor command stream beyond the legacy gateway channel capacity while publishing a later authoritative event | The event-only gateway reader continues delivering the event promptly because it reads neither commands nor snapshot requests. |
| Start a gateway while a partition is empty, active, or temporarily has no subscribed executor | Executor placement can converge before gateway routing; the gateway retries one boot-unique request, consumes every preceding requested snapshot and then its exact same-stream completion marker, and remains unready until that proof arrives. Snapshot publication waits on the existing actor checkpoint without blocking partition command dispatch. |
| Remove a gateway event reader's nonzero cursor from the bounded stream before initial readiness or after routing | The reader emits a transport discontinuity, the critical replica worker exits, readiness is false, and no surviving old completion marker can make the task routable. |
| Fail an owned partition executor closed, remove its final local handle, then begin task drain | The process-boot failure latch makes executor drain fail and no WebSocket `Drain` is announced; lease expiry plus ordinary reconnect remains authoritative. An empty handle map alone never passes. |
| Inject failure at each WebSocket drain phase | Old socket remains usable until replacement auth, rejoin, snapshot, and switch complete; only one sends commands. |
| Let the old socket advance beyond the frozen Pong frontier, promote the caught-up candidate, then deliver covered candidate snapshots both before and after the socket swap | Visible game state never rolls backward. Covered nonterminal stream-zero and sequenced frames remain suppressed until the promoted stream exceeds the old applied watermark; a terminal snapshot remains authoritative. |
| Close an old socket after a new socket restores the same lobby/game | Old cleanup does not remove new presence or active context. |
| Persist a lobby member without delivering its Pub/Sub hint, then enqueue a stale hint | Every connected member converges to the durable roster by the next successful periodic read; the stale payload is never presented as authoritative state, and unchanged later reads enqueue no duplicate update. |
| Attempt `CreateLobby` and `JoinLobby` while already in a lobby | Both requests receive `AccessDenied` and require `LeaveLobby`; the current presence, roster subscription, match subscription, and chat subscription continue without duplicate roster or history delivery. |
| Crash gateway during an ambiguous command send | Resend uses the same identity; outcome is one acceptance or one terminal rejection. |
| Admit new sessions continuously while a backend performs the configured eight-second route-withdrawal wait | Existing sockets migrate; late new upgrade attempts may receive retryable `503`, and a same-IP certification burst may receive retryable `429`, but every new session retries through the ordinary reconnect path, reaches a ready backend within the unchanged ten-second deadline, and surfaces no terminal user error. The per-IP sustained average remains 50 upgrades per second, the burst is 512, and provider/health settings plus exact healthy-backend coverage corroborate capacity; no internal readiness-transition timestamp is required. |
| Repeat and concurrently submit one lobby admission, then submit two lobbies containing the same user | One immutable lobby identity and one per-user claim win; every queue/MMR index has one exact member; conflicting admission is rejected; cancellation or match commit removes every winning claim so no stale lobby can rematch a user. |
| Lose admission or cancellation responses, retry them, and interrupt the caller between durable queue mutation and presentation refresh | The atomic queue identity and lobby metadata state agree (`queued`, `waiting`, or `matched`); retries converge without a hidden queue member or stranded queued banner. |
| Kill a gateway after it reads `QueueForMatch` but before the client observes durable admission | The client restores the exact lobby. `waiting` replays its one retained intent; `queued`, `matched`, or `JoinGame` clears it. Every member receives exactly one game assignment, and an assignment received during lobby restoration is not discarded. |
| Send `QueueForMatch` before the socket has joined a lobby | The server rejects it without creating a hidden lobby or queue identity; normal browser entry points establish and retain the explicit lobby first. |
| Concurrent matchmakers select the same lobbies | Exactly one atomic claim wins; no player or lobby belongs to two committed matches. |
| Kill matchmaker before/after the matchmaking commit, destination outbox delivery, and source acknowledgement, including loss of each response | Before commit, entrants remain queued. After commit, match/mappings/outbox exist. Any task delivers exactly one partition `GameCreated`; retries repair either half, and the executor creates the checkpoint before ACK. Disconnected recipients recover from mappings. |
| Commit immediately before a connected lobby listener subscribes, then drop or duplicate the Pub/Sub hint | Subscribe-then-read or the five-second reconciliation forwards the durable game ID once; duplicate hint/read overlap does not send a second `JoinGame`, and a later play-again game ID is still delivered. |
| Kill after the fenced Valkey completion commit and before each DynamoDB effect or its confirmation marker | A successor reloads the same immutable completion revision; completed game, XP, MMR, rankings, and high scores converge to one application per effect key, and pending completion state is retained until all effects are confirmed. |
| Time out the fenced completion commit after it may have executed, then retry it repeatedly | The exact same completion record is accepted; one terminal snapshot and one terminal status are observable, matchmaking cleanup converges, and no completion effect is duplicated. |
| Deliver the full terminal snapshot to a gateway replica | The snapshot is broadcast before local eviction; the immutable completion record, final recovery envelope, stored snapshot, and pending-effect index are already durable from the same fenced transaction, so no command-stream completion marker is consumed or awaited. |
| Delay completion cleanup until a player or lobby is mapped to a newer game | The old active-match record is removed, but every newer user/lobby mapping remains unchanged. |
| Complete a game on a takeover executor with a different task/server identity | Final state and every durable effect commit once under the original game ID and one completion revision; original executor identity is not required. |
| Join an active game through a newly ready cold task | Snapshot warming succeeds inside the six-second authorization deadline or returns `GameWarming` with a 500 ms retry hint, never a false missing-game result. A playing-phase certification client pauses command emission, retries `JoinGame` on the same authenticated socket within its existing game deadline, and resumes only after a fresh snapshot and `CommandOutcomesComplete`; it must not manufacture a reconnect or continue sending against a cold replica. |
| Make Valkey unavailable through the deterministic local fault proxy | Readiness drops within seven seconds, liveness remains healthy, and restoration creates no conflicting authority. A remote ElastiCache outage is not a separate release test because availability during that accepted dependency outage is out of scope. |
| With recovery retention set to 60 seconds, crash the sole task and delay replacement 30 seconds | The documented availability gap occurs, then games recover automatically. |
| With recovery retention set to 60 seconds, delay sole-task replacement 61 seconds | The game returns the explicit unrecoverable outcome and no fabricated state. |
| Run the fixed 128-session / 64-duel `every-tick` natural scale-out gate from the two-vCPU minimum task | CPU or memory target tracking produces a successful scale-out above one while the pre-movement baseline and automatic movement window both keep every command outcome within one second, without a task exit, readiness failure, or manual desired-count update. After the added tasks are ready in ECS, Traefik, and the executor control plane, at least 60 complete post-ready seconds satisfy the same command budget and produce scheduled work on all fifty partitions. Failure to trigger, insufficient post-ready duration, or a budget violation is a failed certification, not permission to adjust the fixed cohort or weaken the budget. The load then finishes, all of its clients and games reach zero, and none are reused for the forced staircase. |
| Ramp at four new sessions per second, then hold 256 authenticated sessions / 128 duels with `every-tick` commands for at least five minutes | The run begins only after ten tasks are healthy in ECS and Traefik and settled in the executor control plane; every full hold second resolves exactly its submitted commands with no terminal outcome taking more than one second; Serverless Valkey reports zero `Evictions` and `ThrottledCmds`, no write failure occurs, and there is no zero-ready interval, ECS health failure, or Traefik health failure. |
| Exhaust the CPU of a planned scale-in destination until control operations miss their deadlines | The run fails the destination-capacity gate and is not classified as a handoff-protocol defect. No stale or unproven mutation commits: fencing rejects it, the executor fails closed, cooperative drain is not advertised, and ordinary lease-expiry recovery remains authoritative. |
| Run the complete protocol against actual ElastiCache Serverless Valkey 8 | The AWS cache identity reports major/full engine version 8; TLS certificate validation, RESP3, and cluster discovery through the advertised 6379 primary and 6380 read endpoints succeed, as do operations across every hash-slot family; all fifty deterministic partition-hot lanes, all fifty independently bootstrapped partition-scoped recovery-read lanes, and the independently bootstrapped control, single per-task checkpoint-write, task-wide maintenance, separate metrics, loss-tolerant Pub/Sub, and stream-reader connections operate under the fixed load without cross-role or cross-partition queue amplification, and no subscription push confirmation is consumed as an ordinary command response; no `CROSSSLOT`, `MOVED` exhaustion, unsupported `KEYS`, or nonzero database error occurs; all Lua/multi-key key-family tests pass. A standalone local Valkey run alone is insufficient evidence. |
| Stall one partition's recovery-read lane while reading another partition's recovery envelope | The other partition completes its recovery read within one second, and the metrics dispatcher is independently bootstrapped from every correctness-bearing recovery lane. |
| Stall the task-wide maintenance lane during live command delivery | Pending-completion scans and trim may pause, but the partition executor continues scheduling and checkpointing commands before maintenance is released; completion retry and trim each retain at most one background worker per partition. |
| Stall one partition's `GameCreated` destination lane while another partition has a valid durable outbox record | The unstalled partition publishes and compare-deletes its record within one second while the stalled record remains authoritative in the outbox; after release, the stalled partition publishes exactly once and is acknowledged. |
| Make one `GameCreated` delivery fail for a record-specific wrong-type marker, followed by a valid record in the same partition batch | The failed record remains in the durable outbox, while the later record still publishes, is compare-deleted, and receives marker expiry within one second. |
| Remove all certification load from a verified ten-task baseline | CPU or memory target tracking returns the service automatically to `minTasks=1`; the activity is distinct from the forced continuity staircase. |

## 16. Delivery plan

### Phase 0 — Foundations and observability

- Add the metrics and deterministic fault hooks required by the acceptance matrix.
- Add chaos runners alongside the existing resilience test scripts.
- Provision uncapped ElastiCache Serverless for Valkey, add zero-tolerance eviction/throttling alarms, and statically test every atomic hash-slot family.
- Record the fixed certification envelope and its Valkey, functional
  Traefik/NAT, per-task CPU/memory, and socket evidence.

Exit gate: current behavior and every safety invariant are measurable.

### Phase 1 — Authoritative recovery path

- Add the versioned recovery envelope and partition active-game index.
- Add stable client command identity, server deduplication, and resolved command outcomes.
- Add idempotent DynamoDB finalization/effect writes.
- Add truthful live/ready endpoints.
- Add server support for explicit authentication and drain messages.
- Make atomic matchmaking admission, cancellation, and commit the only queue lifecycle path.
- Make gateways publish durable commands and executors consume them through stable consumer groups.

Exit gate: crash takeover, fencing, command recovery, and atomic matchmaking pass deterministic tests.

### Phase 2 — Client reconnect and planned handoff

- Move the command sequence/outbox outside the rebuilt WASM engine.
- Add socket-generation guards, immediate/jittered reconnect, explicit auth handling, strict validation of the current required capabilities, and resolved-outcome support.
- Add dual-socket planned drain and require explicit authentication before promotion.
- Separate transport loss from explicit lobby leave.

Exit gate: crash reconnect and planned make-before-break handoff remain stable without user action.

### Phase 3 — Assignment, readiness, and planned drain

- Enable membership-driven balanced assignment recomputation.
- Handle SIGTERM with the bounded cooperative partition and WebSocket drain.
- Point Traefik to readiness, ECS to liveness, remove stickiness, tune active checks, and fix Traefik's own ping health check.
- Configure and validate ECS stop timeout.
- Enable normal autoscaling after the ownership and recovery smoke tests pass.

Exit gate: deterministic local SIGKILL, stale-owner, and planned-handoff tests
pass, and the non-production deployment is ready for the two external Phase 4
results.

### Phase 4 — Non-production certification and production ramp

- Run the fixed planned-path staging suite and the one separately authorized
  non-production task-SIGKILL exercise.
- Only after both pass on the current `main` commit, manually dispatch
  production with that successful exact-commit certification run ID, then ramp
  by environment/region while watching recovery, pending, checkpoint, socket,
  and duplicate-effect metrics.

Exit gate: both non-production external results and all definition-of-done
criteria pass before the production ramp.

## 17. Component impact

| Area | Expected changes |
| --- | --- |
| Assignment module + `redis_keys.rs` | Membership, coordinator, canonical assignment, monotonic partition views, allocator, and explicit Cluster hash-slot families. |
| `server/src/redis_utils.rs` | One standalone/cluster-aware connection abstraction; TLS and Redis Cluster selection from the deployment URL. |
| `server/src/game_bus.rs` | Executor consumer-group reader, safe command retention, deterministic routing through fifty prewarmed partition-hot lanes and fifty independently prewarmed partition-scoped recovery-read lanes, event-only gateway subscriptions, lease-aware single-slot scripts, versioned checkpoint APIs, idempotent outbox delivery, and separately retryable completion cleanup. |
| `server/src/game_executor_v2.rs` | Recovery envelope, dedupe, active-game index, backlog-first resume, cooperative checkpoint/release, idempotent finalization. |
| `server/src/game_server.rs` and `main.rs` | SIGTERM, lifecycle state, readiness state, critical-worker failure policy, one bounded drain deadline. |
| `server/src/replication.rs` | Event-only resumable partition readers and broadcast-before-eviction handling for atomically durable terminal snapshots. |
| `server/src/matchmaking.rs` and manager | Atomic queue admission/cancellation/commit scripts, durable user-to-game mappings, and a bounded `GameCreated` outbox routed through one nonblocking delivery worker per fixed partition. |
| `server/src/ws_server.rs` | Explicit auth response, drain protocol, generation-safe cleanup, active-game resolution, retryable warming. |
| `client/web/contexts/WebSocketContext.tsx` | Immediate/backoff reconnect, socket generations, dual-socket drain, explicit auth, one command owner. |
| Client game integration | Stable session command IDs, external outbox, resolved watermark/sparse outcomes, terminal rejection. |
| `cdk/lib/valkey-stack.ts` and `fargate-stack.ts` | Serverless Valkey, TLS cluster URL, Serverless metrics/alarms, liveness/readiness routing, sticky-cookie removal, health timing, and stop timeout. |
| Production deployment workflow | Manual-only mutation, successful exact-commit staging-certification gate, immutable image deployment, and post-deploy verification. |
| Development certification infrastructure | Fixed public `dev.snaketron.io` reuses but never owns or mutates the production VPC. Protected Network ingress/EIP/A-record/EBS/TLS state plus the ECS cluster, ECR repository, and DynamoDB tables remain reusable between runs; each run creates and deletes only its Serverless Valkey, Server, and Monitoring stacks and stops the persistent ingress after cleanup. |
| Traefik configuration | Automatic health-based withdrawal/discovery and valid self-health endpoint. |
| Test runners | Deterministic failure points, scaling/load scenarios, stale-owner and pending-entry tests. |

## 18. Decisions and open tuning

### Locked decisions

- Crash recovery is the correctness path; SIGTERM is an optimization.
- Desired assignment is persisted explicitly; internal `hashring` state is not.
- Fixed fifty-partition placement uses the direct balanced/minimal-movement allocator; no hash-ring dependency is required.
- Active authority is an exact, unique lease token.
- Consumer groups are executor-only.
- The fenced consumer-group executor is the only executor implementation.
- Gateway replicas tail only authoritative partition events; they do not consume
  executor commands or snapshot requests.
- `CommandScheduledV2` is the positive semantic acknowledgement; `XACK` is still required internally.
- Checkpoints remain full and per game.
- Recovery reads use fifty fixed partition lanes, resilience metrics use a
  separate dispatcher, and full-state checkpoint/completion writes retain one
  dispatcher per task. No additional pools, correctness-bearing deadline
  increases, or cache layer are part of this release.
- Gateway and executor remain in the same binary/service but are logically independent.
- Planned task removal uses dual sockets; executor movement alone never moves sockets.
- Matchmaking safety comes from atomic admission/cancellation and one matchmaking-slot commit, plus one narrow idempotent outbox bridge into the executor partition slot; it does not require a singleton or generic saga system.
- Readiness and liveness are separate.
- Executor-placement eligibility is also separate from gateway readiness so an
  unready first/replacement gateway can own the executor that completes its
  ordered initial-snapshot barrier.
- CPU/memory autoscaling and `minTasks=1` remain.
- Regional Serverless Valkey and single-ingress availability risks are accepted for this phase.
- Serverless Valkey uses its fixed `volatile-lru` policy. CDK sets no data/ECPU usage maximum; any eviction or throttling fails certification and alarms in production.
- Production planning and mutation are manual-only, restricted to current
  `main`, and require a successful main-branch Ephemeral Development
  Certification run for the exact outer-repository commit.
- Development certification uses the fixed `dev.snaketron.io` hostname. Its
  protected ingress/EIP/EBS/ACME state and ECS/ECR/DynamoDB foundations carry no
  run expiry; the ingress instance is started for certification and stopped
  afterward.
- CloudFormation retains the production Serverless cache on stack deletion or
  replacement. Development Serverless Valkey, Server, and Monitoring are
  disposable runtime stacks; the reusable development foundations are not.

### External release evidence still required

The deterministic suite is the release evidence for safety invariants that do
not require AWS: group-aware trimming retains and reclaims more than 8,192
pending commands and bounds the stream after ACK; checkpoint cadence is
wall-clock driven and checkpoint failure crosses the fail-closed age budget
without falsely retiring work; and real-browser Playwright covers every planned
handoff phase and the stale/disconnected UI.

Only these external results remain:

- the non-production staging run passes three distinct gates: natural
  target-tracking scale-out under 128 `every-tick` game sessions; planned
  `1 -> 10 -> 1` under the one-task-capacity-valid 128-session transition
  cohort, 23 context probes, and bounded open-loop admission at four starts per
  second with a 64-session in-flight ceiling;
  and the separate 272-session ten-task capacity run with its 256-session /
  128-duel five-minute floor. The planned gate must also pass exact
  healthy-backend, checkpoint-age, Valkey-capacity, and 45-second application
  shutdown checks while retaining the configured 60-second ECS container
  stop-timeout safety margin; and
- during a separate run of the fixed ten-task 272-session crash envelope, one
  separately authorized non-production ECS task receives SIGKILL without
  graceful cleanup.
  Its naturally observed affected-partition backlog must meet the five-second
  authoritative-output objective, affected gateway sessions must meet the
  ten-second recovery objective, commands must retain one logical outcome, and
  ECS must restore healthy capacity.

No separate Fargate-host, remote-Valkey-outage, staging-browser rendering,
connection-tracking, synthetic maximum-backlog, or internal
local-readiness-to-route timing run is required. Those either duplicate the two
evidence paths above, test an accepted unavailable dependency, or add telemetry
without strengthening the user-visible guarantee.

The records below preserve the earlier combined-harness `Run A` / `Run B`
terminology and its then-current load decisions. They are historical diagnostic
evidence, not the current three-gate requirement. Their full-absence cleanup
inventories also describe the then-current fully disposable infrastructure and
must not be read as the cleanup contract for the current persistent foundations.

Neither external result has a passing report attached. The fixed-node attempt
exposed cache saturation and handoff defects. The first Serverless-backed
planned attempt ([GitHub Actions 29990657012](https://github.com/lopatin/snaketron-io/actions/runs/29990657012))
proved Valkey 8.1 provisioning, TLS/Cluster connectivity, zero cache throttling,
zero eviction, and cleanup, but failed the one-second command budget after
exposing both one-task saturation and concurrent task warm-up amplification of
full-game snapshot/checkpoint work. It also exposed an invalid sequential
Traefik timing calculation. Both
attempts are diagnostic evidence, not release evidence. A second exact-source
Serverless-backed attempt ([GitHub Actions 29996912370](https://github.com/lopatin/snaketron-io/actions/runs/29996912370))
again provisioned Valkey 8.1 over TLS/RESP3, moved the expected partitions,
completed 61 of 61 planned active-game handoffs with zero socket reconnects and
zero usable-session gap, admitted 208 of 208 concurrent new sessions, and
recorded zero cache throttling or eviction. It nevertheless failed: all 394
full baseline seconds exceeded the one-second command-outcome budget, with a
12.114-second maximum, and six sessions in three newly created lobbies missed
their exact roster because the WebSocket path treated at-most-once Pub/Sub as
authoritative state. Cleanup independently verified that no development
resource remained. This run is also diagnostic evidence, not release evidence;
the crash suite did not run. The release remains
blocked until fresh planned and crash runs both pass end to end.

A third exact-source Serverless-backed attempt
([GitHub Actions 30007863987](https://github.com/lopatin/snaketron-io/actions/runs/30007863987))
kept Valkey healthy under 197,000--234,000 commands per minute, with zero
throttling or eviction and service-side read/write latency averaging under 1.5
milliseconds. It nevertheless failed before scale-out: paired commands for one
game forced the dispatcher to settle every unrelated game, so one task
self-throttled at 30% average CPU while command outcome latency reached 51.605
seconds and 24 of 574 sessions timed out waiting for an initial snapshot. The
run therefore did not manufacture a scale event by lowering the CPU target or
forcing desired count. Cleanup succeeded and an independent inventory again
found no development resource remaining. A selective per-game settlement fix
then sustained 96 local sockets / 48 duels for three minutes: all 172,093
commands received terminal outcomes, maximum outcome latency was 170
milliseconds, all 192 session attempts passed, and the pending backlog remained
below one second. This local result is causal diagnostic evidence only; a fresh
AWS run remains required.

The next selective-settlement AWS attempt
([GitHub Actions 30014346604](https://github.com/lopatin/snaketron-io/actions/runs/30014346604))
showed that command-only interleaving was incomplete: lifecycle markers still
split adjacent per-game runs and the exact-source dispatcher still globally
settled pending actors on status, creation, terminal, and inactive paths. One
task again processed roughly 430 outcomes per second against roughly 950
submitted commands per second, pending age reached about 70 seconds, and the
maximum outcome latency reached 53.6 seconds before any scale event.

The following exact-source Serverless run
([GitHub Actions 30021797806](https://github.com/lopatin/snaketron-io/actions/runs/30021797806))
proved substantially more of the system. Automatic `1 -> 2 -> 1` and forced
`1 -> 10 -> 1` assignment movement completed, all 64 planned handoffs retained
zero usable-session gap, and 1,248 of 1,248 sessions plus 967,475 of 967,475
command outcomes completed with no ordinary disconnect. Valkey 8 remained at
zero throttling and eviction. The run still failed: reset-to-one had two
1.1--1.2-second sent windows, forced scale-in had one 1.040-second window, and a
later synchronized game rollover grew maximum outcome latency to 44.765 seconds
and game-join p99 to 51.701 seconds. Capacity Run B and SIGKILL therefore did
not run. DynamoDB separately throttled completion-effect and admission writes,
but its largest throttle intervals occurred while command outcomes were
healthy, and throttling ended while the rollover backlog kept growing; command
submission, scheduling outcomes, checkpoints, and ACKs are Valkey-only.

The subsequent patch round-robins the complete per-game stream projection,
makes lifecycle settlement game-local, and preserves global drains only for
Poison, handoff, the bounded fanout window, failure, and batch completion. Its
focused causal regressions and all 163 server library tests pass. A local
96-session / 48-duel rollover run through a simulated six-millisecond cache RTT
completed 288 of 288 sessions, 144 of 144 games, and 251,700 of 251,700 command
outcomes with zero failures, zero disconnects, no sent-second over one second,
and a 291-millisecond maximum outcome. This remains diagnostic evidence; the
release is still blocked on fresh complete planned and SIGKILL AWS runs.

The next exact-source Serverless attempt
([GitHub Actions 30030317623](https://github.com/lopatin/snaketron-io/actions/runs/30030317623))
completed 538 of 538 sessions and returned terminal outcomes for all 399,655
submitted commands without a disconnect. Valkey again recorded zero throttling
and eviction with roughly one-millisecond service-side latency. The run still
failed before scale-out: the 96-session cohort held service-average CPU at only
53--59%, below the unchanged 70% target, maximum command-outcome latency reached
32.1 seconds, game-join p99 reached 32.7 seconds, and pending age reached about
33 seconds. Recovery inspection proved that every authoritative command added
a speculative-command tombstone that could never be consumed on the server;
the set therefore grew forever and was duplicated into every checkpoint and
snapshot. Cleanup succeeded and an independent inventory found no development
runtime resource remaining.

The corrective patch bounded that existing queue bookkeeping, reset the actor
tick interval after slow publication so queued mail got a bounded opportunity,
and routed large checkpoint, takeover, reconnect-outcome, completion-record,
and metrics traffic through the then-single independently bootstrapped recovery
dispatcher. It did not add per-partition pools or a new persistence format. A local
144-session / 72-duel run then passed 288 of 288 sessions, 144 of 144 games, and
258,446 of 258,446 command outcomes with zero disconnect, a 148-millisecond
maximum outcome, sub-second pending age, and a recovery-envelope plateau near
244 KB. CPU was roughly 79% at the full one-task plateau, supporting the
one-time fixed Run A recalibration without changing the CPU 70% / memory 80%
policy. This remains diagnostic evidence; fresh complete planned and SIGKILL
AWS runs are still required.

The next exact-source Serverless attempt
([GitHub Actions 30039460661](https://github.com/lopatin/snaketron-io/actions/runs/30039460661))
successfully exercised natural CPU target-tracking `1 -> 2` and forced
`1 -> 10 -> 1`. It completed 1,852 of 1,852 continuity sessions, 926 of 926 games, and all
1,653,922 authoritative command outcomes with zero disconnects, zero
reconnects, zero measured usable-session gap, and 256 of 256 planned handoffs.
The run nevertheless failed the unchanged one-second latency gate: the
144-session pre-scale baseline had 20 failing sent-seconds and a 2.023-second
maximum while holding one-task CPU at 95.7--98.3%; forced scale-out reached
3.278 seconds and scale-in reached 2.081 seconds. Valkey had zero throttling or
eviction and sub-1.4-millisecond average successful request latency.

The transition evidence also caught seven recovery reads and one retry-safe
fenced checkpoint write timing out together on the shared recovery dispatcher.
Unacknowledged work was retained, the next checkpoint succeeded, the game
completed durably, and no fence rejection or data loss occurred. The minimum
correction opened one fresh checkpoint-write dispatcher per task, leaving bulk
reads and regional metrics on the then-existing recovery-read dispatcher; it did
not add per-partition pools, new persistence, a timeout increase, or a reader
endpoint. The first follow-up calibration used 128 sessions / 64 duels based
on the saturated pre-split run; that was a declared hypothesis, not a product
invariant. Cleanup and its full absence verification succeeded. Capacity Run B
and SIGKILL did not run, so release certification remained blocked.

Exact-source Serverless run
([GitHub Actions 30046381977](https://github.com/lopatin/snaketron-io/actions/runs/30046381977))
then kept the configured 128-session / 64-duel stage active through the full
eight-minute scale-out observation window. It completed 768 of 768 sessions
and 384 of 384 games, resolving all 687,455 commands with a 488-millisecond
maximum
sent-second latency, no sent-second above 500 milliseconds, zero disconnects,
zero reconnects, and zero checkpoint failures. CPU minute averages were only
45.18--48.83%, so the unchanged 70% target correctly did not scale; the run
failed closed and did not enter the forced, capacity, or SIGKILL phases. The
new topology and the smaller cohort changed together, so this result must not
be presented as a causal A/B claim. The command rate fell only about 10% from the
144-session run while CPU, pending age, and latency collapsed, which is
consistent with removing a nonlinear shared-dispatcher queueing knee. Cleanup
and independent absence verification succeeded.

Run A is now fixed once from same-version evidence at 224 sessions / 112
duels. Subtracting the measured 3.5% idle CPU, scaling the observed 128-session
work, and restoring idle overhead projects 76.4--82.8% CPU: wholly above the
70% target without aiming near saturation. This revision replaces a falsified
cross-topology projection; it does not lower a gate, force desired count, or
add adaptive load. The 224-session cohort must not be adjusted again to make a
later run pass.

The first run at that frozen cohort
([GitHub Actions 30050625836](https://github.com/lopatin/snaketron-io/actions/runs/30050625836))
used the same exact server binary and the same ECS availability zone as run
`30046381977`, but ordinary successful Serverless Valkey request latency was
about 1.2--1.3 milliseconds rather than about 0.2 milliseconds. Valkey still
reported zero throttling and zero eviction, while service CPU stayed around
40%, so neither cache capacity nor the CPU target was the limiting resource.
The run failed before scale-out: 220 of 488 sessions failed while waiting for
their initial game snapshot. All 156,742 submitted commands eventually
received terminal outcomes, but only 70,137 were scheduled, 86,605 were
rejected after the backlog formed, maximum outcome latency reached 65.749
seconds, and oldest pending age reached 95.251 seconds.

All ten partition command consumers and their hot writes had been cloning one
`redis-rs` cluster connection. Those clones share one bounded client
dispatcher and the same underlying per-node multiplexed connections, allowing
one partition's work to create head-of-line queueing for every other partition
when ordinary cache latency rises. `ClusterConnection` supports multiple
in-flight requests, so this is not a claim that Redis strictly serializes
requests; the run instead implicates shared client-dispatcher/socket coupling,
not Serverless capacity. The minimum correction is the fixed topology in R4:
ten deterministic prewarmed partition-hot lanes, with the low-volume global
control connection and the then-existing recovery-read, checkpoint-write,
Pub/Sub, and fan-out connections kept separate. The durable `GameCreated`
scanner also routes to one bounded sequential worker per fixed partition so a
caller does not recreate cross-partition queueing above those lanes.
`PartitionLeaseStore` remains on control. This added no dynamic pool, per-game
connection, new persistence format, or retry policy.

Run `30050625836` is diagnostic evidence only. The 224-session / 112-duel
cohort, CPU 70% / memory 80% targets, one-second outcome gate, and all planned
and crash acceptance criteria remain unchanged. Fresh complete planned and
SIGKILL AWS runs are still required after the ten-lane correction.

The next exact-source Serverless run
([GitHub Actions 30057487544](https://github.com/lopatin/snaketron-io/actions/runs/30057487544),
outer commit `e23c6b5f3a62bdacdb51742aa12b03b5d8836a0c`, Snaketron commit
`36f7ac51912072fa6de3d6f2f43f9410d801c6de`) proved the actual Serverless
deployment and substantially more control-plane behavior. Natural CPU target
tracking moved `1 -> 2`; the deterministic staircase then moved `1 -> 10 -> 1`
with healthy assignments and leases. Valkey 8 reported zero `Evictions` and
zero `ThrottledCmds`. The run attempted 2,770 sessions, completed 2,747, and
submitted 2,167,559 commands.

It nevertheless failed the unchanged planned-path acceptance gates. Twenty-three
sessions failed: 19 timed out waiting for their games, while the remaining four
were two WebSocket-upgrade `429` failures and their paired lobby-session
failures. Forty-six planned handoffs hard-reconnected or were marked failed,
with a 3,497-millisecond maximum usable-session gap. Three hundred commands
remained unresolved, maximum command-outcome latency was 10,381 milliseconds,
and 534 original-send seconds exceeded the one-second outcome budget.

The failure evidence identifies one causal application-side gateway queue, one
independent recovery-dispatcher contention risk, and one bounded ingress-burst
mismatch rather than Serverless capacity. All 1,215 exact
`Timed out loading command outcomes for snapshot; retrying` warnings originated
in gateway recovery reads and were transient rather than hard warm-up-deadline
failures. Those reads nevertheless shared one bounded `redis-rs` dispatcher
with all ten partition takeover bootstraps and resilience metrics; 1,172
warnings occurred on the CPU-saturated scale-in survivor, and the warnings
covered every partition. This justifies isolating fixed partition lanes and
best-effort metrics, but does not claim that connection topology alone removes
survivor CPU pressure or within-partition duplicate reads. Separately, each gateway
replica read partition events, executor commands, and snapshot requests through
one sequential reader and three bounded channels. Continuous event traffic
starved command-channel draining; once that channel filled, the reader stopped
fetching later events. The 19 game-wait timeouts corresponded to 19
`Replica did not become subscribable after recovery snapshot` warnings, while
the same authoritative games later completed durably. Finally, the two `429`
responses occurred when the one certification source IP produced the valid
scale-in reconnect/admission burst against Traefik's 100-upgrade burst, rather
than because a backend was unhealthy.

The narrow correction is the topology and reader contract now specified in R4
and R6: ten independently bootstrapped partition-scoped recovery-read lanes, a
separate best-effort metrics dispatcher, event-only gateway readers, and
immediate local replica eviction only after broadcasting the atomically durable
terminal snapshot. The certification client also treats playing-phase
`GameWarming` as retryable on the same authenticated socket and pauses command
emission until a fresh snapshot and `CommandOutcomesComplete`; it then resends
any still-unresolved commands with their stable identities. The one checkpoint-write dispatcher per task
remains unchanged. Traefik keeps its 50-upgrade-per-second average and raises
only its burst to 512; the load client retries `429` through the existing bounded
admission/reconnect path, matching browser behavior. This does not add another
pool, increase the 750-millisecond Redis client deadline or any admission
deadline, add a recovery cache or persistence layer, alter the frozen
224-session / 112-duel cohort, change CPU 70% / memory 80% target tracking, or
relax the one-second, planned-handoff, or crash criteria.
The fresh run must validate the added Serverless connection count and determine
whether any recovery-read warnings remain; do not add a cache, more lanes, or
longer deadlines preemptively.

Cleanup for run `30057487544` completed successfully. Independent AWS inventory
found no development stacks, Serverless cache, ECS/EC2/EIP resources, ECR
repository, DynamoDB tables, alarms, log groups, DNS records, or scaling
targets; the imported production VPC remained untouched. This run is diagnostic
evidence only. A fresh complete planned run and a separate authorized SIGKILL
run are both still pending and required for release certification.

Exact-source Serverless run
([GitHub Actions 30078864960](https://github.com/lopatin/snaketron-io/actions/runs/30078864960),
outer commit `949bda23dc6d40de4117649c95a247b3361005f0`, Snaketron commit
`26d96f553977c9538b8e85c90d710855f2c0cad7`) reached the fixed
224-session / 112-duel Gate A and naturally scaled `1 -> 2`. The successor
recovered 54 active games and replayed 499 commands while taking five
partitions. All 2,882 sessions, 1,441 games, and 2,550,557 commands completed
with terminal outcomes, all ten partitions remained productive, and no client
disconnected, reconnected, or measured a usable-session gap. Serverless Valkey
reported zero throttling and eviction.

The unchanged one-second latency gate nevertheless failed: 11 of 323 complete
baseline seconds exceeded one second with a 1,501-millisecond maximum, and five
of 45 movement seconds exceeded it with a 1,951-millisecond maximum. The
best-effort regional metrics reporter fetched and deserialized roughly 25--27
MB of full recovery envelopes every 15 seconds on the one-vCPU task. Individual
scans lasted up to about 1.9 seconds and aligned with the latency bursts. The
evidence implicates telemetry interference; it does not show lost command
state, ownership instability, or Serverless capacity exhaustion. The fresh
unchanged-load rerun must confirm the causal correction.

The minimum correction changes no authority or storage protocol. The reporter
uses same-key `STRLEN` plus bounded header and tail `GETRANGE` reads to retain
exact checkpoint size, index identity, schema/protocol, and checkpoint-age
signals while cutting the worst observed sample payload by more than 400
times. `ActiveGameIndexMismatches` therefore measures bounded checkpoint
framing and index parity, not arbitrary corruption in the JSON middle;
authoritative takeover continues to deserialize and validate the complete
envelope. Do not add another metadata key, checksum, cache, pool, or timeout
solely to preserve full-body validation in best-effort telemetry.

The run's cleanup and an independent absence audit passed. No active
development resource remained, production stack timestamps and resource
identity were unchanged, and production stayed healthy. The run remains
diagnostic evidence only. Fresh complete planned and SIGKILL runs are still
required, with the fixed cohort and all acceptance thresholds unchanged.

The exact-source follow-up
([GitHub Actions 30085417447](https://github.com/lopatin/snaketron-io/actions/runs/30085417447),
outer commit `19f7fea443684dc3ab23134e0fe596065e8bf4e4`, Snaketron commit
`65e097ae9af948463e4948c181166f1c11b20aac`) exercised the bounded telemetry
path under the fixed 224-session target, which reached a peak of 112 concurrent
duels. All 1,344 sessions, 672 games, and 1,203,217 commands completed; every
command was scheduled and received a terminal outcome. There were zero
disconnects or reconnects. During all 485 full seconds between reaching the
224-session target and the autoscaling timeout, every partition was productive,
maximum command-outcome latency was 451 milliseconds, and no second exceeded
500 milliseconds or the one-second release budget. For comparable samples with
at least 100 active games on one live task, embedded-metric timestamp-to-log
delay fell from a 986-millisecond mean and 1,871-millisecond maximum in the
preceding run to a 227-millisecond mean and 500-millisecond maximum. Telemetry
continued to report checkpoint size and age; index-mismatch, checkpoint-failure,
lease-deficit, owner-mismatch, unowned-time, fenced-write-rejection, divergence,
drain-failure, and quarantine gauges remained zero. Serverless Valkey recorded
zero throttling or eviction.

The run failed closed before any ownership transition because the optimized
one-task workload no longer sustained the original 70% CPU target. Eight
consecutive one-minute CloudWatch averages during the full-load interval were
65.69%, 68.95%, 74.77%, 66.58%, 72.25%, 67.98%, 67.95%, and 71.06%; the series
contained no three consecutive minutes above 70%, no scale-out alarm action
occurred, and the service remained at one task. Planned handoff, capacity, and
SIGKILL therefore did not run. The old load-to-threshold calibration no longer
held on the corrected build. The lower telemetry delay and changed CPU profile
are consistent with reduced telemetry overhead; the result is not evidence of
an application or Serverless capacity failure.

The minimum operational correction is the normative 60% CPU target in R12,
shared by development and production. Memory remains 80%, both cooldowns
remain 60 seconds, the fixed 224-session load and every continuity threshold
remain unchanged, and no custom signal is added. A 65% target would leave only
0.69 percentage points below the weakest observed minute and is too sensitive
to ordinary run variance plus the managed alarm and task-start delay. At 60%,
the weakest minute has 5.69 points of margin and the latency-sensitive service
keeps real command and takeover headroom. Idle cost is unchanged because the
one-task floor remains. This policy change is not certified by the diagnostic
run; fresh complete planned and SIGKILL evidence is still mandatory.

The next exact-source run
([GitHub Actions 30089020521](https://github.com/lopatin/snaketron-io/actions/runs/30089020521),
outer commit `dad79987a3e3ac3bab23bb3e8f5dc292c25f658b`, Snaketron
commit `960ca7e62866dbd4a5a37cee7acda6cd37f35d0e`) validated the 60% CPU
policy. Gate A naturally scaled `1 -> 2`; all 2,876 sessions and 1,438 games
completed, and all 2,572,277 submitted commands received terminal outcomes,
with zero disconnect, reconnect, or observed usable gap.
The maximum command outcome was 326 milliseconds in the full baseline seconds
and 892 milliseconds during movement. Gate B then moved exactly nine
partitions in each direction through forced `1 -> 10 -> 1`. All 1,280
continuity sessions and 640 games completed, and all 1,144,974 submitted
commands received terminal outcomes. All 114 planned game handoffs succeeded
with zero observed usable gap; their total preparation time was at most 1,820
milliseconds while the old socket remained usable.
Scale-out and scale-in command outcomes remained below one second, peaking at
429 and 756 milliseconds respectively. The idle, lobby, and unmatched
matchmaking cohorts also survived intact. Serverless Valkey reported no
throttling or eviction in the executed phases.

The sole observed assertion failure before the script stopped was not a
failover delay. All 480 open-loop admission sessions completed without an
error or reconnect, with 311-millisecond p99 and 1,672-millisecond maximum
readiness; peak in-flight admission was 12 sessions, below the unchanged
64-session safety ceiling. One four-session wave began 1,394 milliseconds after
the prior wave instead of within the unchanged 1,100-millisecond
generator-cadence allowance.
The load coordinator awaited its five-second infrastructure HTTP sample inline
at the coincident 15-second launch tick, then `MissedTickBehavior::Delay`
shifted every subsequent timer deadline. Gate C, automatic target-tracking
scale-in, final metrics assertions, and the hard-crash suite therefore did not
run and remain mandatory. This run also observed transient per-task CPU skew
after natural scale-out (about 82--89% on the successor versus 36--41% on the
incumbent despite five leases each); it caused no correctness failure but must
remain visible in the final per-task evidence.

The minimum test-tool correction runs at most one infrastructure sample
concurrently, keeps the one-second launch timer on its original fixed-rate
anchor, and prioritizes a coincident launch after refreshing terminal-session
bookkeeping. It does not alter any service code, cohort, cadence allowance,
latency budget, handoff criterion, or safety ceiling. Crash certification now
also proves zero WebSockets, zero authoritative games, and drained executor
queues at its independent one-task start, and the workflow retains both suite
statuses so a returned planned-suite failure cannot suppress hard-crash
evidence.

Cleanup for run `30089020521` succeeded and an independent absence audit found
no active development stack, compute, cache, database, ingress, DNS, logging,
or scaling resource. The imported production VPC, every production stack
timestamp, and both production services remained unchanged and healthy.

Exact-source run
([GitHub Actions 30425964773](https://github.com/lopatin/snaketron-io/actions/runs/30425964773),
outer commit `fe4652b72aa82c39f9773b3beaec412f01cc8308`, Snaketron
commit `c179d966c0aac6c908e0d5ac8999f7bfadcc503a`) disproved the prior
working assumption that only a sub-two-second failover exception remained.
Gate A naturally scaled `1 -> 2`; the successor recovered 51 active games and
replayed 380 commands. All 2,790 sessions, 1,395 games, and 2,450,883 submitted
commands completed with terminal outcomes, with zero disconnect, reconnect, or
usable-session gap. No application warning/error, Serverless Valkey throttle or
eviction, or DynamoDB throttle occurred.

The run nevertheless failed the unchanged ordinary-operation command gate.
Of 330 complete pre-movement seconds, 141 failed and the maximum terminal
outcome latency was 3,617 milliseconds; 48 seconds exceeded two seconds.
Movement had 29 failing seconds out of 46 and a 2,715-millisecond maximum.
Every failed second still had exact submitted-to-terminal-outcome accounting,
so this is sustained processing delay rather than loss or duplicate execution.
The initial task remained near 95--100% CPU for about five minutes and the
two-task service remained about 79--84% in aggregate. The old report ended its
strict Gate A window when movement finished; replaying the full post-ready
interval also finds seven later seconds above one second in this run and 39 in
run `30089020521`. Gate A therefore now covers every complete sent-time second
from post-movement readiness through the load stage's recorded finish as a
separate required section and requires at least 60 complete post-ready seconds.

The same serving image and fixed cohort had passed the earlier movement window.
The differentiating evidence is higher Serverless service latency amplified by
full JSON recovery checkpoints: at steady load, each of 112 games wrote a
roughly 242--244 KiB envelope every second, including up to 512 exact outcomes
per client session. The minimum remediation is the aligned 128-entry
client/server bound in R5. Disconnected or resynchronizing browsers create no
new identities; at the certified ten-command-per-second profile, the bound also
permits 12.8 seconds of continued submission without outcomes. The unbounded
contiguous deduplication watermark fences pruned contiguous identities. This
reduces the dominant checkpoint payload by approximately four times without
changing checkpoint cadence, persistence format, CPU/memory targets, fixed
cohort, or any acceptance deadline.

The planned suite stopped at Gate A. The separate crash invocation also did not
inject SIGKILL because a residual target-tracking action changed the service
from one to two tasks before its strict entry check. Crash certification now
suspends target tracking first, normalizes and verifies an independent
`desired/running/pending = 1/1/0` baseline, and restores the canonical enabled
`1/1/0` state in cleanup. This is test isolation only; production crash
recovery remains independent of SIGTERM. The pending-completion backlog alarm
uses the per-minute maximum because every task emits the environment aggregate
while only the elected reporter has the real value; a minimum would let
non-reporting zeroes hide a real backlog after scale-out.

Cleanup and the independent absence audit passed. No development resource
remained; all nine production stack timestamps and resource identities were
unchanged, and production remained healthy. This run is diagnostic, not
release certification. A fresh exact-source planned run and separate SIGKILL
run with all unchanged acceptance thresholds are still required.

The next exact-source run
([GitHub Actions 30444237957](https://github.com/lopatin/snaketron-io/actions/runs/30444237957),
outer commit `e1dfb875633f80d9528e19ea1c931e0f72ec8bc7`, Snaketron
commit `e70d1185c4cba2aeb3eb44867b28357399afabf1`) proved that the
224-session Gate A no longer represented a valid one-task headroom test. The
one-vCPU task remained at approximately 100% CPU for six complete minutes
before target tracking added a second task. All 2,846 sessions, 1,423 games,
and 2,516,545 commands completed with exact terminal accounting and zero
disconnects, reconnects, or usable-session gaps, but 43 complete baseline
seconds exceeded the one-second command budget and the maximum was 3,233
milliseconds. Thirteen baseline seconds also exceeded the separately discussed
two-second tolerance. Movement peaked at 1,739 milliseconds and post-ready
delivery at 1,689 milliseconds. The run therefore remains a failure rather
than being reclassified as the accepted sub-two-second transition exception.

After scale-out, Container Insights placed the original task at approximately
823--877 CPU units and the successor at 405--433, while an idle task used about
7.5% CPU. This makes 144 sessions / 72 duels the smallest evidence-backed fixed
trial expected to keep one-task CPU above the 60% target but below saturation;
it also remains above Gate B's 128 command-bearing sessions. The projection is
not treated as a pass: failure to trigger naturally or any command-budget
violation still fails certification. This decision supersedes the historical
one-time freeze at 224, retains the one-second hard budget, 20-minute stage,
four-session-per-second ramp, CPU/memory targets, and every safety invariant,
and adds no production mechanism or paid Serverless Valkey minimum.

The crash invocation reached ten healthy ECS, Traefik, and executor members but
stopped before SIGKILL because four newly started ECS Exec managed agents were
still `PENDING` at a one-shot setup assertion. Certification now polls the exact
already-verified task cohort for at most 120 seconds and still executes the
actual kill exactly once. Runtime cleanup succeeded: the Server, Serverless
Valkey, and Monitoring stacks were absent, while the protected development
Network/EIP/EBS/TLS, ECS, ECR, and DynamoDB foundations remained reusable and
the ingress instance was stopped.

Exact-source run
([GitHub Actions 30454722583](https://github.com/lopatin/snaketron-io/actions/runs/30454722583),
outer commit `8f476b972b000e99d0b4da5de7705f659dcfac6e`, Snaketron
commit `eaba0de0dc89abe6fa00fef13c8cd903b06658e0`) separated the
autoscaling transition from one-task capacity pressure. The automatic movement
passed all 48 complete seconds with a 368-millisecond maximum, exact outcomes,
all ten partitions productive, and zero disconnects or reconnects. Before the
action, however, the 144-session origin remained at 87.61--90.43% CPU for four
complete minutes. Four of 296 baseline seconds exceeded one second, including
2,677- and 2,361-millisecond seconds; one later settled second reached 1,271
milliseconds. The user-approved sub-two-second transition allowance does not
apply to these ordinary-operation failures. Gate A correctly blocked Gates B
and C.

The production policy now scales at the measured safe one-task plateau rather
than weakening the latency SLO. Historical exact-source run `30046381977`
held 128 sessions / 64 duels for eight minutes at 45.18--48.83% CPU with a
488-millisecond maximum, exact command accounting, and no disconnect,
reconnect, or checkpoint failure. The shared development/production CPU target
is therefore 40%, and Gate A uses that fixed 128-session cohort. This leaves
the weakest historical minute 5.18 points above the trigger while retaining
`minTasks=1`, one-vCPU tasks, the 80% memory target, the 60-second cooldowns,
and every one-second correctness gate. The historical run predates material
server changes, so this is an evidence-backed headroom correction requiring
fresh exact-source certification, not a pass.

The separate crash run completed all 1,376 sessions and 688 games without a
failure; 22 affected clients recovered through different tasks with fresh
snapshots in 2,506--2,706 milliseconds, and ECS recorded the selected container
exiting 137. It is not formal crash certification because killing the essential
process tore down ECS Exec before its post-kill marker arrived, after which the
harness waited for Session Manager's 20-minute timeout. The single injection now
SIGKILLs the exact server PID directly while a read-only control-plane observer
brackets each selected-partition observation with start and completion times.
Certification disables AWS CLI retries for the one mutating call, anchors the
bounded observations to ECS `executionStoppedAt`, requires exact exit 137, and
rejects explicit OOM/unhealthy reasons rather than relying on stdout that is
inherently lost during container teardown. Those deadline comparisons assume
that the certification runner and ECS UTC clocks are NTP-synchronized.
Runner-side observations use conservative monotonic-bracketed wall-clock
intervals; Valkey timestamps are retained only for same-Valkey correlation and
are not compared with ECS time. No production crash endpoint, distributed
clock-calibration subsystem, or additional external action is added.

The run reused the exact protected Network stack, ingress instance, root EBS,
EIP, hostname, and certificate, with no development DNS/ACME change. Cleanup
again removed only the runtime Server, Serverless Valkey, and Monitoring stacks
and stopped the retained ingress.

Exact-source run
([GitHub Actions 30475852468](https://github.com/lopatin/snaketron-io/actions/runs/30475852468),
outer commit `be4040cafd8ddce72be51eab63ff4de158994e55`, Snaketron
commit `2c3f6c06a725b585c6f161aa7a32cdf98938b547`) naturally scaled
Gate A `1 -> 3`. All 1,664 sessions, 832 games, and 1,476,683 commands
completed with exact outcomes and zero reconnects, but the unchanged
one-second gate failed 22 of 332 baseline seconds, two of 46 movement seconds,
and 17 later settled seconds; the maximum was 5,852 milliseconds. Twenty-nine
ordinary failures clustered at seconds `:01` through `:03` while the elected
reporter's serial full recovery scan took as long as 4,719 milliseconds.
Partition 3 also failed closed once, then recovered six games and 124 commands
without loss. Gate A correctly blocked the planned staircase and capacity gate.

The minimum correction keeps exact all-game gauges while replacing serial
per-game reads with fixed 32-key same-slot metadata batches under a
two-second whole-collection bound. A nonzero
`RegionalCollectionFailures` metric makes a timeout fail certification rather
than emit a misleading healthy zero. Pending-completion index scans and bounded
approximate trim use one separate task-wide maintenance dispatcher. Separate
at-most-one background completion and trim workers ensure neither Valkey
maintenance nor DynamoDB effects pause authoritative command ingestion. No
authority deadline, checkpoint cadence, load envelope, or scaling policy was
changed.

The separate exit-137 load completed 788 sessions, 394 games, and 705,108
commands before proof generation stopped. Twenty-six affected sessions
recovered with fresh snapshots in 2,311--2,518 milliseconds from detection;
27 balanced attempts include one retryable replacement. Successor assignment,
new fenced lease, and all 53 sampled pending entries were present within
3.88 seconds of exact ECS stop. The rich PEL/status observation itself completed
just outside five seconds, so certification now keeps it for the two-second PEL
proof and uses a prestarted lightweight coherent ownership observer for the
unchanged five-second gate. This changes proof cost, not the product deadline
or production coordination path.

Cleanup removed only Server, Serverless Valkey, and Monitoring, stopped the
same ingress instance, and retained the fixed hostname/DNS, EIP,
certificate-bearing volume, shared production VPC, and all reusable
development foundations. This run remains diagnostic evidence. Complete
Gate A/B/C, automatic scale-in, and exit-137 certification are still required.

Exact-source run
([GitHub Actions 30486700133](https://github.com/lopatin/snaketron-io/actions/runs/30486700133),
outer commit `d2ba3692b4841f6ea0ba1a929c164239c3bc0b8b`, Snaketron
commit `9d176c3be3a4510cfd86f583ca5e2aa915312125`) naturally scaled
Gate A `1 -> 2`. All 1,664
sessions, 832 games, and 1,488,138 commands completed with exact terminal
accounting and zero disconnects or reconnects. All 50 ownership-movement
seconds passed the unchanged one-second gate with a 608-millisecond maximum,
and all 789 post-ready seconds passed with a 461-millisecond maximum. The
one-task baseline still failed five of 323 complete seconds, however, with
maxima from 1,701 to 2,531 milliseconds. Gates B and C therefore correctly did
not run.

The evidence is most consistent with insufficient one-task compute headroom,
not an ownership or cache failure. The one-vCPU task averaged 70.91--84.87%
CPU and reached 90.04%;
memory remained below 9%. Load began at 20:24:42 UTC, but delayed one-minute
samples kept the managed target-tracking alarm from entering `ALARM` until
20:30:42, so one task carried the full 128-session / 64-game envelope for six
minutes. During the final cluster of failures, scheduled receipts fell across
all partitions and then drained in the next second. Valkey and DynamoDB had
zero throttling, Valkey had zero evictions, and there were no lease, fencing,
checkpoint, completion, or executor errors.

The exact regional metrics scan was correlated with some failures but is not a
sufficient explanation: it contributes about 0.07% of observed Valkey command
volume, uses a separate connection, and normally completes in about 100
milliseconds. Its 500-millisecond telemetry deadline was nevertheless too
tight for three otherwise successful 512--545-millisecond samples, so the
deadline now has two seconds of operational margin while
`RegionalCollectionFailures == 0` remains release-blocking. The ownership
outage tracker conservatively accounts for the actual gap between successful
samples, including time spent in that scan. INFO volume also did not track the
failed seconds. Neither metrics concurrency nor log suppression is added.

The minimum evidence-backed capacity correction is a two-vCPU / four-GiB
Fargate task with `minTasks=1`. It supplies a second runtime worker and
headroom during the managed alarm delay without changing game execution,
checkpointing, connection topology, or the one-second gate. Diagnostic run
`30496453531` tested outer commit
`9c729f8f075a556d3caee6e2a37302f8da0981e6` and nested commit
`a424647639b156abfc8d0731b3fbf51ecbded634`. Its configured 35% managed 3/3
CPU alarm never fired: Gate A timed out, so planned Gates B and C did not run.
The nine consecutive load periods measured 30.33--35.36%. A 30% target would
have supplied seven valid three-period alarm windows, while whole-percent
targets from 31% through 35% would have supplied at most one. The CPU target is
therefore 30%. A fresh unchanged Gate A must still prove that it produces a
CPU-only natural scale-out. Memory remains 80% and both cooldowns remain 60
seconds.

The interrupted Gate A stimulus still supplied clean diagnostic one-task
capacity evidence: all 768 sessions, 384 games, and 687,693 command outcomes
completed exactly, with zero failed sessions and zero evaluated command
seconds above one second. The worst per-second command-outcome latency was 914
milliseconds. This is not formal Gate A or capacity acceptance because the
stage plan did not complete; it isolates the observed failure to the 35%
managed scaling trigger rather than workload instability or command loss.

The independent exit-137 run demonstrated clean product recovery before its
proof harness returned early. ECS recorded one selected container exiting 137.
The successor acquired a new fenced partition-6 lease on a pre-existing task
and completed bootstrap 3.72 seconds after exact `executionStoppedAt`,
recovering 15 games and replaying 151 commands. All 28 affected clients
reconnected through fresh snapshots in 2.512--2.658 seconds from exact stop;
all 954 sessions, 477 games, and 855,103 terminal command outcomes completed,
and 138 sessions admitted after the stop also completed. Traefik retained at
least one healthy backend throughout.

Formal crash acceptance stopped on an observation artifact. The first rich
post-stop status call began at +168 milliseconds and returned 151 exact
old-consumer pending entries, but its fully bracketed multi-read interval ended
at +2.300 seconds and therefore could not prove the unchanged two-second PEL
criterion. The harness now uses one narrow partition-local atomic `TIME` /
consumer-filtered `XPENDING` probe and retains the full-interval two-second
bound.
The prior report's first complete post-stop receipt bucket gave a 4.2-second
upper bound, but that measurement could admit an event buffered before the
crash and therefore is not accepted as causal successor proof. The corrected
harness atomically reads the new assignment, fenced lease, and exact
event-stream tail from the same partition slot, then immediately saves the
first later non-replay `CommandScheduledV2` stream entry using a bounded,
paged read. This avoids both an unbounded diagnostic query and loss of valid
evidence to approximate event-stream retention while ECS exposes the exact
stop timestamp. The saved ownership/output pair is accepted only when its
replacement assignment was computed after that exact stop and its Valkey
timestamps correlate to the unchanged five-second window. Tasks already
stopping when crash evidence begins are also excluded from the unrelated-stop
check. These are proof corrections only; no production recovery mechanism or
deadline changes.

The independent crash phase of run `30496453531` then formally passed all 12
hard-crash checks. Its one selected ECS task exited 137. The narrow PEL
observation completed 1.161 seconds after exact `executionStoppedAt`; the
survivor's fenced owner-ready sample completed at 4.022 seconds and its first
causal authoritative output appeared at 4.006 seconds. All 32 affected clients
recovered with a formal 4.314-second upper bound. All 1,378 sessions, 689
games, and 1,234,049 command outcomes completed with exact accounting and no
pending commands. Traefik recorded zero zero-backend samples or scrape errors
across 308 observations, and ECS recorded no unrelated task stop.

Workflow cleanup succeeded. It removed the Server, Serverless Valkey, and
Monitoring runtime stacks, stopped ingress, and retained the reusable
hostname, DNS, certificate-bearing EBS volume, EIP, shared production VPC, and
Network/ECS/ECR/DynamoDB foundations. No certificate was issued or recreated.
This remains diagnostic evidence; fresh complete Gate A/B/C, automatic
scale-in, and exit-137 acceptance are required.

Exact-source run
([GitHub Actions 30503270454](https://github.com/lopatin/snaketron-io/actions/runs/30503270454),
outer commit `d31d64f93288aa246bb2038ec11b1adf401d05fc`, Snaketron
commit `463a164bb66bb3e187c4681274bb6edb28ba6e88`) proved the fixed
30% CPU target and completed the three load gates. Gate A naturally scaled
`1 -> 2`; all 1,664 sessions and 1,477,620 commands completed exactly, with no
failed seconds across 374 baseline, 44 ownership-movement, and 747 post-ready
seconds. Gate B moved nine partitions in each direction. All 1,280 game
sessions and 480 open-loop admissions completed, and all 117 planned handoffs
had zero usable-session gap and no reconnect. Gate C completed all 1,748
sessions and 874 games and produced a final 403-second qualifying streak
against the unchanged 300-second capacity requirement. Three earlier seconds
were nonqualifying at 1,111 milliseconds, 1,006 milliseconds, and 127 fully
joined duels; they were outside the final continuous qualifying streak and are
retained rather than hidden.

The run is not complete certification. After load reached zero, genuine target
tracking successfully reduced the service from ten tasks to two in eight
activities, approximately one task every two minutes. The harness's
20-minute observation ceiling expired while the final successful activity's
configured 60-second cooldown still prevented a `2 -> 1` action. Both
certification-only Valkey SSM tunnels then reached their inactivity timeout,
so the independent crash suite could not read its initial control-plane
snapshot and performed no crash injection.

The harness now allows up to 40 minutes for this AWS observation and returns
immediately when desired, running, and pending counts reach `1 / 1 / 0`. That
ceiling covers the managed low-alarm window, bucket alignment, eight subsequent
cooldown/evaluation cycles, and final ECS convergence; it is not a product
scale-in SLO. A read-only control-plane query once per minute, together with
explicit connection checks and the cluster client's bootstrap and PING of both
advertised Serverless Valkey ports, keeps the existing SSM sessions active and
fails closed if the control path disappears. No production policy, authority
path, lease, target, cooldown, command budget, or WebSocket requirement
changed.

Cleanup for `30503270454` again removed only Server, Serverless Valkey, and
Monitoring and stopped ingress. It retained the same protected Network stack,
instance, EIP, DNS record, certificate-bearing EBS volume, shared VPC, and
ECS/ECR/DynamoDB foundations. No deployment-time Network update or certificate
creation was observed. Fresh complete Gate A/B/C, automatic scale-in, and
exit-137 acceptance remain required on one exact source.

Exact-source diagnostic run
([GitHub Actions 30513482816](https://github.com/lopatin/snaketron-io/actions/runs/30513482816),
outer commit `6d5e293afe9f42bb1530e5feb2a4bff1f9bae3b9`, Snaketron
commit `554d4564efb1b9b946260a33b39e4e5f819aa340`) recorded the
successful target-tracking action inside the fixed 480-second Gate A decision
window, but the added Fargate task became ready 11 seconds after that window.
The harness now separates the managed scaling decision from subsequent cold
task readiness without changing either requirement. Its independent hard
crash phase formally passed all 12 checks: 23 affected game clients recovered,
the successor owner and first causal output appeared in 4.009 and 4.060
seconds, and all 1,380 sessions, 690 games, and 1,235,425 command outcomes
completed exactly.

The next exact-source diagnostic
([GitHub Actions 30516147896](https://github.com/lopatin/snaketron-io/actions/runs/30516147896),
outer commit `35e88366fefa818c9dfcd44e34351d8ce294fb4c`, Snaketron
commit `fb59d91654016fb3354fbbcc07b64bd65b467b52`) exercised that
corrected decision/readiness split, but the unchanged fixed workload remained
at 18.21--22.13% one-minute CPU instead of the prior identical image's
approximately 28.5--35.2%. Command acknowledgement throughput differed by
only 0.12%. A 30% target is therefore placement-sensitive; 15% remains above
the approximately 2% idle level and below the weakest loaded minute with
useful margin. The fixed workload, 480-second decision gate, memory target,
cooldowns, task range, and one-second command gate remain unchanged.

The same run's task exit-137 recovered all 23 affected active-game clients in
2.542--2.738 seconds from client detection. The successor owner and first
causal authoritative output appeared about 4.1 seconds after exact ECS stop,
and all 1,232,737 submitted game commands had one terminal outcome with zero
pending. It also exposed a separate admission-boundary defect: one two-player
lobby sent `QueueForMatch` 162 milliseconds before the killed task stopped.
The host restored the exact lobby, but a successful socket write had been
mistaken for durable admission, so neither member was requeued and both timed
out. Clients now retain one in-memory intent until authoritative
`queued`/`matched` state or `JoinGame`, replaying it only after restored state
is still `waiting`; existing atomic admission makes the replay idempotent.

Cleanup for both diagnostics again deleted only the run-tagged Server,
Serverless Valkey, and Monitoring stacks and stopped the same ingress. The
VPC, Network/ECS/ECR/DynamoDB foundations, EIP, DNS, EBS volume, hostname, and
TLS certificate serial/fingerprint were identical throughout; no network or
certificate resource was recreated. Fresh complete Gate A/B/C, automatic
scale-in, and hard-crash acceptance are still required on one exact source.

Changing a timing value requires the same evidence again. It must not change a safety invariant or make graceful shutdown necessary for correctness.

## 19. Definition of done

This work is complete only when:

1. Every functional requirement is implemented or explicitly removed by an approved PRD change.
2. All safety invariants pass with zero violations in deterministic and chaos testing.
3. The independent natural scale-out, one-task-capacity-valid planned
   `1 -> 10 -> 1`, and ten-task capacity gates all pass at their fixed
   non-production envelopes.
4. SIGKILL at each command/checkpoint/finalization boundary recovers without acknowledged command loss or duplicate authoritative effect.
5. A paused stale executor cannot commit any fenced mutation after takeover.
6. Planned scale-up causes zero WebSocket reconnects.
7. Planned scale-down with another ready task produces zero usable-session gap for supported clients and does not wait for games to finish.
8. One non-production ECS task SIGKILL during the fixed ten-task 272-session
   crash envelope proves that hard crashes reconnect and recover automatically
   within the validated service targets when a survivor exists.
9. The documented `minTasks=1`, regional Serverless Valkey, ingress, and retention limitations are visible in operational runbooks.
10. Readiness, liveness, assignment, fencing, pending commands, checkpoints, recovery, WebSocket drain, and idempotent effects are observable and alerted.
11. The superseded Raft high-availability document is marked superseded by this PRD.
