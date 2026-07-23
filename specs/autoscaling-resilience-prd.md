# PRD: Seamless ECS Autoscaling and Crash Recovery

| Field | Value |
| --- | --- |
| Status | Direct-only implementation acceptance draft |
| Product | Snaketron regional game service |
| Owners | Engineering / Product |
| Last updated | 2026-07-22 |
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

This PRD deliberately does not introduce a separate gateway service, a new consensus system, whole-partition snapshots, a generic WebSocket event log, a self-managed cache cluster, or custom autoscaling signals. ElastiCache Serverless itself is TLS-only and cluster-mode-only; the application must therefore be Redis-Cluster-aware.

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
- The **minimum certification load envelope** is 256 authenticated game sessions, 128 concurrent duel games, four new sessions per second, and `every-tick` commands across ten partitions, held for at least five minutes. The harness may run a small game/session buffer above this floor so ordinary completion churn cannot turn one brief peak into false hold evidence; it must report both the configured target and the per-second minimum. Mixed idle/lobby/matchmaking probes and make-before-break candidate sockets are additional traffic. The report also records checkpoint write rate/size, pending backlog, per-task sockets, and Valkey/ingress saturation. Raising the required minimum requires another certification run.
- A command is **semantically acknowledged** when the client receives its matching terminal `CommandScheduledV2`, `CommandRejected`, or resolved recovery outcome. Gateway `XADD` success alone is not a player-visible acknowledgement.

## 5. Goals

1. Make crash recovery authoritative: SIGTERM accelerates recovery but is never required for it.
2. Rebalance all ten executor partitions automatically on task arrival and departure.
3. Prevent any stale executor from committing authoritative output after its lease term ends.
4. Make executor commands durable across periods with no active executor.
5. Resume each active game from a recent per-game checkpoint without scanning all game keys.
6. Preserve logical exactly-once command effects over at-least-once delivery.
7. Preserve gateway/executor independence and avoid reconnecting sockets for executor-only movement.
8. Provide zero-usable-session-gap WebSocket handoff on planned task removal for supported clients.
9. Make matchmaking commits atomic and recoverable without Pub/Sub.
10. Route only to ready tasks and preserve the existing CPU/memory autoscaling policy.

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
4. A task may enter `ACTIVE` only after its local readiness conditions pass.
5. Only `ACTIVE` tasks are eligible for new desired assignments.
6. On SIGTERM, the task must become unready and mark itself `DRAINING` before releasing work.
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
4. With ten fixed partitions, use the direct balanced/minimal-movement allocator. Do not add or persist a consistent-hash ring for this release; it would not replace the explicit assignment contract or its balance and movement tests.
5. The coordinator itself must use a unique tokened lease. Assignment writes must compare the exact coordinator token and expected assignment version atomically.
6. Readers must observe either the complete old assignment or the complete new assignment, never a partial map.
7. If the coordinator is unavailable, the last assignment remains valid and existing partition authorities continue.
8. A task may acquire a free partition lease only if it is the desired owner in the current assignment.
9. An incumbent stops renewing when it is no longer the desired owner. Its existing unexpired lease token remains the active authority until it is compare-deleted or expires.
10. Assignment change alone must not invalidate a final fenced checkpoint. This desired-versus-active distinction is the cooperative handoff; no separate transfer state machine is required.
11. Every Valkey key must be constructed through `RedisKeys`. Cluster hash tags define distinct atomicity categories: regional membership, regional canonical assignment, global matchmaking, active-server metrics, and one separate family for each of the ten executor partitions. All keys in one Lua script, transaction, multi-key command, or pipeline batch must share one tag. Executor families must remain distinct so Serverless can distribute authoritative traffic across slots.
12. The canonical assignment document lives in the assignment slot. After a successful compare-and-set, the coordinator projects the complete document into each partition slot using a monotonic per-partition view. Lease acquire/renew reads that local view. A crash during projection may delay movement until reconciliation, but must never authorize two generations or roll a view backward.

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

### R5 — Command acknowledgement, idempotency, and resend

1. Use `CommandScheduledV2` as the positive semantic acknowledgement. Do not add a redundant gateway-level `CommandAccepted` event.
2. Add a terminal `CommandRejected` result containing the stable client command identity and reason.
3. Each player command must use the current `GameCommandV2` envelope with a stable identity scoped by game, authenticated user, client game-session ID, and monotonic session sequence. No legacy command envelope is accepted on the WebSocket path.
4. The browser must keep the command sequence and an in-memory pending outbox outside the WASM engine instance so snapshot-driven engine reconstruction does not reset command identity. Cap it at 512 unresolved commands per client game session; at the cap, do not create or send another command identity until an entry resolves.
5. Only one WebSocket generation may send game commands during a planned dual-socket overlap.
6. After reconnect, the client must wait for authentication, game rejoin, and fresh resolved-command state before resending unresolved commands in original order.
7. The server must deduplicate by stable client command identity before scheduling. A duplicate must return the recorded `CommandScheduledV2` or `CommandRejected` outcome. Repeated physical delivery must not repeat the logical game effect.
8. Dedupe state must contain:
   - the highest contiguous terminally resolved sequence for each client game session;
   - a bounded sparse result map above that point when outcomes have gaps or arrive out of order.
9. A rejected or never-received sequence must not be represented as accepted merely because a higher sequence resolved. The contiguous watermark never advances across an unresolved gap.
10. Retain at most 512 exact outcomes per client game session and at most 64 client game sessions per game. The contiguous watermark remains in every recovery checkpoint for the checkpoint lifetime.
11. Recovery checkpoints must contain the resolved watermark and sparse command outcomes. After a recovery snapshot, the server sends that user's `CommandOutcomes` records followed by an explicit `CommandOutcomesComplete` barrier before the client may resend unresolved commands.
12. A client may remove an outbox entry only after matching `CommandScheduledV2`, matching `CommandRejected`, matching `CommandOutcomes`, or an authoritative terminal game state (`Complete` or definitive `GameLoadFailed`) proving no command can still execute.
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
4. Checkpoints must also be written at game creation, cooperative handoff, and completion.
5. Maintain a `partition -> active game IDs` index. Recovery must query this index and fetch checkpoints in a pipeline/batch rather than scan all `game:snapshot:*` keys.
6. The authoritative recovery source is the versioned checkpoint. A local replica may accelerate recovery only if it carries the same cursor and dedupe metadata; an ordinary state-only replica must not override the checkpoint.
7. A successor must batch-load the partition decision journal under its new
   fence, attach entries to reclaimed commands by exact stream ID, and process
   pending/new commands in stream order. It skips commands already covered by
   the checkpoint; for an uncovered journaled command it restores the exact
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

### R7 — Planned partition handoff and task shutdown

1. Planned scale-up and scale-down must use the same crash-safe primitives as abrupt recovery.
2. When desired ownership changes, the incumbent must stop fetching new group work and stop renewing the lease.
3. While its current token is still valid, the incumbent must place a barrier into every active game loop. Each loop must process already queued commands, stop tick advancement, acknowledge quiescence, and perform no later authoritative mutation under that token.
4. After every game loop reaches the barrier, the incumbent must:
   1. write handoff checkpoints for active games;
   2. ACK entries covered by those checkpoints;
   3. compare-delete its partition lease.
5. If every loop does not reach the barrier before the handoff deadline, stop the old executor and fall back to normal lease-expiry recovery; do not extend shutdown or publish an unfenced partial handoff.
6. The successor may acquire only after the old lease is deleted or expires.
7. A crash at any handoff step must require no cleanup: the successor claims pending entries and resumes from the last successful checkpoint.
8. Partition transfer must not wait for a game to complete.
9. Partition ownership movement alone must not close or move a WebSocket.
10. Configure one 60-second ECS container stop timeout and one 45-second application drain deadline. Any change must preserve a safety margin and pass the planned-drain suite.
11. Executor handoff must start immediately after the task becomes unready and `DRAINING`, before the route-withdrawal wait and WebSocket drain notice. The task continues serving existing WebSockets while ownership and socket handoffs proceed.

### R8 — WebSocket recovery and planned make-before-break

1. Preserve the single-process gateway/executor deployment. A gateway can serve any game through regional Valkey streams and its local replicas.
2. Scale-up must not move existing WebSockets.
3. Hard-crash reconnect behavior must be:
   - an immediate first retry;
   - short jittered exponential backoff after failure;
   - automatic reauthentication, lobby/game restoration, and fresh snapshot;
   - no page reload or user action.
4. Use an explicit server `Authenticated` response containing task boot identity and the current required capabilities. The client must not mark a socket authenticated merely because it sent a token or a timeout elapsed. All advertised requirements are mandatory; there is no version negotiation or fallback mode.
5. Every socket must have a monotonically changing local generation. Callbacks from an older generation must not close, reconnect, overwrite state, or clear readiness for a newer socket.
6. Transport closure must be distinct from explicit `LeaveLobby`. Unexpected or planned transport loss stops that connection's heartbeat and lets its short presence lease expire; it must not immediately delete durable lobby or matchmaking state.
7. Cleanup from an old socket must compare its session/generation and must not erase presence created by the replacement socket.
8. Maintain durable `user -> active game / committed match` resolution. Pub/Sub `MatchFound` remains a best-effort hint.
9. For planned task drain:
   1. mark the task unready so Traefik begins route withdrawal while keeping old sockets open;
   2. during the bounded route-convergence window, finish in-flight and safe stateless HTTP work but reject every new WebSocket upgrade with a retryable `503`; supported clients retry without surfacing a terminal error;
   3. send one drain message containing task identity and deadline over every existing socket;
   4. the client opens a second socket through the same regional URL, not a server-specific URL;
   5. the second socket authenticates and restores lobby/game context;
   6. for a game, the second socket receives a current snapshot, resolved-command outcomes, and `CommandOutcomesComplete`, then buffers subsequent events;
   7. after the candidate is ready, the client sends a uniquely tagged application Ping on the old socket and receives its matching Pong; this proves temporal transport overlap only, not game-stream ordering;
   8. the client atomically switches command ownership to the new socket generation;
   9. only then does the client close the old socket. If the old transport closes or the shared deadline fires after the candidate is fully ready but before the Pong, retain and promote that candidate as crash recovery while recording a planned-handoff failure.
10. During overlap, only one socket sends player commands. Event delivery may overlap, but stable event revisions and socket-generation filtering must make duplicates harmless.
11. Use one drain message containing only the departing gateway task identity and deadline; it must never direct clients to an executor host because executor and gateway placement are independent.
12. Generic durable outbound WebSocket replay is not required; reauthentication, rejoin, and a fresh snapshot are sufficient.

### R9 — Crash-safe, concurrency-safe matchmaking

1. Keep matchmaking selection and scoring in Rust.
2. Admit a lobby through one atomic Valkey Lua operation that:
   - verifies the lobby metadata still exists and no lobby member or lobby is already matched;
   - creates one immutable queue identity for the lobby and one exact queued-lobby claim for each member;
   - rejects a conflicting lobby or user claim, so one user cannot be queued through two tabs or two lobbies;
   - inserts every queue and MMR-index member and sets the lobby state to `queued` in the same operation.
   A retry of the same physical request is idempotent. A later request while the lobby is already queued preserves the first admitted preferences; changing them requires cancel and requeue.
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
8. `GameCreated` carries the full initial state. The executor remains the only checkpoint writer: it creates the initial recovery checkpoint and active-game index entry before ACKing `GameCreated`.
9. `MatchFound` remains a hint. A connected lobby listener must subscribe first, then read the durable lobby-to-game mapping; every hint triggers another authoritative mapping read, and a five-second fallback reconciliation covers a missed hint or subscription reconnect. Deduplicate forwarded `JoinGame` messages by game ID. Disconnected recipients resolve the same durable mapping during authentication.
10. The atomic admission predicates and atomic eligibility/commit are the matchmaking fences. A separate matchmaking ownership epoch or general saga framework is not required; the one outbox is the narrow cross-slot bridge imposed by Redis Cluster.

### R10 — Truthful liveness, readiness, and routing

1. Expose separate endpoints:
   - `/health/live`: the process and HTTP runtime are functioning;
   - `/health/ready`: the task may receive new regional traffic.
2. Start unready.
3. Readiness requires:
   - the HTTP/WebSocket listener is bound;
   - a recent bounded Valkey operation succeeded;
   - all partition replica stream readers are subscribed and alive;
   - membership and assignment watchers are alive;
   - other critical local workers are alive;
   - the task is not draining.
4. Readiness must not require owning any executor partition.
5. A critical background worker exiting unexpectedly must fail the process so ECS restarts it. Do not add a general in-process supervisor.
6. A transient regional Valkey failure makes tasks unready but must not make ECS liveness fail and create a replacement storm.
7. A newly started task may be globally ready once its readers are alive. If a requested active game's replica is still cold, perform a bounded on-demand snapshot request/load and return a retryable warming response rather than delaying readiness for all users.
8. Traefik backend health must use `/health/ready`; ECS container health must use `/health/live`.
9. Keep `/api/health` as the lightweight client-side regional latency probe; it must not be the Traefik readiness signal.
10. Remove the Traefik sticky-session cookie. Affinity is not required and can route reconnects back toward a draining backend.
11. Keep automatic ECS discovery. Planned drain waits eight seconds after becoming unready before assuming new upgrade attempts have stopped reaching that backend; the server still rejects any late upgrade with retryable `503`.
12. Traefik active backend health uses a two-second interval and one-second timeout.
13. ECS provider discovery polls every five seconds. No node-event webhook or manual Traefik update is required.
14. ECS container liveness checks run every five seconds with a startup grace period.
15. New tasks become routable automatically after ECS discovery and readiness pass.
16. Traefik's `ping` endpoint must remain enabled because its container health command is `traefik healthcheck --ping`.

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
11. The partition-local fenced completion commit must durably publish one full terminal snapshot and one terminal status notification. Retrying after a timeout or crash must repair a missing notification without publishing duplicates during the completion grace period.
12. Durable effects must enforce their dependency order in the storage transaction: no XP, MMR, ranking, or high-score effect may commit before the completed-game record, and a ranking projection may not commit before its matching MMR effect.
13. A successor executor must be able to finalize a game created or previously executed by another task. Completion identity must be the durable game ID and immutable completion revision; it must not require the finalizing task's server ID to match the original executor.

### R12 — Autoscaling behavior and capacity constraints

1. Retain the existing target-tracking policies:
   - CPU target: 70%;
   - memory target: 80%;
   - 60-second scale-in and scale-out cooldowns.
2. Retain `minTasks=1` and allow ten tasks in both development and production so the release-blocking `1 -> 10 -> 1` staircase can run outside production. The cap remains aligned with the ten executor partitions. The staircase uses a fixed one-task-safe continuity cohort; it must not force the complete capacity envelope onto one task.
3. The autoscaler must never select zero desired tasks.
4. Validate `1 -> 10 -> 1` with active games, lobbies, matchmaking, idle sockets, continuously submitted commands, and four new admissions per second. Separately hold the 256-session/128-duel capacity envelope only after ten tasks are healthy and ready.
5. No custom game-specific autoscaling metric is added in this phase.
6. Every task currently replicates every partition, so task-local replica memory may not fall on scale-out. Scaling tests must prove memory behavior is acceptable; otherwise the replication model or memory policy needs a separate decision.
7. Existing WebSockets do not redistribute on scale-up, so service-average CPU can hide a hot gateway task. Record per-task CPU, memory, connections, and event-forwarding load during validation.
8. Do not increase the partition count or add adaptive splitting without load evidence.
9. Load tests must include Serverless Valkey read/write latency, ECPU, bytes, connections, network traffic, `ThrottledCmds`, and `Evictions`, plus the shared regional NAT/Traefik host's CPU, network, connection success, and admission latency/error evidence.
   Connection-tracking occupancy is an optional capacity diagnostic when the
   host exposes it; it is not an autoscaling-correctness or release gate.
   Redesigning those dependencies remains out of scope.

## 10. Logical Valkey data model

Exact suffixes are implementation details, but the brace-delimited hash-tag families are part of the Serverless compatibility contract. Keys outside an atomic multi-key operation may remain independently slotted.

| Logical record | Suggested shape | Purpose |
| --- | --- | --- |
| Task membership | Keys tagged `{snaketron:members:<region>}` | Detect active, warming, draining, and crashed tasks atomically. |
| Coordinator lease + canonical assignment | Keys tagged `{snaketron:assignment:<region>}` | Elect one writer and persist one explicit versioned owner map. |
| Per-partition assignment view | Key tagged `{snaketron:exec:<p>}` containing the complete canonical document/version | Let partition lease scripts verify desired ownership without a cross-slot read. |
| Partition lease, streams, active-game index, recovery and completion records | Keys tagged `{snaketron:exec:<p>}` | Keep every fenced executor transaction single-slot while spreading ten partitions across Serverless slots. |
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
2. It remains `WARMING` and unready until local readers and dependencies are ready.
3. It becomes `ACTIVE`; Traefik and the assignment coordinator discover it automatically.
4. The coordinator computes a minimally moved balanced desired map.
5. Incumbents for moved partitions stop renewing, checkpoint, ACK covered commands, and compare-delete their leases.
6. The new task acquires those leases, claims pending commands, restores checkpoints, catches up, and publishes fresh snapshots.
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
4. The client clears it on `CommandScheduledV2`, `CommandRejected`, matching `CommandOutcomes`, or an authoritative terminal game state.
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

The supported staging envelope is 256 authenticated sessions, 128 concurrent duel games, four new sessions per second, and the `every-tick` command profile across ten partitions, held at target for at least five minutes on ten verified tasks. The planned `1 -> 10 -> 1` transition uses a separate one-task-safe continuity cohort, including four new low-CPU admissions per second during scale-in; this phase separation is not a reduction of the capacity envelope. The five- and ten-second recovery objectives must pass inside the full envelope. Timing targets may be changed only by an explicit product decision; correctness properties may not be relaxed.

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

| Test | Pass criteria |
| --- | --- |
| Scale `1 -> 10` under the fixed one-task-safe continuity load while games receive commands | Exactly nine partitions move, owner counts become one each, no active WebSocket hard-reconnect occurs, every full transition second resolves exactly its submitted commands with no terminal outcome taking more than one second from original send, and fingerprints match. The real-browser planned-drain suite and staging protocol evidence jointly prove that no stale overlay occurs. |
| Scale `10 -> 1` under the same continuity load with games, lobbies, matchmaking, and idle clients distributed across the service | Exactly nine partitions move; every observed drain handoff has zero usable-session gap and one command owner; every full transition second resolves exactly its submitted commands with no terminal outcome taking more than one second; no active socket hard-reconnects; four newly started low-CPU sessions per second each reach a ready backend, with p99 initial WebSocket authentication within ten seconds and no terminal error; no game completion is awaited. |
| Kill after command `XADD`, before group delivery | Successor reads it as new work and applies one logical result. |
| Kill after delivery into pending, before schedule | `XAUTOCLAIM` recovers it and applies one logical result. |
| Kill after schedule, before checkpoint | Replay does not lose or double-apply the command. |
| Kill after checkpoint, before `CommandScheduledV2` publication | Replayed `CommandOutcomes` clears the outbox without reapplying the command. |
| Kill after `CommandScheduledV2`, before `XACK` | Successor reclaims and retires or replays as required, but one logical effect and no pre-reanchor duplicate incremental schedule reaches consumers. |
| Reject sequence N, accept N+1, and lose both terminal events | Reconnect does not treat the higher sequence as proof that N was accepted; resolved watermark/sparse outcomes clear each entry according to its own result. |
| Hold 512 client commands unresolved, then submit one more | The first 512 identities remain intact and resendable; the client does not allocate or send identity 513 until one entry resolves. |
| Pause owner A beyond its lease, let B acquire, then resume A | Every event, checkpoint, finalization, active-index mutation, and ACK from A is rejected. |
| Let stale consumer A read or claim after B acquires exclusive authority | A's atomically fenced read/claim is rejected without changing the PEL or last-delivered ID; B receives the exact entry in stream order and A cannot dispatch a committed mutation. |
| Crash coordinator during assignment write | Readers observe a complete old or new document; recovery reconciles monotonic versions. |
| Kill a task that owns both the coordinator lease and partitions | A survivor reacquires coordination, publishes a complete assignment, claims pending commands, and resumes authoritative output inside the crash objective. |
| SIGKILL one ECS task during the fixed non-production certification load while another task is ready | The task receives no graceful cleanup; its membership and leases expire; a survivor recovers its naturally observed pending backlog and resumes fresh authoritative output within five seconds; affected gateway sessions automatically authenticate, rejoin, and receive a fresh snapshot within ten seconds; commands have one logical outcome; and ECS restores healthy capacity. This is the only distinct external crash action required. |
| Change eligible membership `1 -> 4 -> 2 -> 10 -> 1` at 500 ms intervals while prior leases are still live | Within ten seconds of the final change, all ten partitions have one matching desired/live owner, assignment versions are monotonic, owner counts differ by at most one, and no stale assignment overwrites a newer one. |
| Recover RNG-dependent games, queued commands, and custom slow ticks | Recovery envelope fields restore the same logical fingerprint and wall-clock checkpoint cadence. |
| Recover with 10,000 unrelated snapshot keys in Valkey | Recovery reads only the indexed games for the acquired partition; unrelated key count does not change the fetched envelope count. |
| Leave a command pending beyond 8,192 later appends | Safe trimming retains and reclaims the pending command. |
| Fail checkpoint writes for nine seconds, then for eleven seconds, with the ten-second age budget | In the nine-second case commands remain pending and checkpointing recovers; in the eleven-second case the actor fails closed at the budget without falsely retiring work. |
| Inject failure at each WebSocket drain phase | Old socket remains usable until replacement auth, rejoin, snapshot, and switch complete; only one sends commands. |
| Close an old socket after a new socket restores the same lobby/game | Old cleanup does not remove new presence or active context. |
| Crash gateway during an ambiguous command send | Resend uses the same identity; outcome is one acceptance or one terminal rejection. |
| Admit new sessions continuously while a backend performs the configured eight-second route-withdrawal wait | Existing sockets migrate; late new upgrade attempts may receive retryable `503`, but every new session reaches a ready backend within ten seconds and surfaces no terminal user error. Provider/health settings and exact healthy-backend coverage corroborate capacity; no internal readiness-transition timestamp is required. |
| Repeat and concurrently submit one lobby admission, then submit two lobbies containing the same user | One immutable lobby identity and one per-user claim win; every queue/MMR index has one exact member; conflicting admission is rejected; cancellation or match commit removes every winning claim so no stale lobby can rematch a user. |
| Lose admission or cancellation responses, retry them, and interrupt the caller between durable queue mutation and presentation refresh | The atomic queue identity and lobby metadata state agree (`queued`, `waiting`, or `matched`); retries converge without a hidden queue member or stranded queued banner. |
| Concurrent matchmakers select the same lobbies | Exactly one atomic claim wins; no player or lobby belongs to two committed matches. |
| Kill matchmaker before/after the matchmaking commit, destination outbox delivery, and source acknowledgement, including loss of each response | Before commit, entrants remain queued. After commit, match/mappings/outbox exist. Any task delivers exactly one partition `GameCreated`; retries repair either half, and the executor creates the checkpoint before ACK. Disconnected recipients recover from mappings. |
| Commit immediately before a connected lobby listener subscribes, then drop or duplicate the Pub/Sub hint | Subscribe-then-read or the five-second reconciliation forwards the durable game ID once; duplicate hint/read overlap does not send a second `JoinGame`, and a later play-again game ID is still delivered. |
| Kill after the fenced Valkey completion commit and before each DynamoDB effect or its confirmation marker | A successor reloads the same immutable completion revision; completed game, XP, MMR, rankings, and high scores converge to one application per effect key, and pending completion state is retained until all effects are confirmed. |
| Time out the fenced completion commit after it may have executed, then retry it repeatedly | The exact same completion record is accepted; one terminal snapshot and one terminal status are observable, matchmaking cleanup converges, and no completion effect is duplicated. |
| Delay completion cleanup until a player or lobby is mapped to a newer game | The old active-match record is removed, but every newer user/lobby mapping remains unchanged. |
| Complete a game on a takeover executor with a different task/server identity | Final state and every durable effect commit once under the original game ID and one completion revision; original executor identity is not required. |
| Join an active game through a newly ready cold task | Snapshot warming succeeds inside the six-second authorization deadline or returns `GameWarming` with a 500 ms retry hint, never a false missing-game result. |
| Make Valkey unavailable through the deterministic local fault proxy | Readiness drops within seven seconds, liveness remains healthy, and restoration creates no conflicting authority. A remote ElastiCache outage is not a separate release test because availability during that accepted dependency outage is out of scope. |
| With recovery retention set to 60 seconds, crash the sole task and delay replacement 30 seconds | The documented availability gap occurs, then games recover automatically. |
| With recovery retention set to 60 seconds, delay sole-task replacement 61 seconds | The game returns the explicit unrecoverable outcome and no fabricated state. |
| Run the fixed 48-session `every-tick` continuity calibration from one task | CPU or memory target tracking produces a successful scale-out above one without a task exit, readiness failure, command backlog beyond the ten-second recovery budget, or manual desired-count update; failure to trigger is a failed certification, not permission to put the capacity envelope on one task. The earlier 64-session calibration was removed after live evidence showed it was not one-task-safe. |
| Hold 256 authenticated sessions / 128 duels at four new sessions per second with `every-tick` commands for at least five minutes | The run begins only after ten tasks are healthy in ECS and Traefik and settled in the executor control plane; every full hold second resolves exactly its submitted commands with no terminal outcome taking more than one second; Serverless Valkey reports zero `Evictions` and `ThrottledCmds`, no write failure occurs, and there is no zero-ready interval, ECS health failure, or Traefik health failure. |
| Run the complete protocol against actual ElastiCache Serverless | TLS certificate validation, RESP3, and cluster discovery through the advertised 6379 primary and 6380 read endpoints succeed, as do operations across every hash-slot family; loss-tolerant Pub/Sub uses a connection pool isolated from authoritative commands, and no subscription push confirmation is consumed as an ordinary command response; no `CROSSSLOT`, `MOVED` exhaustion, unsupported `KEYS`, or nonzero database error occurs; all Lua/multi-key key-family tests pass. A standalone local Valkey run alone is insufficient evidence. |
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
- Only after both pass, ramp by environment/region while watching recovery,
  pending, checkpoint, socket, and duplicate-effect metrics.

Exit gate: both non-production external results and all definition-of-done
criteria pass before the production ramp.

## 17. Component impact

| Area | Expected changes |
| --- | --- |
| Assignment module + `redis_keys.rs` | Membership, coordinator, canonical assignment, monotonic partition views, allocator, and explicit Cluster hash-slot families. |
| `server/src/redis_utils.rs` | One standalone/cluster-aware connection abstraction; TLS and Redis Cluster selection from the deployment URL. |
| `server/src/game_bus.rs` | Executor consumer-group reader, safe command retention, lease-aware single-slot scripts, versioned checkpoint APIs, idempotent outbox delivery, and separately retryable completion cleanup. |
| `server/src/game_executor_v2.rs` | Recovery envelope, dedupe, active-game index, backlog-first resume, cooperative checkpoint/release, idempotent finalization. |
| `server/src/game_server.rs` and `main.rs` | SIGTERM, lifecycle state, readiness state, critical-worker failure policy, one bounded drain deadline. |
| `server/src/matchmaking.rs` and manager | Atomic queue admission/cancellation/commit scripts, durable user-to-game mappings, and a bounded `GameCreated` outbox. |
| `server/src/ws_server.rs` | Explicit auth response, drain protocol, generation-safe cleanup, active-game resolution, retryable warming. |
| `client/web/contexts/WebSocketContext.tsx` | Immediate/backoff reconnect, socket generations, dual-socket drain, explicit auth, one command owner. |
| Client game integration | Stable session command IDs, external outbox, resolved watermark/sparse outcomes, terminal rejection. |
| `cdk/lib/valkey-stack.ts` and `fargate-stack.ts` | Serverless Valkey, TLS cluster URL, Serverless metrics/alarms, liveness/readiness routing, sticky-cookie removal, health timing, and stop timeout. |
| Ephemeral development infrastructure | Public run-unique stage reuses but never owns or mutates the production VPC; the run owns and destroys its cache, compute, security groups, ingress/EIP, and DNS. |
| Traefik configuration | Automatic health-based withdrawal/discovery and valid self-health endpoint. |
| Test runners | Deterministic failure points, scaling/load scenarios, stale-owner and pending-entry tests. |

## 18. Decisions and open tuning

### Locked decisions

- Crash recovery is the correctness path; SIGTERM is an optimization.
- Desired assignment is persisted explicitly; internal `hashring` state is not.
- Fixed ten-partition placement uses the direct balanced/minimal-movement allocator; no hash-ring dependency is required.
- Active authority is an exact, unique lease token.
- Consumer groups are executor-only.
- The fenced consumer-group executor is the only executor implementation.
- `CommandScheduledV2` is the positive semantic acknowledgement; `XACK` is still required internally.
- Checkpoints remain full and per game.
- Gateway and executor remain in the same binary/service but are logically independent.
- Planned task removal uses dual sockets; executor movement alone never moves sockets.
- Matchmaking safety comes from atomic admission/cancellation and one matchmaking-slot commit, plus one narrow idempotent outbox bridge into the executor partition slot; it does not require a singleton or generic saga system.
- Readiness and liveness are separate.
- CPU/memory autoscaling and `minTasks=1` remain.
- Regional Serverless Valkey and single-ingress availability risks are accepted for this phase.
- Serverless Valkey uses its fixed `volatile-lru` policy. CDK sets no data/ECPU usage maximum; any eviction or throttling fails certification and alarms in production.

### External release evidence still required

The deterministic suite is the release evidence for safety invariants that do
not require AWS: group-aware trimming retains and reclaims more than 8,192
pending commands and bounds the stream after ACK; checkpoint cadence is
wall-clock driven and checkpoint failure crosses the fail-closed age budget
without falsely retiring work; and real-browser Playwright covers every planned
handoff phase and the stale/disconnected UI.

Only these external results remain:

- the planned non-production `1 -> 10 -> 1` staging run passes the fixed load,
  continuous admission, exact healthy-backend, checkpoint-age, Valkey-capacity,
  and 45-second application shutdown gate while retaining the configured
  60-second ECS container stop-timeout safety margin; and
- during a separate run of the same fixed load envelope, one separately
  authorized non-production ECS task receives SIGKILL without graceful cleanup.
  Its naturally observed affected-partition backlog must meet the five-second
  authoritative-output objective, affected gateway sessions must meet the
  ten-second recovery objective, commands must retain one logical outcome, and
  ECS must restore healthy capacity.

No separate Fargate-host, remote-Valkey-outage, staging-browser rendering,
connection-tracking, synthetic maximum-backlog, or internal
local-readiness-to-route timing run is required. Those either duplicate the two
evidence paths above, test an accepted unavailable dependency, or add telemetry
without strengthening the user-visible guarantee.

Neither external result has a passing report attached. The first public planned-path attempt exposed fixed-node Valkey saturation and handoff defects and therefore does not count. The release remains blocked until both Serverless-backed runs pass.

Changing a timing value requires the same evidence again. It must not change a safety invariant or make graceful shutdown necessary for correctness.

## 19. Definition of done

This work is complete only when:

1. Every functional requirement is implemented or explicitly removed by an approved PRD change.
2. All safety invariants pass with zero violations in deterministic and chaos testing.
3. `1 -> 10 -> 1` passes at the fixed non-production certification load envelope.
4. SIGKILL at each command/checkpoint/finalization boundary recovers without acknowledged command loss or duplicate authoritative effect.
5. A paused stale executor cannot commit any fenced mutation after takeover.
6. Planned scale-up causes zero WebSocket reconnects.
7. Planned scale-down with another ready task produces zero usable-session gap for supported clients and does not wait for games to finish.
8. One non-production ECS task SIGKILL during the fixed load proves that hard
   crashes reconnect and recover automatically within the validated service
   targets when a survivor exists.
9. The documented `minTasks=1`, regional Serverless Valkey, ingress, and retention limitations are visible in operational runbooks.
10. Readiness, liveness, assignment, fencing, pending commands, checkpoints, recovery, WebSocket drain, and idempotent effects are observable and alerted.
11. The superseded Raft high-availability document is marked superseded by this PRD.
