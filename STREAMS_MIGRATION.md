# Valkey Streams Migration

**Status: COMPLETE — streams is the only transport.** The Pub/Sub game-bus
backend and the `SNAKETRON_BUS` flag were removed after streams proved out
(Phase 5 below); `server/src/game_bus.rs` (`GameBus`) is now a plain Streams
implementation, and Pub/Sub remains only for loss-tolerant fan-out (chat,
lobby updates, user counts) in `pubsub_manager.rs`. The pubsub-era
workarounds went with it — most notably the GameCreated ack/retry handshake,
which existed because Pub/Sub drops messages published before the executor
subscribes (under streams, a GameCreated missed during an executor restart
is recovered by the stored-snapshot resume path). Dedicated streams tests
cover zero-loss under a paused consumer, reconnect resume via CLIENT KILL,
ordering, tail anchoring, and trimming; measured delivery latency is p50
~1.3 ms / p99 ~3.6 ms — within ~0.2 ms of Pub/Sub and ~300x faster than the
abandoned 2025 implementation. The sections below are kept as the historical
record of the assessment, the design rules, and the rollout.

## Why the old Streams implementation was slow (git archaeology)

The 2025-07 Streams era (commits `61a5c31`…`67c0798`, replaced by Pub/Sub in
`0f7ee58`) was slow for reasons that are all fixable usage errors, not
properties of Streams. Ranked by measured impact:

1. **Ten blocking readers multiplexed on ONE connection.** All 10
   `PartitionReplica` loops issued `XREAD … BLOCK 100` on clones of a single
   `ConnectionManager` (one TCP socket). Redis parks the *connection* during a
   blocking read, so an event for partition P waited for up to nine other
   partitions' reads to time out first: ~400–500 ms expected, ~900 ms worst.
   This alone was 1–6 game ticks of lag and matches the "339ms+" measurement
   in the `0f7ee58` commit message.
2. **The publisher was queued behind its own parked reader.** The executor's
   `XADD`s shared a connection with its own `XREADGROUP … BLOCK 100` loop, so
   the first event of every tick burst waited out the BLOCK timeout *before
   even reaching Redis*.
3. **Per-WebSocket `XREAD` loops starting from ID `0`** over untrimmed
   streams (until `e077485`): unbounded catch-up work per join, and every
   client's blocking loop parked the shared WS connection that inputs were
   published on — a ~100 ms input-latency floor.
4. **100 ms BLOCK quantization + `COUNT 10` batches + one `XACK` round trip
   per message**, on a game whose tick was 100–300 ms.
5. **No trimming, ever** (`XTRIM`/`MAXLEN` appear nowhere in history), and
   consumer groups whose recovery half (XPENDING/XAUTOCLAIM) was never built
   — all the cost of groups, none of the benefit.

Used correctly — **a dedicated connection per blocking reader, `XREAD BLOCK`
as a push-like wait (it wakes on write, it is not a poll), start at `$`/last
delivered ID, `MAXLEN ~` trimming, no consumer groups** — Streams deliver in
~1 RTT, comparable to Pub/Sub's <5 ms, while keeping an ordered, replayable
log.

## What Streams solve, and what they don't

The current tree already detects and heals transport loss (stream_seq gap
detection → snapshot resync at replica and client; TickHash divergence
probes). Streams make most of that machinery *structurally unnecessary on the
server side* instead of load-bearing:

| Problem | Pub/Sub today | With Streams |
|---|---|---|
| Message lost on subscriber reconnect/blip | Lost; gap detected; snapshot resync | Not lost — reader resumes `XREAD` from last-delivered ID |
| Slow consumer (broadcast `Lagged`, mpsc full) | Messages dropped; loud logs; resync | Real backpressure — the reader simply reads later; nothing drops |
| Subscribe/join race | Subscribe-first ordering + watermark filter | Position-based reads; race-free by construction |
| Replica cold start with dead executor | Stored-snapshot fallback | Same, plus optional `XRANGE` catch-up from the snapshot's stream position |
| Client (WebSocket) hop loss | stream_seq gap → RequestResync | **Unchanged** — Streams end at the server; keep client resync |
| Engine nondeterminism / phantom deaths | TickHash probes, trace RCA | **Unchanged** — not a transport problem |

Code simplification is real but modest: the resync/self-heal paths remain as
defense-in-depth (and as *verification* — their counters should flatline on
Streams), but they stop being the thing correctness depends on. The bigger
win is operational: no more mystery loss class at all.

## Configurable backend vs rewrite: make it configurable

A rewrite is not warranted. The current architecture is already
message-passing with snapshot anchors, and the whole game-critical surface is
funneled through one API: `publish_event` / `publish_snapshot` /
`publish_command` / `request_partition_snapshots` /
`subscribe_to_partition → PartitionSubscription` (mpsc receivers). Both
backends can implement that seam; consumers (executor, replicas) don't change.

Two deliberate scope choices:

- **Only game-critical traffic migrates.** Chat, lobby updates, user counts,
  and matchmaking notifications are loss-tolerant fan-out — they stay on
  Pub/Sub under either setting. This keeps the migration surface small.
- **Dispatch by enum, not trait object.** Two variants behind a config flag;
  no `async_trait`/dyn issues:

```rust
// server/src/message_bus.rs
pub enum MessageBus {
    PubSub(PubSubBus),   // current PubSubManager partition paths, moved
    Streams(StreamsBus), // new
}
```

`stream_seq` stays in the message payload under both backends: it is the
end-to-end integrity check reaching the browser, the bridge across a backend
flip, and the metric that proves the Streams backend works (gap counters go
to zero).

## Streams backend design (the anti-sluggish rules)

Topology: three streams per partition —
`snaketron:stream:events:{p}`, `…:commands:{p}`, `…:snapreq:{p}`.
Same serde-JSON payloads as today in a single `data` field.

1. **Publishers** use the shared non-blocking `ConnectionManager`. `XADD`
   with approximate trimming: `MAXLEN ~ 8192` (events; several minutes of
   backlog at game rates), `~ 1024` (commands), `~ 64` (snapreq). Publishers
   never share a connection with a blocking reader.
2. **One reader task per partition subscription, with its own dedicated
   connection.** A single command watches all three streams:
   `XREAD BLOCK 5000 COUNT 512 STREAMS ev cmd req <id1> <id2> <id3>`.
   BLOCK is a liveness checkpoint, not a poll interval — Redis wakes the
   reader the moment any watched stream gets an entry. Sub-ms delivery.
3. **No consumer groups, no XACK.** Every consumer is fan-out (replicas) or a
   lease-guarded singleton (executor command intake); each tracks its own
   last-delivered ID in memory. On reader restart within a process: resume
   from last ID → zero loss. On process restart: start at `$` and rely on the
   existing snapshot anchoring (snapshots re-anchor watermarks at every hop —
   this machinery already exists and is tested).
4. **Backpressure instead of drops**: the reader forwards into the existing
   bounded mpsc; when full it `send().await`s (pausing reads) rather than
   `try_send`-and-drop.
5. `store_snapshot` / `get_stored_snapshot` (plain KV) are backend-independent
   and shared; executor failover/resume works identically under both.

Dependency: one Cargo feature flag (`redis = { …, features = [… "streams"] }`).

## Migration phases

**Phase 0 — dead-code cleanup (optional, 30 min).** Remove
`server/proto/stream_exchange.proto` (never referenced; pre-Redis scaffolding)
and the stubbed `game_relay.proto`/`grpc_server.rs` relay (every handler is a
`not yet implemented` warn, no live call site), plus their `build.rs` entries.
Avoids confusion between "gRPC streaming" and "Redis Streams".

**Phase 1 — extract the seam (mechanical, no behavior change).**
Create `MessageBus` enum; move the partition-scoped methods out of
`PubSubManager` into `PubSubBus` (delegating to the existing code);
`subscribe_to_channel` (chat/lobby/counters) stays where it is. Plumb
`SNAKETRON_BUS` (initially defaulting to `pubsub`; flipped to `streams` after validation) through `main.rs` → `game_server.rs` /
`replication.rs` (the only construction sites). Exit gate: entire existing
suite green with default config.

**Phase 2 — implement `StreamsBus`.**
Per the design above. Tests (real Valkey via test-deps, unique key prefixes to
avoid dev-server cross-talk):
- publish→consume round trip across all three streams;
- *the headline test*: pause the consumer mid-game, publish N events, resume —
  assert **zero** stream_seq gaps (this exact scenario loses messages on
  Pub/Sub and is why resync exists);
- trimming bounds respected under sustained publishing;
- reader reconnect resumes from last ID;
- latency smoke: p99 XADD→delivery < 10 ms at game-like rates (guards against
  ever re-introducing the old blocking-read mistakes).

**Phase 3 — wire consumers.** Executor and replication construct
subscriptions via the bus; they already consume `PartitionSubscription`, so
this is small. Run the chaos suite and full server suite under both configs
(CI matrix: `SNAKETRON_BUS=pubsub` and `=streams` for the server test job).

**Phase 4 — measure with the trace infra.** The flight recorders already
timestamp every publish (`EventOut`) and receipt (`EventIn`); run identical
bot games under each backend and compare delivery-latency distributions from
the traces (`trace_rca --json`). Acceptance: Streams p99 within ~2 ms of
Pub/Sub. This is the empirical answer to "are Streams fast enough now" —
measured on this game, not assumed.

**Phase 5 — rollout and (later) simplification.** Default stays `pubsub`;
flip staging to `streams`; watch the SyncStatus counters
(`stream_gap_count`, resync rate, `Lagged` warnings — all should go to ~0),
then flip prod. After confidence: consider relaxing the stored-snapshot
refresh cadence and demoting the gap-driven snapshot-request path to
cold-start only. Keep the Pub/Sub backend as the escape hatch until Streams
has weeks of clean counters; remove it only when the flag has become dead
weight. *(Done: the Pub/Sub backend, the `SNAKETRON_BUS` flag, the enum
dispatch, and the GameCreated ack handshake have all been removed —
`GameBus` is streams-only.)*

## Effort estimate

Phases 1–3 are one focused PR each (the seam extraction is mostly mechanical;
the Streams backend is ~300–500 lines plus tests). Phases 4–5 are
measurement and operations, not code. Total: a normal PR sequence on top of
the current branch — not a rewrite.
