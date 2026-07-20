# Debugging State Synchronization

This document is the runbook for diagnosing and fixing state-sync bugs — the
class of issue where the client's view of a game diverges from the server's
(phantom deaths, drifting snakes, ghost games that keep running after the
backend stopped). It covers the observability primitives built into the game,
the trace capture pipeline, and the workflow for turning an unreproducible
production report into a local failing test.

## The sync architecture in one paragraph

The authoritative game runs in a server-side executor
([game_executor.rs](server/src/game_executor.rs)) that advances a shared
`GameEngine` by wall clock and publishes every event over Redis pub/sub.
WebSocket servers hold replicas ([replication.rs](server/src/replication.rs))
that apply those events and forward them to clients. The client runs the same
engine compiled to WASM: its **committed** state advances only when server
events arrive; its **predicted** state (what the player sees) is rebuilt from
committed and free-runs ahead on the local clock, bounded by a prediction cap.
Player commands are scheduled optimistically at the client's predicted tick and
confirmed by the server, which may reschedule them to a later tick if they
arrive after the committed-lag window (500 ms) has passed.

Every hop in that pipeline is asynchronous and, before this infrastructure
existed, silently lossy. The primitives below make loss and divergence
*observable and self-healing* instead of silent.

## The four primitives

### 1. State fingerprints (`GameState::sync_hash`)

[common/src/fingerprint.rs](common/src/fingerprint.rs) — a deterministic
64-bit digest of everything gameplay-critical (snakes, food, scores, status,
tick). Two states with equal hashes at the same tick are gameplay-equivalent.
Excludes the RNG, in-flight command queue, and cosmetic fields, so it can be
compared across server (native) and client (WASM) builds.

### 2. TickHash heartbeat (`GameEvent::TickHash`)

Every `TICK_HASH_INTERVAL_TICKS` (10 ticks ≈ 1 s) the executor broadcasts its
committed hash + server timestamp. This one small message does three jobs:

- **Divergence detection** — the client recomputes its own committed hash at
  that tick and compares. Two consecutive mismatches ⇒ automatic resync.
- **Liveness** — it arrives even when nothing is happening in the game, so
  "no message for 3 s" now reliably means the stream is dead, and the client
  shows a reconnect banner instead of simulating a ghost game.
- **Clock reference** — `server_ts_ms` gives the client a continuous drift
  signal.

### 3. Transport sequencing (`GameEventMessage.stream_seq`)

The executor stamps every published message with a per-game, strictly
monotonic sequence number. Every consumer (replica, client engine) checks
contiguity: a gap means messages were lost (Redis pub/sub is at-most-once) and
triggers an automatic snapshot resync instead of silent divergence. Duplicates
and stale messages are skipped instead of double-applied. This replaces the
expensive "send a full snapshot every few seconds" workaround with resyncs
that happen only when something was actually lost.

Client-side health lives in `GameEngine::sync_status()` (gap counts, missed
messages, probe results, `needs_resync`) and is surfaced to the UI via
`GameClient.getSyncStatusJson()`.

### 4. Flight recorders (traces)

Both sides continuously record what their engine observed, in the shared
JSONL format of [common/src/trace.rs](common/src/trace.rs):

- **Server** — every game writes `traces/game_<id>_server_<ts>.jsonl`: the
  initial state *including the RNG seed* (the deterministic replay anchor),
  every command received, every message published, periodic fingerprints.
  Append-and-flush, so a crashed game still leaves a readable trace.
- **Client** — a ring buffer (last ~20k records) of every message received,
  every command sent, clock-sync samples, and anomaly notes. Downloadable via
  `window.snaketronDebug.downloadTrace()`, and auto-uploaded to
  `POST /api/debug/client-trace` the first time a desync is detected in a
  game.

A server trace + a client trace for the same game is a complete, replayable
record of the bug from both perspectives.

## The workflow: from prod report to local failing test

Someone reports "I died but I never hit the wall" or "the game froze but kept
playing". You cannot reproduce it locally. Do this:

1. **Get the traces.** Server: `traces/game_<id>_server_*.jsonl` on the
   executor host (or wherever `SNAKETRON_TRACE_DIR` points). Client: the
   auto-uploaded `game_<id>_client_<user>_*.jsonl` next to it — desyncs
   upload themselves; for other reports ask the player to run
   `window.snaketronDebug.downloadTrace()`.

2. **Run the analyzer.**

   ```bash
   cargo run --bin trace_rca -- game_42_server.jsonl game_42_client_7.jsonl
   ```

   It replays both traces deterministically through the real engine and
   cross-diffs them, printing a `DivergenceReport`:
   - the **first divergent tick** and the state difference at that tick,
   - **missing stream_seq ranges** (transport loss, and at which hop),
   - **command latency**: when each input left the client vs. when the server
     scheduled it, and whether it got rescheduled to a later tick,
   - **clock drift** over the session,
   - a **verdict**: `TRANSPORT_LOSS`, `ENGINE_NONDETERMINISM`, `CLOCK_DRIFT`,
     or `IN_SYNC`.

3. **Interpret the verdict.**
   - `TRANSPORT_LOSS` — messages were dropped before reaching the client.
     The client should have detected the gap and resynced; if it didn't,
     that's the bug. Look at which hop lost them (server trace has
     `EventOut`, client has `EventIn` — the diff names the missing ranges).
   - `ENGINE_NONDETERMINISM` — same inputs, different states: a real logic
     bug in the shared engine (native vs WASM, or event-application order).
     This is the highest-severity verdict; the replay gives you the exact
     first divergent tick to bisect.
   - `CLOCK_DRIFT` — the client's tick computation was off; commands were
     scheduled at the wrong ticks. Check the `Clock` samples.
   - Phantom-death reports specifically: check the command-latency section —
     a turn that left the client at predicted tick N but was scheduled by the
     server at N+k (because it arrived > 500 ms late) is the classic cause;
     the player saw the turn happen, the authoritative snake didn't turn.

4. **Freeze it as a test.**

   ```bash
   cargo run --bin trace_rca -- game_42_server.jsonl --emit-test server/tests/repro_game_42.rs
   ```

   This writes a test that replays the trace and asserts on the divergence —
   a permanent local reproduction of the prod bug. Fix the code until the
   test's expectation changes to "deterministic", then keep the test.

5. **Verify the fix against the whole class**, not just the instance: the
   chaos suite ([sync_equivalence_test.rs](server/tests/sync_equivalence_test.rs))
   drives a simulated client over a lossy, latent, jittery transport and
   asserts detection + resync. Add the newly discovered failure shape there.

## Monitoring signals worth alerting on

All of these are cheap counters already tracked in `SyncStatus` (client) and
logs (server):

| Signal | Meaning | Healthy |
|---|---|---|
| TickHash mismatch rate | real divergence happening in prod | ~0 |
| stream gap incidents / game | Redis pub/sub or broadcast loss | ~0 |
| resync requests / game | self-healing frequency (masking loss) | < 1 |
| commands rescheduled (server tick > client tick) | inputs arriving past the lag window | rare |
| watchdog activations | dead streams noticed by clients | ~0 |
| clock drift magnitude | client tick computation quality | < half a tick |

A rising resync rate with a flat mismatch rate means the transport is losing
messages but detection is working — degraded, not broken. Mismatches without
gaps mean engine nondeterminism — page someone.

## Root causes found and fixed (2026-07)

Fourteen distinct defects have been confirmed — the first thirteen by a
multi-agent audit (each verified by an independent 3-lens panel against the
code), the fourteenth from the enemy-snake respawn-desync report. The fixes
landed together with this infrastructure; the chaos suite and the replay
harness lock each one in:

| Defect | Fix |
|---|---|
| Events labeled with the pre-step tick, applied by clients one movement-step early — permanent body-geometry forks (e.g. snake grows a tick early). Found *empirically* by the replay harness's lossless-equivalence test | `run_until` labels events with the post-step tick ([game_engine.rs](common/src/game_engine.rs)) |
| One `broadcast Lagged` or one malformed payload permanently killed a partition's whole event/command feed (`?` in `ChannelReceiver::recv`) | Lagged/malformed are logged and skipped; only shutdown ends the task ([pubsub_manager.rs](server/src/pubsub_manager.rs)) |
| `FilteredEventReceiver` swallowed Lagged with a warning — clients silently lost N events forever | Lagged surfaces to the WS forwarder, which resyncs the client with a fresh watermarked snapshot ([replication.rs](server/src/replication.rs), [ws_server.rs](server/src/ws_server.rs)) |
| Replica caught up at most ONE tick per received event (`if` vs `while`) — every mid-game join snapshot was geometrically wrong | Loop until the event's tick ([replication.rs](server/src/replication.rs)) |
| Join race: state read before broadcast subscription — events in between were lost undetectably | Subscribe first; stream_seq watermark filter dedups the overlap ([replication.rs](server/src/replication.rs)) |
| No loss detection anywhere (engine `sequence` was duplicated/fabricated and unusable) | Executor-assigned `stream_seq` + gap detection at replica and client + auto-resync |
| Ghost games: prediction free-ran on wall clock forever; silent forward-loop exits; DB-fallback join with no live feed | Bounded prediction cap (engine), client liveness watchdog + reconnect overlay, loud logging + final snapshots on dead feeds |
| `clockSync.reset()` on every WS close snapped the time base to zero drift until 3 new Pongs | `reset()` carries the last drift estimate forward ([clockSync.ts](client/web/utils/clockSync.ts)) |
| One clock spike poisoned `last_command_tick` forever — all later commands scheduled in the far future (snake stops responding) | Ratchet bounded to predicted + 8 ticks ([game_engine.rs](common/src/game_engine.rs)) |
| `HashSet` iteration order made multi-death respawn processing nondeterministic across native/WASM | Deaths processed in sorted order ([game_state.rs](common/src/game_state.rs)) |
| Redis PING timeout `?`-exited the cluster singleton without stopping the service — zombie executor + split-brain duplicate games | Timeout takes the step-down path ([cluster_singleton.rs](server/src/cluster_singleton.rs)) |
| The web UI delivered every WS game event to the engine through a single-slot React state (`lastGameEvent`); React's last-write-wins batching silently dropped events whenever two frames landed in one commit. A crash tick is exactly such a burst (`SnakeDied` + `SnakeRespawned` + score updates as adjacent frames): with `SnakeRespawned` dropped, the delivered `SnakeDied` re-kills the snake the client's committed catch-up had already respawned locally, and nothing revives it until the stream-gap snapshot resync — the "enemy vanishes on crash, reappears mid-screen seconds later" report. Per-event full-state `console.log` serialization amplified the trigger by stalling the main thread | Game events now flow through a lossless ref-based FIFO drained strictly in order ([useGameWebSocket.ts](client/web/hooks/useGameWebSocket.ts), [GameArena.tsx](client/web/components/GameArena.tsx)); the state-serializing debug logging is gone ([useGameEngine.ts](client/web/hooks/useGameEngine.ts)); the failure shape (respawn lost, death delivered) is frozen in `lost_enemy_respawn_is_detected_and_healed_by_resync` ([sync_equivalence_test.rs](server/tests/sync_equivalence_test.rs)) |
| Executor lease loss cancelled every in-flight game permanently; a lost `GameCreated` message meant a game that never started | Full failover path: 3 s lease with a 60 % grace window for *transient* renewal errors (a Redis blip no longer cancels games — safety proof in `renew_error_should_step_down`), Lua own-lease reclaim after blips, stored snapshots refreshed at TickHash cadence, and `resume_partition_games` on executor (re)start restarts every non-complete game from replica or stored-snapshot state. Snapshots re-anchor `stream_seq` watermarks at every hop (`FilteredEventReceiver`, replica, client), so a restarted executor's new stream is adopted instead of filtered as stale ([game_executor.rs](server/src/game_executor.rs), [cluster_singleton.rs](server/src/cluster_singleton.rs), [replication.rs](server/src/replication.rs)) |

Known, intentionally unfixed (documented behavior):

- **Command rebasing past the 500 ms window** — a turn arriving after the
  committed-lag window is rescheduled later than the player saw it. This is
  inherent to the current netcode model; it is now *measurable* (trace_rca
  reports per-command tick deltas) rather than invisible. Eliminating it
  requires input-delay or rollback netcode — an architectural decision.
- **Executor failover loses the dead window** — commands sent while no
  executor held the partition are dropped (clients see the reconnect overlay
  and the resumed game fast-forwards to wall-clock time). Bounded by the ~3 s
  lease + claim jitter; games whose executor stays dead longer than the 5-min
  stored-snapshot TTL are not resumable.

## Why these bugs reached production (postmortem)

The three reported symptoms all trace to the same root pattern: **every hop
could silently drop messages, nothing checked for loss, and the client had no
way to notice it had diverged.**

1. **Phantom wall deaths** — commands arriving after the 500 ms committed-lag
   window are rescheduled to a later tick; the client had already executed
   them at the requested tick. The turn happened on screen, not on the
   server. Locally RTT ≈ 0, so the window never expired — prod-only by
   construction. (Clock-sync error has the same effect via mis-stamped
   command ticks.)
2. **Cumulative drift needing periodic snapshots** — Redis pub/sub is
   at-most-once; the broadcast fan-out drops messages when a receiver lags
   (and one error path ended forwarding entirely, silently); nothing checked
   `sequence` contiguity, so every lost event became permanent divergence.
   Periodic snapshots masked the loss instead of detecting it.
3. **Ghost games** — the predicted state free-ran on the local clock with no
   bound and no liveness check, so a dead backend looked like a live game.

Why tests never caught it:

- Several game-flow test binaries (`solo_game_test`, `duel_game_test`,
  `simple_game_test`, plus the terminal crate's tests) **did not even
  compile** on the main branch — the suites most likely to exercise these
  flows were dead code, so a green-looking local run never covered them.
  (Since repaired: all pass again, and CI runs `cargo test --all --no-run`
  so a test binary can never silently rot out of coverage again. Note:
  running these against a Redis shared with a live dev server causes
  cross-talk — pub/sub channels are global across Redis DBs — so stop the
  dev `snaketron-server` container before running them.)
- All integration tests ran over **perfect in-memory transports** — the
  lossy/laggy paths (Redis reconnects, broadcast lag, ALB idle timeouts)
  never executed in CI.
- **No test ever compared the client-path state against the server-path
  state.** The engine is shared, but the client drives it through a
  different call sequence (`process_server_event` + `rebuild_predicted_state`
  vs `run_until`); their equivalence was assumed, never asserted.
- **Local dev is structurally incapable of reproducing the triggers**: one
  process, localhost Redis, ~0 ms RTT, no proxy timeouts. Solo games in prod
  still traverse the full multi-hop relay — which is why "even solo games"
  broke in prod only.
- Error paths logged-and-continued (or logged-and-died) — loss was invisible
  by design.

What now guards each gap: the chaos suite simulates loss/latency/jitter/silence
in-process on every `cargo test`; fingerprint probes make divergence
observable in prod within ~1 s; stream sequencing makes loss detectable at
every hop; traces make any surviving bug replayable offline.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SNAKETRON_TRACE_DIR` | `./traces` | Where server + uploaded client traces go |
| `SNAKETRON_TRACE_DISABLE` | unset | `1` disables the server flight recorder |
| `SNAKETRON_TRACE_MAX_FILES` | `200` | Trace-dir rotation limit |
| `SNAKETRON_BUS` | `streams` | Game-critical transport: `streams` (default) or `pubsub` fallback (see STREAMS_MIGRATION.md). On `streams`, the gap/resync counters should sit at ~0 — nonzero values there mean a transport bug |
