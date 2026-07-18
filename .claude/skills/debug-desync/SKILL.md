---
name: debug-desync
description: Root-cause a SnakeTron state-sync bug (phantom death, drift, ghost game, desync report) from captured traces — replay them deterministically, find the first divergent tick, and turn the bug into a local failing test. Use whenever a sync issue is reported from prod or observed in dev.
---

# Debugging a state-sync issue from traces

Read DEBUGGING.md at the repo root first — it explains the four sync
primitives (fingerprints, TickHash probes, stream_seq, flight recorders) and
the full runbook. This skill is the condensed procedure.

## 1. Collect evidence

- Server trace: `traces/game_<id>_server_*.jsonl` (or `$SNAKETRON_TRACE_DIR`).
- Client trace: `traces/game_<id>_client_<user>_*.jsonl` — auto-uploaded on
  detected desyncs; otherwise ask the reporter to run
  `window.snaketronDebug.downloadTrace()` in the browser console.
- If you only have one side, proceed anyway — a server trace alone can prove
  or rule out engine nondeterminism; a client trace alone shows what the
  client actually received.

## 2. Analyze

```bash
cargo run --bin trace_rca -- <server_trace> [<client_trace>]      # human report
cargo run --bin trace_rca -- <server_trace> <client_trace> --json # machine-readable
```

Trust the verdict as a starting hypothesis, then verify against the report
details:

- `TRANSPORT_LOSS`: check `missing_stream_seqs` — which messages, at what
  tick. Then ask: did the client detect the gap (`Note` records) and resync?
  If not, the detection/resync path is the bug, not the loss itself.
- `ENGINE_NONDETERMINISM`: same inputs produced different states. Use the
  first divergent tick to bisect the engine logic (`common/src/game_state.rs
  tick_forward`, event application order, native-vs-WASM differences). This
  is the most serious verdict.
- `CLOCK_DRIFT`: client tick computation off; check `Clock` records and
  command reschedule deltas.
- Phantom-death report: look at command latency — a command whose
  `command_id_server.tick` > `command_id_client.tick` was rescheduled because
  it arrived past the 500 ms committed-lag window; the client showed the turn
  at the requested tick, the server executed it later.

## 3. Freeze the bug as a test

```bash
cargo run --bin trace_rca -- <server_trace> --emit-test server/tests/repro_<desc>.rs
cargo test -p server --test repro_<desc>
```

Commit the trace fixture and the test with the fix.

## 4. Fix and generalize

After fixing, extend the chaos suite
(`server/tests/sync_equivalence_test.rs`) with the failure shape you found —
that suite simulates lossy/latent/jittery transport in-process and is the
regression barrier for this bug class. `cargo test -p server` must be green.

## Live-system spot checks

- Client sync health in the browser console:
  `JSON.parse(engine.getSyncStatusJson())` — gap counts, probe mismatches,
  `needs_resync`.
- Server logs: grep for `lagged`, `gap`, `resync`, `channel full` — every
  loss path now logs loudly.
