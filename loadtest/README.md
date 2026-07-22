# Snaketron coordinated load test

`snaketron-loadtest` creates real guest sessions, connects them to one Snaketron region, puts each intended match into a complete lobby, queues synchronized waves, and plays every snake with the repository's existing AI.

The runner deliberately does not use one browser per virtual user. The React hooks require a DOM, router, storage, and animation frames, making the load generator the bottleneck at autoscaling volumes. It instead reuses the same `common::GameEngine` compiled to WebAssembly for the React client, the same `common::calculate_ai_move` AI, and the server's canonical WebSocket message types.

## Production autoscaling run

Build in release mode and run from a machine outside the Snaketron cluster:

Deploy the accompanying server changes first; `--require-scale-out` preflights
the new `/api/regions/server-counts` observer endpoint before creating users.

```bash
cargo run --release -p loadtest -- \
  --target https://snaketron.io \
  --confirm-production \
  --require-scale-out \
  --mode duel \
  --queue-mode competitive \
  --run-id autoscale-20260721
```

The default staircase targets 4, 16, 64, 128, then 256 concurrent sessions for progressively longer bounded ramp-and-hold windows. Whole match groups are started at no more than four sessions per second, and completed groups are replaced through that same limiter. Override the plan and rate with, for example:

```bash
--stages 4@30s,32@1m,128@3m,512@5m
--spawn-rate 8
```

Supported modes are `solo`, `duel`, `2v2`, and `ffa`. Every target must be a multiple of the match size: 1 for solo, 2 for duel, and 4 for 2v2/FFA. Solo uses one player per lobby; duel, 2v2, and FFA put the complete 2- or 4-player party in one lobby, deliberately covering multi-user lobby create/join/update behavior. The FFA matchmaker prefers a complete party over older partial public lobbies so test membership remains deterministic.

Duel and 2v2 use the authoritative server game limit. Solo and FFA have no server time limit, so they play with the AI for two minutes by default, send `LeaveGame`, and confirm that the server processed it with an ordered ping. Natural completion before that window remains authoritative. Override the window with `--untimed-play-duration 5m`; when increasing it, also leave enough `--drain-timeout` for authentication, lobby setup, matchmaking, and the complete play window.

`--max-total-sessions 4096` is a hard default safety ceiling across replacements. Production targets require `--confirm-production`; the check is repeated against the region origin and effective WebSocket URL returned by discovery. TLS certificate and hostname checks are never disabled.

For a small invariant check, four duel sessions are released together as two complete lobbies and therefore create exactly two initial games:

```bash
cargo run --release -p loadtest -- \
  --target https://snaketron.io \
  --confirm-production \
  --mode duel \
  --queue-mode competitive \
  --stages 4@30s \
  --run-id four-session-check
```

## Session behavior

Each virtual user performs the production lifecycle:

1. `POST /api/auth/guest` with a deterministic `lt...` nickname.
2. Discover regions through `/api/regions`; all sessions use the same healthy region.
3. Connect by WebSocket and authenticate with the guest token.
4. Create or join the match group's lobby and wait until every expected user ID appears in `LobbyUpdate`.
5. Wait at a wave barrier, then have the lobby host queue the complete party.
6. Receive `JoinGame`, join the assigned game, and initialize the shared `GameEngine` from its snapshot.
7. Verify that the snapshot contains exactly the intended lobby members.
8. Reconcile authoritative events, calculate AI moves once per predicted tick, and send normal game commands until authoritative completion. For an untimed Solo/FFA game, play until `--untimed-play-duration`, then leave successfully.
9. Send application pings, measure RTT/clock offset, and reconnect up to twice using the browser client's two-second delay.

A complete duel/2v2 lobby is split across the opposing teams by the existing matchmaker; a complete FFA lobby is selected intact. These choices isolate intended games from unrelated public queue participants. Snapshot membership validation turns any unexpected pairing into an explicit session failure.

`--command-profile realistic` sends only actual direction changes, like UI input. `--command-profile every-tick` intentionally saturates the command path.

## Reports

Artifacts are written to `loadtest-reports/<run-id>/` by default:

- `index.html` — aggregate session, authoritative/timeboxed game, latency, traffic, ramp, and infrastructure overview.
- `summary.json` — the machine-readable aggregate plus compact status and completion kind for every session.
- `failures/*.json` — complete lifecycle, metrics, failure context, and recent protocol events for every failed, cancelled, or incomplete session; the HTML report links to these files.

The command exits unsuccessfully if fewer than 98% of launched sessions complete, the configured peak is never observed as concurrent logical sessions that have sent their authentication tokens, a session is lost from coordinator accounting, a stage misses that token-sent session target, a launched game is never observed, or deterministic pairing validation fails. Logical session concurrency continues across short reconnect gaps and does not claim every transport socket remains continuously open. Coordinator panics and force-aborted groups are synthesized as individual failed-session artifacts instead of disappearing from the denominator.

Autoscaling evidence is reported separately. The harness samples regional user counts and active regional server counts throughout the run. Backend-cookie aliases, when present, remain a secondary in-band routing hint.

Press Ctrl-C to stop adding load and drain active games. After `--drain-timeout` (five minutes by default), remaining sessions are cancelled and still included in the report. A drain cancellation is never reported as a successful timebox.

## Production data note

The current guest endpoint persists users, and completed matches can affect game/ranking records. Synthetic names are deterministic and prefixed `lt`; normally each created user ID is retained in its session diagnostics. A process/task panic after guest creation can lose that server-assigned ID, though the deterministic name and a synthetic failure record remain. If production load tests become routine, add a server-side synthetic-user marker with TTL and ranking exclusion.

## Validation

```bash
cargo test -p loadtest
cargo check -p loadtest
```
