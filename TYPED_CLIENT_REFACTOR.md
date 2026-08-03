# Typed Client Refactor — Audit & Plan

## Implementation status (updated)

**Shipped and verified green** (`cargo check --workspace`, `cargo clippy --all-features -D warnings`,
`cargo fmt --check`, `tsc --noEmit`, and `npm run build` all pass):

- **Phase 0 — deletions + enforcement (COMPLETE).** Deleted the stale ambient
  `wasm-snaketron.d.ts` (tsc now binds the real generated `pkg/client.d.ts`),
  the dead `public/client.*` artifacts, `index.ts`, and `test-simple.ts`.
  Replaced the `window.wasm`/`window.wasmReady` globals with a typed singleton
  (`client/web/wasm/index.ts`: `initWasm()`/`getWasm()`). Wired `tsc --noEmit`
  into `npm run build` and added a `client-types` CI job (build WASM → codegen
  drift check → tsc) — the client had zero type enforcement before.
- **Codegen foundation (COMPLETE).** Added `ts-rs` behind a feature-gated
  `ts-gen` feature in `common` and `server` (off for all normal builds; verified
  the feature-off workspace and the `--all-features` clippy gate both stay
  green). `scripts/gen-types.sh` regenerates 25 wire types from the Rust source
  of truth into `client/web/types/generated/` (+ a barrel), CI fails on drift.
  Wide ints (`u64`/`i64`/`usize`) are pinned to `number` to match `JSON.parse`;
  engine-internal `command_queue`/`rng` are skipped.
- **Phase 1 — protocol typing (SUBSTANTIALLY COMPLETE).** New
  `client/web/types/protocol.ts` derives a tag→payload map from the generated
  `WSMessage` union. `WebSocketContext` now decodes with a single typed
  `parseServerMessage` chokepoint (the `Object.keys` envelope-cracking is gone),
  `onMessage<K>` hands each handler a typed payload, and `sendMessage` takes a
  typed `OutboundMessage`. The drifted hand-written wire types in
  `types/index.ts` were deleted in favour of the generated ones. The type
  swap surfaced the audit's drift bugs as compile errors, now fixed: removed the
  phantom `CustomGameCreated`/`CustomGameJoined`/`NicknameUpdated` inbound
  handlers (server never sends them), corrected `LobbyMember` field access
  (`joined_at`/`is_host` → real `ts`), and typed the game command as the real
  `GameCommandMessage`.

- **Phase 2 — raw-string event path (COMPLETE).** The inbound `GameEvent` path
  no longer round-trips u64s through JS. The WS frame's raw text is carried
  through `TypedMessage.raw` → the `useGameWebSocket` queue (`QueuedGameEvent =
  { raw, message }`; `message` is used only for u32/structural routing) →
  `useGameEngine.processServerEvent`, which hands the raw frame to two new
  Rust-side WASM entry points: `GameClient.processServerFrame` (events) and
  `newFromSnapshotFrame` (snapshots). Both deserialize in Rust, so
  `TickHash.hash` and `rng.state` (full-range u64) stay intact — the live
  false-mismatch/spurious-resync bug is fixed. A regression test in
  `client/src/lib.rs` (`frame_parse_preserves_full_range_u64_hash`) pins it.
  The outbound command is typed as `GameCommandMessage` (its envelope is all
  u32, so `JSON.parse` there is lossless). The dead "bare event wrapping"
  heuristic was removed.

- **Phase 4 — HTTP DTOs + api layer (COMPLETE).** Added ts-rs derives to the
  server's HTTP DTOs (`server/src/api/{auth,leaderboard,regions}.rs`:
  AuthResponse, UserInfo, GuestUserInfo, CheckUsernameResponse,
  CreateGuestResponse, LeaderboardEntryResponse, HighScoreEntryResponse,
  LeaderboardEntry (untagged), LeaderboardResponse, SeasonsResponse,
  UserRankingResponse, RegionMetadata, HealthResponse) — 38 generated types
  total; serde renames (`isGuest`/`winRate`/`gameId`/…) are honored. Rewrote
  `services/api.ts`: `request<T>` (no `= any` default — JSON lands in `unknown`
  and is asserted to the generated DTO at one boundary), a typed `ApiError` +
  `isApiError` guard replacing the thrown-object/`(err as any)` pattern in
  `AuthContext`, and the drifted hand-written response types deleted from
  `types/index.ts` in favour of the generated ones. Added runtime guards on the
  localStorage/network boundaries (`regionPreference` load + `/api/regions`,
  `useRegions` user-counts). The `requiresPassword` phantom (the server never
  sends it) is now an explicit client-only `CheckUsernameResult` field
  defaulted false, documented like the other wire-drift items.

- **Phase 3 — render in Rust (COMPLETE).** Rewrote `client/src/render.rs`
  (~1500 lines) off `serde_json::Value` string-indexing onto a typed
  `render_game_state(&GameState, …)` — every field access is now type-checked
  and the silent `unwrap_or` defaults are gone. Added `GameClient.render(canvas,
  cell_size, rotation, local_user_id)` which renders the engine's own predicted
  state directly, and removed the free `render_game(json, …)` export: GameArena
  no longer does a per-frame `JSON.stringify(gameState)` → parse-to-`Value` →
  serialize round-trip. Usernames/teams are resolved inside the renderer from
  the typed state (the scalar `local_username`/`opponent_username` side-channel
  args are gone). The screen→game input rotation transform moved into Rust
  (`screenDirectionToGame`, beside `transform_coords`, so input and rendering
  share one convention and cannot desync); the duplicated TS `transformDirection`
  was deleted. `getSnakeIdForUser` no longer serializes+reparses the whole state
  on every keypress (reads the typed predicted state directly). Deleted the
  per-frame head-adjacency debug check. GameArena renders via a `renderTo`
  callback that reads the live engine ref (safe across snapshot rebuilds); the
  redundant JSON work the "collapse the second rAF loop" item targeted is
  eliminated, so the two loops now do genuinely different jobs (engine stepping +
  React chrome vs. canvas paint) with no duplicated serialization.

  Caveat: rendering output was not visually verified here (that needs the live
  backend + a running game). The rewrite is a faithful 1:1 mapping of every
  canvas operation; the compiler and clippy cover field/type correctness, and
  the drawing logic, colors, coordinates, and control flow are preserved.

**Not yet done (follow-ups):**

- Some inbound handler *bodies* still annotate `(message: any)` and keep their
  defensive coercion, even though the dispatch/registration surface is fully
  typed (`onMessage<K>` infers the payload). Remaining: the lobby/chat handlers
  in `WebSocketContext` (LobbyCreated, JoinedLobby, LobbyUpdate,
  LobbyChatMessage/History, GameChatMessage/History, AccessDenied,
  LobbyRegionMismatch, `normalizeLobbyPreferences`, `extractGameId`) and a few
  lifecycle handlers in `useGameWebSocket` (GameLoadFailed, SoloGameCreated,
  JoinGame, AccessDenied). Several are entangled with real wire drift — the
  lobby handlers read a `lobby_id` the wire never sends (only `lobby_code`;
  §1 bug 3), so their id-match paths are already dead code — so these want a
  focused, behavior-reviewed pass rather than rushed edits. (The `GameEvent`
  handler and `useGameEngine.processServerEvent` were already retyped in
  Phase 2.) `syncTrace`'s `EventIn.msg`/`CmdOut.cmd` stay `any` intentionally
  until they can reference the generated `GameEventMessage`/`GameCommandMessage`.

---

Goal: eliminate untyped JS-object munging and unvalidated JSON parsing from the client.
All core logic becomes typed operations — either TypeScript checked against
machine-generated types, or Rust behind the WASM boundary. Rust remains the single
source of truth for every wire type; TypeScript never hand-mirrors a Rust type again.

Audit method: 20-agent workflow (9 area auditors, each adversarially re-verified
against the actual code, plus a completeness sweep and a build-feasibility check).
All 9 area recommendations were confirmed by independent verifiers; every finding
below cites confirmed file:line locations.

---

## 1. The headline findings

The untyped code is not just a hygiene problem — it is already causing broken
features and live protocol corruption that the type system never saw:

| # | Live bug (exists today) | Where |
|---|---|---|
| 1 | **TickHash u64 corruption on the live event path.** Every inbound `GameEvent` is `JSON.parse`d in TS (all numbers → f64), then re-`JSON.stringify`d and handed to WASM serde. `TickHash.hash` is a full-width 64-bit FNV-1a digest (`fingerprint.rs` `finish()` returns raw state; `game_engine.rs:255` compares full u64). Any hash above 2^53 — ~99.95% of uniform digests — is silently altered in transit, so hash probes mismatch for reasons that have nothing to do with real divergence, feeding `consecutive_hash_mismatches >= 2` → spurious `needs_resync`. The codebase already knows the rule (`getCommittedHash` returns a *string* for exactly this reason, `lib.rs:204-209`) but applies it in only one direction. Same corruption class applies to `GameState.rng.state` (full-range u64) inside Snapshots. | `WebSocketContext.tsx:322` → `useGameEngine.ts:458` → `lib.rs:112-114` |
| 2 | **The entire custom-game feature is silently dead.** The client sends `CreateCustomGame`, `JoinCustomGame`, `UpdateCustomGameSettings`, `StartCustomGame`, `SpectateGame` — none of these variants exist in the server's `WSMessage` enum, so serde rejects each frame with a server-side log and the UI silently does nothing. In the other direction the client registers handlers for `CustomGameCreated`, `CustomGameJoined`, `NicknameUpdated` — which the server never defines or sends — and for `SoloGameCreated`, `SpectatorJoined`, `ServerShutdown`, `AuthorityTransfer`, which are declared server-side but never constructed. | `useGameWebSocket.ts:340-415`, `WebSocketContext.tsx:1286-1299`, `ws_server.rs:35-160` |
| 3 | **`Lobby.id` is always `undefined` at runtime** despite being typed `number`. Handlers destructure `lobby_id` from `LobbyCreated`/`JoinedLobby`/`LobbyUpdate` payloads, but the server sends only `lobby_code`. The undefined id is persisted to localStorage and the `matchesById` path in the LobbyUpdate handler is dead code. | `WebSocketContext.tsx:633,735,1021` vs `ws_server.rs:129-147` |
| 4 | **tsc typechecks the WASM API against fiction.** The hand-written ambient `declare module 'wasm-snaketron'` shadows the tsconfig `paths` mapping to the accurate generated `client/pkg/client.d.ts` (empirically reproduced: ambient module declarations beat paths resolution). The ambient file declares a phantom `Game` class, a phantom `render` export, `GameType` variants `QuickPlay`/`Competitive` that don't exist in Rust, `GameState.properties: any`, and omits ~10 real fields. | `client/web/wasm-snaketron.d.ts` |
| 5 | **Nothing enforces any of the types anyway.** The webpack build uses babel type-stripping; `tsc --noEmit` exists only as a manual script no build or CI path invokes. CI (`github-action-test-simple-game.yml`) runs cargo fmt/clippy/test only — it never builds the WASM pkg, never installs client deps, never typechecks. There is no eslint and no runtime-validation library in the dependency tree. | `client/web/package.json`, `.github/workflows/` |
| 6 | **Hand-written TS mirrors have drifted in both directions.** `types/index.ts` `GameState` declares phantom fields (`game_id: string`, `game_ended`, `final_score`, `duration`) that Rust never serializes, and omits real ones (`queue_mode`, `command_queue`, `rng`, `game_code`, `host_user_id`); `Arena` omits `team_zone_config`; `LobbyMember` declares phantom `joined_at`/`is_host` and misses `ts`; `Command` has a phantom `Respawn` variant and `Turn` omits `snake_id`; `command_id_server: null` is wrong for the receive direction (server echoes `Some`); every `Option` field is typed `?:` when the wire says always-present-but-null; `User` misses `mmr`; `CheckUsernameResponse.requiresPassword` is a phantom the client fabricates. The TS `WebSocketMessage` union covers 6 of 37 real variants plus 2 that don't exist. | `types/index.ts` throughout |

And the structural problem underneath all six: **the protocol has no shared
definition.** `WSMessage` (37 variants, externally tagged serde) lives in the
server crate only. The client re-derives the envelope convention by hand —
`Object.keys(rawMessage)[0]` dispatch into `(message: { type: string; data: any })`
handlers (27 registration sites, every one receiving `any`) — and then defends
itself with ~600 lines of `typeof` normalization against shapes the server can
never send, while missing the drift that actually exists.

## 2. Decision framework

For each part, the language decision follows one rule: **where does the authoritative
type already live, and does the logic need the engine's determinism?**

- Logic that must be deterministic and identical to the server (simulation, sync,
  time base, rendering of simulation state) → **Rust**, behind the WASM boundary,
  and the data should not leave Rust at all if it doesn't have to.
- Logic that is React/DOM/lifecycle-coupled (dispatch to hooks, UI state machines,
  fetch, localStorage) → **TypeScript**, but typed against **generated** types whose
  source of truth is the Rust definition (ts-rs), with `JSON.parse` landing in
  `unknown` at exactly one chokepoint per boundary.
- Moving the WS dispatch layer itself into Rust was considered and rejected by both
  the auditor and the verifier: it would add a WASM crossing per message and hand
  payloads back to React as untyped JSON anyway. Wrong altitude.
- tsify/serde-wasm-bindgen (structured `JsValue` instead of JSON strings) was
  evaluated and **deferred**: it changes runtime representation (u64 → BigInt,
  HashMap → JS Map) which breaks every existing consumer at once, and the JSON-string
  boundary's byte-identity caching in `useGameEngine` is a real optimization worth
  keeping. ts-rs types the existing boundary with zero runtime change. (The workspace
  even contains a homegrown, unused `#[serde_wasm_bindgen]` proc-macro in `macros/` —
  evidence this migration was once started; delete or leave, but don't block on it.)

### Per-part decisions

| Part of the code | Decision | Why |
|---|---|---|
| Wire protocol definition (`WSMessage`, lobby/chat structs) | **Rust source of truth, moved to `common`**, split into `ClientMessage`/`ServerMessage`, TS types generated | Same tags/shapes = wire-compatible; split kills the `JoinGame` two-semantics ambiguity and makes dead variants visible |
| WS envelope parse + dispatch (`WebSocketContext`) | **TypeScript**, typed via generated discriminated union | React-coupled; one typed `parseServerMessage` chokepoint replaces `Object.keys` cracking |
| Game event payload transit (WS → engine) | **Raw string pass-through; payloads never materialized in JS** | Fixes u64 corruption (bug #1) with no wire change; TS peeks only at the envelope tag |
| Engine state consumed by React (`useGameEngine`) | **TypeScript against generated `GameState`/`SyncStatus` types**; decisions move behind **Rust accessors** | `isCommittedComplete()`, `acceptsInput()`, `takeSyncIncidents()` replace duck-typed `'Complete' in status` probing on `any` |
| Rendering (`render.rs` + `GameArena` rAF loop) | **Rust end-to-end** — render from the engine's own `&GameState`; delete the JSON round-trip | The engine already holds the typed state; today it's stringified, parsed untyped in TS, re-stringified, and re-parsed as `serde_json::Value` per frame |
| Input direction mapping (rotation transform) | **Rust** (`GameClient` method) | `GameArena.transformDirection` hand-inverts `render.rs::transform_coords` in TS — same math, two languages, no shared source |
| Clock sync (`clockSync.ts`) | **Math into Rust engine; transport stays TS** | Drift compensation determines command tick stamping — engine-determinism territory |
| Sync trace recorder (`syncTrace.ts`) | **TypeScript with generated `TraceRecord` types** | Records must mirror `common::trace::TraceRecord` exactly for offline RCA joining; today `msg`/`cmd` slots are `any` |
| HTTP/auth/regions/leaderboard DTOs (`api.ts`, hooks) | **TypeScript with generated DTOs from server api structs** | fetch/localStorage/React glue; Rust adds marshalling for zero gain. Kill `request<T = any>` blind casts |
| localStorage prefs (`lobbyPreferencesStorage`, `regionPreference`, latency) | **TypeScript + hand-written guards** (`parse` → `unknown` → guard) | 3 small shapes; zod is overkill; nothing here touches game state |
| Component computations (Scoreboard/Leaderboard aggregation, GameType constructors) | **TypeScript typed selectors module** over generated types | Pure presentation math; exhaustive `never`-checked discriminators in one place |
| Chat normalization (4 duplicated ~40-line blocks) | **Delete** — typed payloads make it dead code | The blocks exist only because payloads are `any` |
| Stale artifacts (`public/client.*`, `wasm-snaketron.d.ts`, `index.ts`, dead `lib.rs` methods) | **Delete** | Verified dead by grep + webpack-chain analysis |

## 3. Type-generation mechanism (verified feasible)

**ts-rs v11, feature-gated, codegen at test time.** Verified against the actual
toolchain: wasm-bindgen 0.2.100, serde 1.0.219, edition 2024, TS 5.7.3 — no blockers.

- `ts-rs = { version = "11", optional = true }` in `common` (and `server` for HTTP
  DTOs) behind a `ts-gen` feature. `#[cfg_attr(feature = "ts-gen", derive(TS), ts(export))]`
  on wire types. Server build, WASM build, and clippy `-D warnings` run with the
  feature **off** — zero impact.
- Generation runs natively: `TS_RS_EXPORT_DIR=client/web/types/generated cargo test -p common --features ts-gen`.
  Generated files are **committed**; CI regenerates and fails on `git diff --exit-code`.
- serde external tagging maps directly to TS discriminated unions
  (`{"Variant": data}` | `"UnitVariant"`); external tagging is mandatory anyway —
  `trace.rs:27` documents integer-keyed maps that internal tagging can't represent.
- Sharp edges, all with known answers: `CommandQueue`'s `BinaryHeap<Reverse<_>>` →
  `#[ts(skip)]`-class annotation (client TS never reads it); numeric-keyed HashMaps →
  ts-rs emits `{ [key: number]: T }` which matches serde's string-keyed JSON closely
  enough for TS consumers; u64/i64 → see policy below.
- The `sync_equivalence` chaos suite passes Rust structs in-memory (its
  `serde_json::to_value` is only for determinism recording, and `serde_json::Number`
  holds u64 exactly) — it does **not** pin the client boundary format. Safe.

**u64 policy** (documented in the generated header): monotonic counters
(`sequence`, `stream_seq`, `tick`-adjacent) are fine as `number` — they are
nowhere near 2^53. Full-range digests (`TickHash.hash`, `rng.state`) must never
be materialized as a JS number; they either stay inside the raw-string pass-through
path (Phase 2) or cross as strings (as `getCommittedHash` already does).

## 4. Phased plan

Each phase is independently shippable and leaves the game working. Order is
chosen so enforcement lands first (so later phases can't regress) and the live
correctness bug lands early.

### Phase 0 — Deletions + enforcement (small, no behavior change)

1. Delete `client/web/wasm-snaketron.d.ts` → tsc binds the real generated
   `client/pkg/client.d.ts` via the existing `paths` mapping. (Verifier confirmed
   every actually-called method matches the generated surface; run `tsc --noEmit`
   after.) CI must run `wasm-pack build` before typechecking so `pkg/` exists.
2. Delete dead artifacts: `client/web/public/client.{js,d.ts}`,
   `client_bg.{js,wasm}`, `client_bg.wasm.d.ts`, `public/package.json`,
   `client/web/index.ts`, `test-simple.ts` (or align it with `global.d.ts`).
3. Delete dead `lib.rs` methods (zero call sites, each a JSON contract maintained
   for nothing): `runUntil`, `initializeFromSnapshot`, `getEventLogJson`,
   `getCommittedHash`, `getPredictedTick`, `getGameId`, and one of the
   `getCurrentTick`/`getCommittedTick` aliases.
4. Replace `window.wasm`/`window.wasmReady` globals with a typed module singleton
   (`getWasm(): Promise<typeof wasm>`); update `useGameEngine`/`GameArena`/`main.tsx`.
5. Enforcement: add `tsc --noEmit` to the build script and CI; add a client CI job
   (wasm-pack build → npm ci → tsc → future codegen drift check); add eslint +
   `@typescript-eslint` with `no-explicit-any` + `no-unsafe-*` (error on new, so
   the count only shrinks). Optional tsconfig hardening: `noUncheckedIndexedAccess`,
   drop `allowJs`.

### Phase 1 — Protocol: shared enum + generated types + typed dispatch

1. Move the WS protocol out of `server/src/ws_server.rs` into `common` (new
   `protocol` module): split `WSMessage` into `ClientMessage` and `ServerMessage`
   keeping every existing tag and field shape wire-compatible. Move
   `LobbyChatBroadcast`, `GameChatBroadcast`, `lobby_manager::LobbyMember`,
   `LobbyPreferences` (the wire halves) along with it. Make broadcast-struct
   fields non-private where TS needs them.
2. The split immediately surfaces bug #2 at compile time. Decide per variant:
   dead server variants (`SoloGameCreated`, `SpectatorJoined`, `ServerShutdown`,
   `AuthorityTransfer`) — delete or implement; phantom client sends (custom-game
   family, `SpectateGame`) — delete the UI paths or implement the server side.
   **This is a product decision checkpoint**; the refactor only makes it visible.
3. ts-rs derives on `ClientMessage`/`ServerMessage` + transitive graph
   (`GameEventMessage`, `GameEvent`, `GameCommandMessage`, `GameCommand`,
   `CommandId`, `GameState`, `Arena`, `Snake`, `Position`, `Direction`,
   `GameStatus`, `GameType`, `GameMode`, `QueueMode`, `TeamId`, `GameProperties`,
   `CustomGameSettings`, `SyncStatus`, `Player`, `TraceRecord`). Emit to
   `client/web/types/generated/`, commit, CI drift-gate.
4. Typed dispatch in `WebSocketContext`: one `parseServerMessage(raw: string):
   { msg: ServerMessage; rawText: string } | null` chokepoint (validates the
   single-key envelope; retains the raw frame text for Phase 2);
   `onMessage<K extends ServerMessageTag>(type: K, handler: (data: PayloadOf<K>) => void)`;
   `sendMessage(m: ClientMessage)`. Delete the 4 duplicated chat-normalization
   blocks, `extractGameId`, `normalizeLobbyPreferences`'s dual-casing tolerance,
   and every `message?.data ?? message?.X ?? message` envelope probe — all dead
   once payloads are typed.
5. Replace `AccessDenied` reason-substring matching (`isLobbyMissingReason`,
   lobby-restore retry logic) with a typed error-code enum on the wire
   (`AccessDenied { code, reason }` — additive, wire-compatible).
6. Fix bug #3 as a by-product: `Lobby` becomes code-keyed (drop the phantom `id`).
7. Delete the hand-written protocol/game mirrors in `types/index.ts` (keep the
   genuinely client-local UI types: `Region`, `UIGameSettings`, `ChatMessage`, etc.).

### Phase 2 — Game event path: raw strings in, typed views out (fixes bug #1)

1. Inbound: for `ServerMessage::GameEvent` frames, TS never materializes the
   payload. `parseServerMessage` hands the handler the payload's **raw JSON text**
   (sliced from the retained frame text, or the whole frame); `useGameWebSocket`'s
   queue becomes `Array<{ rawEvent: string; gameId: number; isSnapshot: boolean }>`
   (envelope fields it needs for routing come from the typed parse);
   `useGameEngine.processServerEvent` passes the raw string straight to
   `GameClient.processServerEvent`/`newFromState`. u64s (`TickHash.hash`,
   `rng.state`) now cross JS as opaque text — corruption gone. Verify with the
   flight recorder afterward: `total_mismatches` on healthy connections should
   drop to ~0, and spurious `needs_resync` churn should disappear.
2. Outbound: `processTurn` return stays a JSON string but TS parses it as the
   generated `GameCommandMessage` type at one chokepoint; `onCommandReady`
   loses its `any`.
3. Engine accessors (Rust) replace duck-typed decisions on parsed state:
   `isCommittedComplete()`, `acceptsInput()`, `getSnakeIdForUser` (already exists —
   but fix it to read `players` directly instead of serializing the whole predicted
   state per keypress, `lib.rs:219-229`), and a `takeSyncIncidents()` that returns
   typed gap/mismatch/watchdog deltas so `useGameEngine` stops diffing `SyncStatus`
   JSON by hand. `getGameStateJson`/`getSyncStatusJson` keep their string form
   (byte-identity caching is worth keeping) but their `JSON.parse` results are
   typed as the generated `GameState`/`SyncStatus`.
4. `clockSync.ts`: move drift math (offset estimation, tick-time computation)
   into the engine; TS keeps only the Ping transport and feeds raw samples in.
   `syncTrace.ts`: type `EventIn.msg`/`CmdOut.cmd` as generated
   `GameEventMessage`/`GameCommandMessage`.

### Phase 3 — Rendering never leaves Rust

1. Add `GameClient.render(canvas, cellSize, rotation, localUserId)` that renders
   the engine's own predicted `&GameState` directly. Rewrite `render.rs` to take
   `&GameState` instead of parsing `serde_json::Value` — deleting all 51
   `unwrap_or` silent-default sites and every stringly index. Usernames/teams
   resolve inside Rust from `state.players`/`state.usernames` (kills the scalar
   side-channel args and `GameArena`'s `Object.entries` + `parseInt` munging).
2. Collapse `GameArena`'s second rAF loop into `useGameEngine`'s single loop:
   step the engine and render in the same frame. React keeps receiving parsed
   state at tick cadence for UI chrome (scoreboard, overlays) — rendering itself
   no longer round-trips.
3. Move the input rotation transform into Rust (e.g. `processTurnScreen(direction,
   rotation)` or an exported pure mapping) so `transformDirection` in TS dies and
   input/render rotation can't desynchronize.
4. Delete the per-frame head-adjacency debug check in `GameArena` (engine-invariant
   diagnostics belong in the engine; it also checks the wrong snake).

### Phase 4 — HTTP DTOs, storage, components

1. ts-rs derives (behind the same feature) on server api structs: auth
   (`AuthResponse`, `UserInfo`, `GuestUserInfo`, `CheckUsernameResponse`, …),
   regions (`RegionMetadata`, `HealthResponse`), leaderboard (`LeaderboardResponse`,
   `SeasonsResponse`, `UserRankingResponse`, entries), plus a shared
   `ErrorResponse` replacing the ad-hoc `json!({"error": ...})` envelopes.
   Rewrite `services/api.ts`: `request` returns `unknown`, callers cast to
   generated types at one line each; typed error object replaces
   `(err as any)?.response?.status` probing. Delete the `requiresPassword`
   fabrication; add `mmr` to the user flow (real field, currently dropped).
2. localStorage boundaries: `parse` → `unknown` → hand-written guards
   (`isRegionPreference`, stored-lobby shape); clamp `LatencySettings` per-field.
   Delete the three dead `constants.ts` exports (one already drifted from the
   server value); share `DEFAULT_TICK_INTERVAL_MS` via a drift-guard test.
3. `client/web/game/selectors.ts`: typed, `never`-exhaustive
   `matchGameStatus`/`matchGameType` helpers and the Scoreboard/Leaderboard
   aggregation (snake↔player↔team mapping, score math) — one place that knows
   the union shapes; components consume named results. Consolidate the three
   duplicated `GameType`-constructor literals; type `LobbyUpdate.members`
   against generated `LobbyMember` (which fixes the phantom `joined_at`/`is_host`
   fields Sidebar currently renders).
4. Type `window.__wsMonitor` against the generated `ServerMessage` (or delete it
   with its Playwright consumers if obsolete).

### Phase 5 (optional, later) — wire-level hardening

Not required for type safety, surfaced by the audit as follow-ups:
- Sanitize the Snapshot payload: a client-facing `GameState` view without `rng`,
  `command_queue`, and (for non-hosts) `game_code`/`host_user_id` — smaller wire,
  removes the remaining full-range u64 from the wire, and stops leaking private
  game codes to all players. Touches the sync suite; do it as its own change.
- String-encode `TickHash.hash` on the wire (defense in depth once Phase 2
  already fixed the client path).
- tsify/serde-wasm-bindgen structured boundary — only if profiling shows the
  JSON-string boundary itself matters; the string-identity caching argues for
  keeping it.
- Server-internal stringly typing found in passing: matchmaking Redis pubsub
  messages are `serde_json::Value` with magic `"type"` strings
  (`ws_server.rs:741-789` / `matchmaking.rs:1249`); lobby `state: String` is a
  cross-file magic-string state machine. Same cure (shared enums), server-only
  scope.

## 5. Risks

- **Phase 1 step 2 is a feature decision, not a refactor step** — the compile
  errors force explicit choices about the custom-game/spectate family. Budget a
  product pass.
- The `LobbyUpdate`-handler's `normalizeLobbyPreferences` currently *throws* if
  `preferences` is absent; typed payloads change error behavior from silent
  tolerance to compile-time requirements — review each deleted defensive branch
  against what the server actually guarantees (the generated types encode exactly
  that).
- ts-rs emits types from serde's *attributes*; any future
  `#[serde(rename/skip/default)]` change regenerates TS and the CI drift gate
  catches a missed regeneration. The gate is the load-bearing piece — land it in
  Phase 0/1, not at the end.
- Phase 2 changes event transit plumbing around the engine; the chaos suite
  (`sync_equivalence_test.rs`) stays green by construction (it doesn't exercise
  the JS boundary), so add one integration check: a client-side test that feeds a
  recorded frame sequence (with >2^53 hashes) through the new raw-string path and
  asserts zero mismatches.

## 6. What stays TypeScript forever (and that's correct)

React components and hooks' lifecycle logic, the WS reconnect/backoff state
machine, fetch/localStorage plumbing, UI-only types (`Region`, `UIGameSettings`,
`ChatMessage`), and the visual layer around the canvas. The refactor's end state
is not "everything in Rust" — it is: **every byte that crosses a process or
language boundary has exactly one typed definition, in Rust, and TypeScript code
is checked against generated types derived from it, with `unknown`-not-`any` at
the few remaining parse sites.**
