import assert from 'node:assert/strict';
import test from 'node:test';

import {
  PLANNED_HANDOFF_MAX_MS,
  RECONNECT_MAX_MS,
  advanceCandidateGameWatermark,
  activeGameIdFromPath,
  candidateDeadlineDelayMs,
  candidateCoversActiveWatermark,
  isFreshSnapshotForGame,
  isCommandOwner,
  isSnapshotForGame,
  isTerminalSnapshotForGame,
  missingRequiredServerCapabilities,
  plannedDrainRemainingMs,
  promotionOrderingFrontier,
  reconnectDelayMs,
  replacementFailureAction,
  replacementReadyForPromotion,
} from '../../services/websocketLifecycle.ts';

test('the current websocket protocol fails closed when a capability is absent', () => {
  const current = [
    'explicit-auth-v1',
    'planned-drain-v1',
    'socket-generation-v1',
    'command-delivery-v2',
    'command-outcomes-v1',
    'command-outcome-barrier-v1',
  ];
  assert.deepEqual(missingRequiredServerCapabilities(current), []);
  assert.deepEqual(
    missingRequiredServerCapabilities(current.filter((value) => value !== 'command-delivery-v2')),
    ['command-delivery-v2'],
  );
});

test('the first crash reconnect is immediate', () => {
  assert.equal(reconnectDelayMs(0, () => 1), 0);
});

test('a failed candidate falls back to crash reconnect if the old socket died', () => {
  const deadline = 10_000;
  assert.equal(
    replacementFailureAction(false, true, deadline, 1_000),
    'reconnect-active',
  );
  assert.equal(
    replacementFailureAction(true, true, deadline, 1_000),
    'retry-candidate',
  );
  assert.equal(
    replacementFailureAction(false, false, deadline, 1_000),
    'none',
  );
});

test('a candidate cannot send commands until ownership switches atomically', () => {
  assert.equal(isCommandOwner(1, 1, 'active', 1), true);
  assert.equal(isCommandOwner(1, 2, 'candidate', 1), false);
  assert.equal(isCommandOwner(2, 1, 'retired', 1), false);
  assert.equal(isCommandOwner(2, 2, 'active', 1), true);
  assert.equal(isCommandOwner(2, 2, 'active', 0), false);
});

test('jittered reconnects remain inside the two-second bound', () => {
  for (let attempt = 1; attempt < 50; attempt += 1) {
    for (const random of [-1, 0, 0.5, 1, 2]) {
      const delay = reconnectDelayMs(attempt, () => random);
      assert.ok(delay >= 0);
      assert.ok(delay <= RECONNECT_MAX_MS);
    }
  }
  assert.equal(reconnectDelayMs(50, () => 1), RECONNECT_MAX_MS);
});

test('game restoration only accepts a matching fresh snapshot', () => {
  assert.equal(activeGameIdFromPath('/play/42'), 42);
  assert.equal(activeGameIdFromPath('/play/42/details'), 42);
  assert.equal(activeGameIdFromPath('/play/not-a-number'), null);
  assert.equal(activeGameIdFromPath('/lobby'), null);

  const snapshot = {
    GameEvent: {
      game_id: 42,
      event: { Snapshot: { game_state: {} } },
    },
  };
  assert.equal(isSnapshotForGame(snapshot, 42), true);
  assert.equal(isSnapshotForGame(snapshot, 41), false);
  assert.equal(
    isSnapshotForGame({ GameEvent: { game_id: 42, event: { TickHash: {} } } }, 42),
    false,
  );
  assert.equal(isFreshSnapshotForGame(snapshot, 42, null), true);
  assert.equal(isFreshSnapshotForGame(snapshot, 42, 1), false);
  snapshot.GameEvent.stream_seq = 7;
  assert.equal(isFreshSnapshotForGame(snapshot, 42, 7), true);
  assert.equal(isFreshSnapshotForGame(snapshot, 42, 8), false);
  snapshot.GameEvent.event.Snapshot.game_state.status = { Complete: { winning_snake_id: null } };
  assert.equal(isFreshSnapshotForGame(snapshot, 42, 8), true);
});

test('planned handoff waits when the old socket advances after the candidate snapshot', () => {
  const snapshotAt10 = {
    GameEvent: {
      game_id: 42,
      stream_seq: 10,
      event: { Snapshot: { game_state: {} } },
    },
  };
  const deltaAt11 = {
    GameEvent: {
      game_id: 42,
      stream_seq: 11,
      event: { TickHash: { hash: 1, server_ts_ms: 1 } },
    },
  };

  let candidateWatermark = advanceCandidateGameWatermark(null, snapshotAt10, 42);
  // Snapshot and outcome barrier are ready at N, but the still-usable old
  // socket has since observed N+1. Promotion must remain blocked.
  assert.equal(candidateCoversActiveWatermark(candidateWatermark, 11), false);
  assert.equal(candidateCoversActiveWatermark(null, null), false);

  candidateWatermark = advanceCandidateGameWatermark(candidateWatermark, deltaAt11, 42);
  assert.equal(candidateCoversActiveWatermark(candidateWatermark, 11), true);

  // A non-contiguous N+2 cannot be mistaken for catch-up.
  const gapAt13 = {
    GameEvent: {
      game_id: 42,
      stream_seq: 13,
      event: { TickHash: { hash: 2, server_ts_ms: 2 } },
    },
  };
  assert.equal(advanceCandidateGameWatermark(candidateWatermark, gapAt13, 42), 11);
});

test('matching pong freezes the promotion frontier while the old stream keeps advancing', () => {
  const readiness = {
    socketOpen: true,
    authenticated: true,
    inFlightRequestCount: 0,
    lobbyReady: true,
    gameReady: true,
    gameComplete: false,
    commandOutcomesReady: true,
    expectsGame: true,
    gameStreamWatermark: 11,
  };

  // The old socket advanced through 12 before its matching pong, so 12 is
  // required. Frames after that pong cannot turn catch-up into a moving target.
  const frozenAtPong = promotionOrderingFrontier(12, true, 12);
  assert.equal(replacementReadyForPromotion(readiness, frozenAtPong), false);

  readiness.gameStreamWatermark = 12;
  const oldStreamNowAt = 100;
  assert.equal(
    replacementReadyForPromotion(
      readiness,
      promotionOrderingFrontier(oldStreamNowAt, true, frozenAtPong),
    ),
    true,
  );
  assert.equal(
    replacementReadyForPromotion(
      readiness,
      promotionOrderingFrontier(oldStreamNowAt, false, null),
    ),
    false,
  );
});

test('a takeover snapshot replaces the retired owners higher transport watermark', () => {
  const takeoverSnapshot = {
    GameEvent: {
      game_id: 42,
      stream_seq: 991,
      event: { Snapshot: { game_state: {} } },
    },
  };
  const replacementSnapshot = {
    GameEvent: {
      game_id: 42,
      stream_seq: 992,
      event: { Snapshot: { game_state: {} } },
    },
  };

  // The old owner published through 1000 after checkpointing 990. Recovery
  // is allowed to re-anchor from that durable checkpoint, so 991 must replace
  // (not be maxed with) the retired transport's watermark.
  const activeWatermark = advanceCandidateGameWatermark(1000, takeoverSnapshot, 42);
  assert.equal(activeWatermark, 991);
  const candidateWatermark = advanceCandidateGameWatermark(null, replacementSnapshot, 42);
  assert.equal(candidateCoversActiveWatermark(candidateWatermark, activeWatermark), true);
});

test('dual-socket handoff promotes only after every replacement phase and switches one command owner', () => {
  const readiness = {
    socketOpen: true,
    authenticated: false,
    inFlightRequestCount: 0,
    lobbyReady: false,
    gameReady: false,
    gameComplete: false,
    commandOutcomesReady: false,
    expectsGame: true,
    gameStreamWatermark: null as number | null,
  };
  assert.equal(replacementReadyForPromotion(readiness, 10), false);

  readiness.authenticated = true;
  readiness.lobbyReady = true;
  readiness.gameReady = true;
  readiness.commandOutcomesReady = true;
  readiness.gameStreamWatermark = 10;
  assert.equal(replacementReadyForPromotion(readiness, 11), false);

  readiness.gameStreamWatermark = 11;
  assert.equal(replacementReadyForPromotion(readiness, 11), true);
  readiness.inFlightRequestCount = 1;
  assert.equal(replacementReadyForPromotion(readiness, 11), false);
  readiness.inFlightRequestCount = 0;
  readiness.socketOpen = false;
  assert.equal(replacementReadyForPromotion(readiness, 11), false);
  assert.equal(isCommandOwner(1, 1, 'active', 1), true);
  assert.equal(isCommandOwner(1, 2, 'candidate', 1), false);
  assert.equal(isCommandOwner(2, 1, 'retired', 1), false);
  assert.equal(isCommandOwner(2, 2, 'active', 1), true);
});

test('a half-open candidate is retired at its absolute deadline and cannot suppress crash recovery', () => {
  const deadline = 20_000;
  assert.equal(candidateDeadlineDelayMs(deadline, 15_000), 5_000);
  assert.equal(candidateDeadlineDelayMs(deadline, 20_001), 0);

  const openButUnauthenticated = {
    socketOpen: true,
    authenticated: false,
    inFlightRequestCount: 0,
    lobbyReady: false,
    gameReady: false,
    gameComplete: false,
    commandOutcomesReady: false,
    expectsGame: true,
    gameStreamWatermark: null,
  };
  assert.equal(replacementReadyForPromotion(openButUnauthenticated, 10), false);
  assert.equal(
    replacementFailureAction(false, true, deadline, deadline),
    'reconnect-active',
  );

  const authenticatedButSnapshotStalled = {
    ...openButUnauthenticated,
    authenticated: true,
    lobbyReady: true,
  };
  assert.equal(replacementReadyForPromotion(authenticatedButSnapshotStalled, 10), false);
});

test('planned drain uses synchronized server time when the browser clock is ahead', () => {
  const clientNowMs = 130_000;
  const serverClockOffsetMs = -30_000;
  const serverDeadlineMs = 115_000;

  assert.equal(
    plannedDrainRemainingMs(serverDeadlineMs, clientNowMs, serverClockOffsetMs),
    15_000,
  );
});

test('planned drain falls back conservatively when browser skew is not synchronized', () => {
  const serverDeadlineMs = 115_000;

  // An ahead browser would previously discard this still-valid server notice.
  assert.equal(
    plannedDrainRemainingMs(serverDeadlineMs, 175_000, null),
    PLANNED_HANDOFF_MAX_MS,
  );
  // A far-behind browser cannot stretch the server's bounded handoff window.
  assert.equal(
    plannedDrainRemainingMs(serverDeadlineMs, 50_000, null),
    PLANNED_HANDOFF_MAX_MS,
  );
  // With a plausible local subtraction and no sync yet, preserve that value.
  assert.equal(plannedDrainRemainingMs(serverDeadlineMs, 105_000, null), 10_000);
  assert.equal(plannedDrainRemainingMs(Number.NaN, 105_000, null), null);
});

test('an authoritative completed snapshot can replace a higher live watermark', () => {
  const completedSnapshot = {
    GameEvent: {
      game_id: 42,
      stream_seq: 0,
      event: {
        Snapshot: {
          game_state: { status: { Complete: { winning_snake_id: null } } },
        },
      },
    },
  };
  assert.equal(isTerminalSnapshotForGame(completedSnapshot, 42), true);
  assert.equal(isFreshSnapshotForGame(completedSnapshot, 42, 900), true);
  assert.equal(replacementReadyForPromotion({
    socketOpen: true,
    authenticated: true,
    inFlightRequestCount: 0,
    lobbyReady: true,
    gameReady: true,
    gameComplete: true,
    commandOutcomesReady: true,
    expectsGame: true,
    gameStreamWatermark: 0,
  }, 900), true);
});
