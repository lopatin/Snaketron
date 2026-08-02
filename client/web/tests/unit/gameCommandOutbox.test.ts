import assert from 'node:assert/strict';
import test from 'node:test';

import {
  GameCommandOutbox,
  MAX_PENDING_COMMANDS_PER_GAME_SESSION,
  gameEventTerminatesCommandOutbox,
  gameLoadOutboxAction,
  recoveryOutcomesReadyForResend,
} from '../../services/gameCommandOutbox.ts';
import type { GameCommand } from '../../types/index.ts';

function command(sequenceNumber: number): GameCommand {
  return {
    command_id_client: { tick: 10, user_id: 7, sequence_number: sequenceNumber },
    command_id_server: null,
    command: { Turn: { direction: 'Up' } },
  };
}

test('resends preserve one stable identity and sequence numbers never close gaps', () => {
  const outbox = new GameCommandOutbox(() => 'session-a');
  const first = outbox.enqueue(42, 7, command(99));
  const second = outbox.enqueue(42, 7, command(1));

  assert.deepEqual(first.command_id, {
    game_id: 42,
    user_id: 7,
    client_game_session_id: 'session-a',
    sequence: 1,
  });
  assert.equal(second.command_id.sequence, 2);
  assert.strictEqual(outbox.pending(42, 7)[0], first);
  assert.equal(outbox.resolve(first.command_id), true);

  const third = outbox.enqueue(42, 7, command(1));
  assert.equal(third.command_id.sequence, 3);
  assert.deepEqual(
    outbox.pending(42, 7).map((entry) => entry.command_id.sequence),
    [2, 3],
  );
});

test('only one retry coordinator can claim an overdue exact envelope', () => {
  const outbox = new GameCommandOutbox(() => 'session-retry');
  const entry = outbox.enqueue(42, 7, command(1), 1_000);

  assert.deepEqual(outbox.takeDue(42, 7, 1_999, 1_000), []);
  assert.deepEqual(outbox.takeDue(42, 7, 2_000, 1_000), [entry]);
  assert.deepEqual(outbox.takeDue(42, 7, 2_000, 1_000), []);
});

test('pending commands fail closed at the server recovery bound', () => {
  assert.equal(MAX_PENDING_COMMANDS_PER_GAME_SESSION, 128);
  const outbox = new GameCommandOutbox(() => 'session-bounded');
  for (let index = 0; index < MAX_PENDING_COMMANDS_PER_GAME_SESSION; index += 1) {
    outbox.enqueue(42, 7, command(index));
  }

  assert.throws(
    () => outbox.enqueue(
      42,
      7,
      command(MAX_PENDING_COMMANDS_PER_GAME_SESSION + 1),
    ),
    /pending game command capacity exhausted/,
  );
  assert.equal(outbox.pending(42, 7).length, MAX_PENDING_COMMANDS_PER_GAME_SESSION);

  const first = outbox.pending(42, 7)[0];
  assert.equal(outbox.resolve(first.command_id), true);
  const next = outbox.enqueue(42, 7, command(999));
  assert.equal(next.command_id.sequence, 129);
  assert.equal(outbox.pending(42, 7).length, MAX_PENDING_COMMANDS_PER_GAME_SESSION);
  assert.deepEqual(
    outbox.pending(42, 7).map(({ command_id }) => command_id.sequence),
    Array.from({ length: 128 }, (_, index) => index + 2),
  );
});

test('snapshot reconciliation clears the contiguous watermark and sparse outcomes', () => {
  const outbox = new GameCommandOutbox(() => 'session-b');
  for (let index = 0; index < 4; index += 1) {
    outbox.enqueue(42, 7, command(index));
  }

  const removed = outbox.reconcile(
    {
      game_id: 42,
      client_game_session_id: 'session-b',
      contiguous_through: 1,
      outcomes: {
        '3': { result: 'REJECTED', reason: 'invalid turn' },
      },
    },
    7,
  );

  assert.equal(removed, 2);
  assert.deepEqual(
    outbox.pending(42, 7).map((entry) => entry.command_id.sequence),
    [2, 4],
  );
});

test('a missing low sequence bounds later resolved identities to the server window', () => {
  const outbox = new GameCommandOutbox(() => 'session-sparse-window');
  const missing = outbox.enqueue(42, 7, command(1));

  for (let sequence = 2; sequence <= 129; sequence += 1) {
    const resolved = outbox.enqueue(42, 7, command(sequence));
    assert.equal(outbox.resolve(resolved.command_id), true);
  }
  assert.deepEqual(
    outbox.pending(42, 7).map(({ command_id }) => command_id.sequence),
    [1],
  );
  assert.throws(
    () => outbox.enqueue(42, 7, command(130)),
    /pending game command sequence window exhausted/,
  );

  assert.equal(outbox.resolve(missing.command_id), true);
  assert.equal(outbox.enqueue(42, 7, command(130)).command_id.sequence, 130);
});

test('a higher sparse result never resolves an earlier lost command outcome', () => {
  const outbox = new GameCommandOutbox(() => 'session-gap');
  const rejected = outbox.enqueue(42, 7, command(1));
  const scheduled = outbox.enqueue(42, 7, command(2));

  // Both live terminal messages were lost. Recovery first exposes only N+1;
  // it must not be interpreted as an implicit acceptance of N.
  assert.equal(outbox.reconcile({
    game_id: 42,
    client_game_session_id: 'session-gap',
    contiguous_through: 0,
    outcomes: {
      '2': { result: 'SCHEDULED', command: scheduled.command },
    },
  }, 7), 1);
  assert.deepEqual(outbox.pending(42, 7), [rejected]);

  // N is retired only when its own recovered rejection arrives.
  assert.equal(outbox.reconcile({
    game_id: 42,
    client_game_session_id: 'session-gap',
    contiguous_through: 0,
    outcomes: {
      '1': { result: 'REJECTED', reason: 'invalid turn' },
    },
  }, 7), 1);
  assert.deepEqual(outbox.pending(42, 7), []);
});

test('a recovery fence rejects its range and rotates only after lower gaps drain', () => {
  const sessionIds = ['session-fenced', 'session-after-fence'];
  const outbox = new GameCommandOutbox(() => sessionIds.shift()!);
  const first = outbox.enqueue(42, 7, command(1), 1_000);
  const second = outbox.enqueue(42, 7, command(2), 1_000);
  const third = outbox.enqueue(42, 7, command(3), 1_000);
  outbox.enqueue(42, 7, command(4), 1_000);

  const removed = outbox.reconcile({
    game_id: 42,
    client_game_session_id: 'session-fenced',
    contiguous_through: 1,
    outcomes: {
      '3': { result: 'SCHEDULED', command: third.command },
    },
    rejection_fence: {
      from_sequence: 3,
      reason: 'command session sparse outcome capacity exhausted',
    },
  }, 7);

  // The watermark and exact outcome win first; the fence then rejects only
  // still-unresolved entries at or above its boundary.
  assert.equal(removed, 3);
  assert.deepEqual(outbox.pending(42, 7), [second]);
  assert.deepEqual(outbox.takeDue(42, 7, 2_000, 1_000), [second]);
  assert.throws(
    () => outbox.enqueue(42, 7, command(5), 2_000),
    /client game command session rejected/,
  );

  assert.equal(outbox.resolve(first.command_id), false);
  assert.equal(outbox.resolve(second.command_id), true);
  const fresh = outbox.enqueue(42, 7, command(5), 2_000);
  assert.equal(fresh.command_id.client_game_session_id, 'session-after-fence');
  assert.equal(fresh.command_id.sequence, 1);
});

test('a live rejection fence resolves the exact event before fencing its session', () => {
  const sessionIds = ['session-live-fence', 'session-live-fresh'];
  const outbox = new GameCommandOutbox(() => sessionIds.shift()!);
  const lowerGap = outbox.enqueue(42, 7, command(1), 1_000);
  const exactRejection = outbox.enqueue(42, 7, command(2), 1_000);
  outbox.enqueue(42, 7, command(3), 1_000);

  assert.equal(
    outbox.reject(
      exactRejection.command_id,
      2,
    ),
    2,
  );
  assert.deepEqual(outbox.pending(42, 7), [lowerGap]);
  assert.throws(
    () => outbox.enqueue(42, 7, command(4)),
    /client game command session rejected/,
  );

  assert.equal(outbox.resolve(lowerGap.command_id), true);
  const fresh = outbox.enqueue(42, 7, command(4));
  assert.equal(fresh.command_id.client_game_session_id, 'session-live-fresh');
  assert.equal(fresh.command_id.sequence, 1);
});

test('another user or browser game session cannot resolve this outbox', () => {
  const outbox = new GameCommandOutbox(() => 'session-c');
  const entry = outbox.enqueue(42, 7, command(1));

  assert.equal(outbox.resolve({ ...entry.command_id, user_id: 8 }), false);
  assert.equal(
    outbox.reconcile(
      {
        game_id: 42,
        client_game_session_id: 'other-session',
        contiguous_through: 999,
        outcomes: {},
        rejection_fence: {
          from_sequence: 1,
          reason: 'stale session fence',
        },
      },
      7,
    ),
    0,
  );
  assert.equal(
    outbox.reject(
      { ...entry.command_id, client_game_session_id: 'other-session' },
      1,
    ),
    0,
  );
  assert.deepEqual(outbox.pending(42, 7), [entry]);
});

test('a delayed outcome barrier keeps recovery resends parked after the snapshot', () => {
  const capabilities = new Set(['command-delivery-v2', 'command-outcome-barrier-v1']);
  const completed = new Set<number>();

  assert.equal(recoveryOutcomesReadyForResend(42, false, capabilities, completed), false);
  assert.equal(recoveryOutcomesReadyForResend(42, true, capabilities, completed), false);

  completed.add(41);
  assert.equal(recoveryOutcomesReadyForResend(42, true, capabilities, completed), false);

  completed.add(42);
  assert.equal(recoveryOutcomesReadyForResend(42, true, capabilities, completed), true);

  // Missing capability fails closed; current client/server deploy together.
  assert.equal(
    recoveryOutcomesReadyForResend(42, true, new Set(['command-delivery-v2']), new Set()),
    false,
  );
});

test('only authoritative completed game events terminate the command outbox', () => {
  assert.equal(
    gameEventTerminatesCommandOutbox({
      Snapshot: {
        game_state: { status: { Complete: { winning_snake_id: null } } },
      },
    }),
    true,
  );
  assert.equal(
    gameEventTerminatesCommandOutbox({
      StatusUpdated: { status: { Complete: { winning_snake_id: 7 } } },
    }),
    true,
  );
  assert.equal(
    gameEventTerminatesCommandOutbox({
      Snapshot: { game_state: { status: { Started: { server_id: 3 } } } },
    }),
    false,
  );
  assert.equal(gameEventTerminatesCommandOutbox({ TickHash: {} }), false);
});

test('terminal state parks pending commands until outcomes and the following barrier', () => {
  const outbox = new GameCommandOutbox(() => 'session-terminal');
  const first = outbox.enqueue(42, 7, command(1), 1_000);
  const second = outbox.enqueue(42, 7, command(2), 1_000);

  assert.equal(outbox.markTerminal(42, 7), true);
  assert.deepEqual(outbox.pending(42, 7), [first, second]);
  assert.deepEqual(outbox.takeDue(42, 7, 10_000, 1_000), []);
  assert.throws(
    () => outbox.enqueue(42, 7, command(3), 10_000),
    /terminal game is awaiting command outcomes/,
  );
  assert.equal(outbox.completeTerminal(42, 7), 'pending');
  assert.deepEqual(outbox.pending(42, 7), [first, second]);

  assert.equal(outbox.reconcile({
    game_id: 42,
    client_game_session_id: 'session-terminal',
    contiguous_through: 1,
    outcomes: {
      '2': { result: 'REJECTED', reason: 'game complete' },
    },
  }, 7), 2);
  assert.deepEqual(outbox.pending(42, 7), []);

  assert.equal(outbox.completeTerminal(42, 7), 'cleared');
  assert.deepEqual(outbox.pending(42, 7), []);
  assert.equal(outbox.completeTerminal(42, 7), 'cleared');
  assert.throws(
    () => outbox.enqueue(42, 7, command(3), 10_000),
    /terminal game is awaiting command outcomes/,
  );
});

test('terminal state before the first command keeps a tombstone through its barrier', () => {
  const sessionIds = ['terminal-tombstone', 'session-after-terminal'];
  const outbox = new GameCommandOutbox(() => sessionIds.shift()!);

  assert.equal(outbox.markTerminal(42, 7), true);
  assert.deepEqual(outbox.pending(42, 7), []);
  assert.deepEqual(outbox.takeDue(42, 7, 10_000, 0), []);
  assert.throws(
    () => outbox.enqueue(42, 7, command(1), 10_000),
    /terminal game is awaiting command outcomes/,
  );

  assert.equal(outbox.completeTerminal(42, 7), 'cleared');
  assert.throws(
    () => outbox.enqueue(42, 7, command(1), 10_000),
    /terminal game is awaiting command outcomes/,
  );
  outbox.clear(42, 7);
  const next = outbox.enqueue(42, 7, command(1), 10_000);
  assert.equal(next.command_id.client_game_session_id, 'session-after-terminal');
  assert.equal(next.command_id.sequence, 1);
});

test('terminal barrier explicitly rejects identities that crossed completion', () => {
  const outbox = new GameCommandOutbox(() => 'session-terminal-cutoff');
  outbox.enqueue(42, 7, command(1), 1_000);
  outbox.enqueue(42, 7, command(2), 1_100);

  outbox.markTerminal(42, 7);
  assert.equal(outbox.completeTerminal(42, 7), 'pending');
  assert.equal(
    outbox.completeTerminal(42, 7, 'game completed'),
    'cleared',
  );
  assert.deepEqual(outbox.pending(42, 7), []);
  assert.throws(
    () => outbox.enqueue(42, 7, command(3), 1_200),
    /terminal game is awaiting command outcomes/,
  );
});

test('terminal recovery fence remains parked until its completion barrier', () => {
  const sessionIds = ['session-terminal-fence', 'session-after-terminal'];
  const outbox = new GameCommandOutbox(() => sessionIds.shift()!);
  outbox.enqueue(42, 7, command(1), 1_000);

  assert.equal(outbox.markTerminal(42, 7), true);
  assert.equal(outbox.reconcile({
    game_id: 42,
    client_game_session_id: 'session-terminal-fence',
    contiguous_through: 0,
    outcomes: {},
    rejection_fence: {
      from_sequence: 1,
      reason: 'command session sparse outcome capacity exhausted',
    },
  }, 7), 1);
  assert.deepEqual(outbox.pending(42, 7), []);
  assert.throws(
    () => outbox.enqueue(42, 7, command(2), 2_000),
    /terminal game is awaiting command outcomes/,
  );

  assert.equal(outbox.completeTerminal(42, 7), 'cleared');
  assert.throws(
    () => outbox.enqueue(42, 7, command(2), 2_000),
    /terminal game is awaiting command outcomes/,
  );
  outbox.clear(42, 7);
  const fresh = outbox.enqueue(42, 7, command(2), 2_000);
  assert.equal(fresh.command_id.client_game_session_id, 'session-after-terminal');
});

test('only a definitive failure for the active game clears its outbox', () => {
  const outbox = new GameCommandOutbox(() => 'session-load');
  const pending = outbox.enqueue(42, 7, command(1), 1_000);
  const applyLoadResult = (messageType: 'GameLoadFailed' | 'GameWarming', gameId: number) => {
    if (gameLoadOutboxAction(messageType, gameId, 42) === 'clear-terminal') {
      outbox.clear(gameId, 7);
    }
  };

  assert.equal(gameLoadOutboxAction('GameLoadFailed', 42, 42), 'clear-terminal');
  assert.equal(gameLoadOutboxAction('GameWarming', 42, 42), 'preserve-and-retry');
  assert.equal(gameLoadOutboxAction('GameLoadFailed', 41, 42), 'ignore');
  assert.equal(gameLoadOutboxAction('GameWarming', 41, 42), 'ignore');
  assert.equal(gameLoadOutboxAction('GameLoadFailed', 42, null), 'ignore');

  // A transient warm-up and a stale terminal response both leave the active
  // game's exact command envelope available for retry.
  applyLoadResult('GameWarming', 42);
  applyLoadResult('GameLoadFailed', 41);
  assert.deepEqual(outbox.pending(42, 7), [pending]);

  applyLoadResult('GameLoadFailed', 42);
  assert.deepEqual(outbox.pending(42, 7), []);
});

test('clearing a terminal game prevents retries and starts a fresh session', () => {
  const sessionIds = ['session-before-complete', 'session-after-complete'];
  const outbox = new GameCommandOutbox(() => sessionIds.shift()!);
  outbox.enqueue(42, 7, command(1), 1_000);

  outbox.clear(42, 7);

  assert.deepEqual(outbox.takeDue(42, 7, 10_000, 1_000), []);
  const next = outbox.enqueue(42, 7, command(2), 10_000);
  assert.equal(next.command_id.client_game_session_id, 'session-after-complete');
  assert.equal(next.command_id.sequence, 1);
});
