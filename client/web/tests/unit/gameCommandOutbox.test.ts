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
  const outbox = new GameCommandOutbox(() => 'session-bounded');
  for (let index = 0; index < MAX_PENDING_COMMANDS_PER_GAME_SESSION; index += 1) {
    outbox.enqueue(42, 7, command(index));
  }

  assert.throws(
    () => outbox.enqueue(42, 7, command(513)),
    /pending game command capacity exhausted/,
  );
  assert.equal(outbox.pending(42, 7).length, MAX_PENDING_COMMANDS_PER_GAME_SESSION);
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
      },
      7,
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
