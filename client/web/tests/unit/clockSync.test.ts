import assert from 'node:assert/strict';
import test from 'node:test';

import { ClockSync } from '../../utils/clockSync.ts';

test('clock offset is explicitly server-minus-client', () => {
  const sync = new ClockSync();

  // Client midpoint is 1,020; server timestamp is 1,120.
  sync.processSyncResponse(1_000, 1_120, 1_040);
  assert.equal(sync.getServerClockOffsetMs(), 100);
});

test('start, reset, and stop own at most one timeout chain', () => {
  let nextTimeout = 1;
  const scheduled = new Map<number, { callback: () => void; delayMs: number }>();
  const requests: number[] = [];
  const sync = new ClockSync({
    now: () => 1_000 + requests.length,
    scheduleTimeout: (callback, delayMs) => {
      const handle = nextTimeout++;
      scheduled.set(handle, { callback, delayMs });
      return handle;
    },
    cancelTimeout: (handle) => {
      scheduled.delete(handle as number);
    },
  });
  sync.setOnSyncRequest((clientTime) => requests.push(clientTime));

  sync.start();
  assert.equal(requests.length, 1);
  assert.equal(scheduled.size, 1);

  // Starting for a replacement active socket cancels the old chain first.
  sync.start();
  assert.equal(requests.length, 2);
  assert.equal(scheduled.size, 1);

  const [handle, timer] = [...scheduled.entries()][0];
  scheduled.delete(handle);
  timer.callback();
  assert.equal(requests.length, 3);
  assert.equal(scheduled.size, 1);

  sync.reset();
  assert.equal(scheduled.size, 0);
  sync.start();
  assert.equal(scheduled.size, 1);
  sync.stop();
  assert.equal(scheduled.size, 0);
});
