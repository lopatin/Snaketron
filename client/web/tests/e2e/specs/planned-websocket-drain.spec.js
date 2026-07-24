const { test, expect } = require('@playwright/test');

const REQUIRED_CAPABILITIES = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
];

const gameState = (tick = 5) => ({
  tick,
  status: { Started: { server_id: 1 } },
  arena: {
    width: 10,
    height: 10,
    snakes: [
      {
        body: [{ x: 5, y: 5 }, { x: 4, y: 5 }],
        direction: 'Right',
        is_alive: true,
        food: 0,
        team_id: null,
      },
    ],
    food: [],
    team_zone_config: null,
  },
  game_type: 'Solo',
  queue_mode: 'Quickmatch',
  properties: {
    available_food_target: 1,
    tick_duration_ms: 100,
    time_limit_ms: null,
  },
  command_queue: {
    queue: [],
    active_ids: [],
    tombstone_ids: [],
  },
  players: { 7: { user_id: 7, snake_id: 0 } },
  rng: { state: 123 },
  game_code: null,
  host_user_id: null,
  start_ms: Date.now() + 10_000,
  event_sequence: 1,
  usernames: { 7: 'drain-tester' },
  spectators: [],
  scores: { 0: 0 },
  team_scores: null,
  player_xp: { 7: 0 },
});

const snapshot = (streamSequence, tick = 5) => ({
  GameEvent: {
    game_id: 42,
    tick,
    sequence: 1,
    stream_seq: streamSequence,
    user_id: 7,
    event: { Snapshot: { game_state: gameState(tick) } },
  },
});

const lobbyUpdate = {
  LobbyUpdate: {
    lobby_id: 1,
    lobby_code: 'LOBBY1',
    members: [{ user_id: 7, username: 'drain-tester', ts: Date.now() }],
    host_user_id: 7,
    state: 'waiting',
    preferences: { selected_modes: ['solo'], competitive: false },
  },
};

async function emitServerMessage(page, socketIndex, message) {
  await page.evaluate(({ socketIndex, message }) => {
    window.__mockSockets[socketIndex].serverMessage(message);
  }, { socketIndex, message });
}

async function emitPongThenQueuedOldMessage(page, socketIndex, pong, queuedMessage) {
  await page.evaluate(({ socketIndex, pong, queuedMessage }) => {
    const socket = window.__mockSockets[socketIndex];
    const alreadyQueuedHandler = socket.onmessage;
    socket.serverMessage(pong);
    alreadyQueuedHandler?.({ data: JSON.stringify(queuedMessage) });
  }, { socketIndex, pong, queuedMessage });
}

async function sendCommandProbe(page, probe) {
  await page.evaluate((value) => {
    window.__wsContext.sendMessage({ GameCommandV2: { probe: value } });
  }, probe);
}

async function socketMessages(page, socketIndex, messageType) {
  return page.evaluate(({ socketIndex, messageType }) => (
    window.__mockSockets[socketIndex].sent
      .map((raw) => JSON.parse(raw))
      .filter((message) => Object.prototype.hasOwnProperty.call(message, messageType))
  ), { socketIndex, messageType });
}

async function continuityPings(page, socketIndex) {
  return (await socketMessages(page, socketIndex, 'Ping'))
    .filter((message) => Number(message.Ping.client_time) < 0);
}

async function confirmContinuityProbe(page, oldSocketIndex) {
  await expect.poll(() => continuityPings(page, oldSocketIndex)).toHaveLength(1);
  const [{ Ping: { client_time: clientTime } }] = await continuityPings(page, oldSocketIndex);
  await emitServerMessage(page, oldSocketIndex, {
    Pong: { client_time: clientTime, server_time: Date.now() },
  });
  return clientTime;
}

async function expectOldSocketUsableWithoutOverlay(page, oldSocketIndex) {
  await expect.poll(() => page.evaluate((index) => ({
    activeSocket: window.__mockSockets.indexOf(window.__wsInstance),
    oldReadyState: window.__mockSockets[index].readyState,
    oldCloseCount: window.__mockSockets[index].closeCalls.length,
    connected: window.__wsContext?.isConnected,
    authenticated: window.__wsContext?.isSessionAuthenticated,
    disconnectedBanner: document.body.innerText.includes('Connecting to game server'),
    staleOverlay: document.body.innerText.includes('CONNECTION LOST'),
    snapshotLoading: Boolean(document.querySelector('[data-testid="game-snapshot-loading"]')),
  }), oldSocketIndex)).toEqual({
    activeSocket: oldSocketIndex,
    oldReadyState: 1,
    oldCloseCount: 0,
    connected: true,
    authenticated: true,
    disconnectedBanner: false,
    staleOverlay: false,
    snapshotLoading: false,
  });
}

async function establishActiveGame(page) {
  await page.goto('/play/42');
  await expect.poll(() => page.evaluate(() => (
    window.__wsInstance ? window.__mockSockets.indexOf(window.__wsInstance) : -1
  ))).toBeGreaterThanOrEqual(0);
  const oldSocketIndex = await page.evaluate(() => window.__mockSockets.indexOf(window.__wsInstance));
  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets[index].readyState
  ), oldSocketIndex)).toBe(1);
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'Token')).toHaveLength(1);
  await emitServerMessage(page, oldSocketIndex, {
    Authenticated: {
      task_boot_id: 'old-task',
      protocol_version: 2,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 1,
    },
  });
  await expect.poll(() => page.evaluate(() => (
    window.__wsContext?.isConnected && window.__wsContext?.isSessionAuthenticated
  ))).toBe(true);
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'JoinLobby')).not.toHaveLength(0);
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'JoinGame')).not.toHaveLength(0);
  // The JoinGame send and the GameArena message-handler effect are separate
  // React commits. Yield once so the initial snapshot cannot outrun the
  // consumer registration in a fast headless browser.
  await page.waitForTimeout(100);
  await emitServerMessage(page, oldSocketIndex, lobbyUpdate);
  await emitServerMessage(page, oldSocketIndex, snapshot(10, 5));
  await emitServerMessage(page, oldSocketIndex, { CommandOutcomesComplete: { game_id: 42 } });
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
  await page.evaluate(() => {
    window.__drainGameEvents = [{ tick: 5, streamSequence: 10 }];
    window.__drainEventUnsubscribe = window.__wsContext.onMessage('GameEvent', (message) => {
      window.__drainGameEvents.push({
        tick: message.data.tick,
        streamSequence: message.data.stream_seq,
      });
    });
  });
  return oldSocketIndex;
}

async function beginDrain(page, oldSocketIndex, { autoOpen = true, deadlineMs = 15_000 } = {}) {
  // Candidate opening is explicit so its handlers are certainly attached
  // before the mock backend fires `open`.
  await page.evaluate(() => {
    window.__autoOpenSockets = false;
  });
  const socketCountBeforeDrain = await page.evaluate(() => window.__mockSockets.length);
  await emitServerMessage(page, oldSocketIndex, {
    Drain: { task_boot_id: 'old-task', deadline_unix_ms: Date.now() + deadlineMs },
  });
  await expect.poll(() => page.evaluate(() => window.__mockSockets.length))
    .toBeGreaterThan(socketCountBeforeDrain);
  const candidateSocketIndex = socketCountBeforeDrain;
  expect(candidateSocketIndex).toBeGreaterThanOrEqual(0);
  if (autoOpen) {
    await page.evaluate((index) => {
      window.__mockSockets[index].serverOpen();
    }, candidateSocketIndex);
    await expect.poll(() => page.evaluate((index) => (
      window.__mockSockets[index].readyState
    ), candidateSocketIndex)).toBe(1);
    await expect.poll(() => socketMessages(page, candidateSocketIndex, 'Token')).toHaveLength(1);
  } else {
    await expect.poll(() => page.evaluate((index) => (
      window.__mockSockets[index].readyState
    ), candidateSocketIndex)).toBe(0);
  }
  return candidateSocketIndex;
}

async function authenticateCandidate(page, candidateSocketIndex) {
  await emitServerMessage(page, candidateSocketIndex, {
    Authenticated: {
      task_boot_id: 'new-task',
      protocol_version: 2,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 2,
    },
  });
  await expect.poll(() => socketMessages(page, candidateSocketIndex, 'JoinLobby')).toHaveLength(1);
  await expect.poll(() => socketMessages(page, candidateSocketIndex, 'JoinGame')).toHaveLength(1);
}

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('token', 'drain-test-token');
    localStorage.setItem('snaketron:lastLobby', JSON.stringify({ id: 1, code: 'LOBBY1' }));
    localStorage.setItem('snaketron_selected_region', JSON.stringify({
      regionId: 'test-region',
      wsUrl: 'ws://snaketron.test/ws',
      origin: 'http://snaketron.test',
      timestamp: Date.now(),
    }));

    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = typeof input === 'string'
        ? input
        : (input instanceof URL ? input.href : input.url);
      let payload;
      if (url.endsWith('/client_bg.wasm')) {
        return nativeFetch(input, init);
      } else if (url.endsWith('/api/auth/me')) {
        payload = { id: 7, username: 'drain-tester', mmr: 1000, isGuest: false };
      } else if (url.endsWith('/api/regions')) {
        payload = [{
          id: 'test-region',
          name: 'Test Region',
          origin: 'http://snaketron.test',
          ws_url: 'ws://snaketron.test/ws',
        }];
      } else if (url.endsWith('/api/regions/user-counts')) {
        payload = { 'test-region': 1 };
      } else if (url === 'http://snaketron.test/api/health') {
        payload = { status: 'ok' };
      } else {
        throw new Error(`Unexpected fetch in planned-drain test: ${url} (${init?.method || 'GET'})`);
      }
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };

    class MockWebSocket {
      static CONNECTING = 0;
      static OPEN = 1;
      static CLOSING = 2;
      static CLOSED = 3;

      constructor(url) {
        this.url = url;
        this.readyState = MockWebSocket.CONNECTING;
        this.sent = [];
        this.closeCalls = [];
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        this.onclose = null;
        window.__mockSockets.push(this);
        queueMicrotask(() => {
          if (window.__autoOpenSockets) this.serverOpen();
        });
      }

      serverOpen() {
        if (this.readyState !== MockWebSocket.CONNECTING) return;
        this.readyState = MockWebSocket.OPEN;
        this.onopen?.(new Event('open'));
      }

      send(data) {
        if (this.readyState !== MockWebSocket.OPEN) {
          throw new Error('send on non-open mock WebSocket');
        }
        this.sent.push(String(data));
      }

      close(code = 1000, reason = '') {
        if (this.readyState === MockWebSocket.CLOSED) return;
        this.closeCalls.push({ code, reason });
        this.readyState = MockWebSocket.CLOSED;
        this.onclose?.({ code, reason, wasClean: true });
      }

      serverMessage(message) {
        if (this.readyState !== MockWebSocket.OPEN) {
          throw new Error('server message on non-open mock WebSocket');
        }
        this.onmessage?.({ data: JSON.stringify(message) });
      }

      serverClose(code = 1012, reason = 'mock backend closed') {
        if (this.readyState === MockWebSocket.CLOSED) return;
        this.readyState = MockWebSocket.CLOSED;
        this.onclose?.({ code, reason, wasClean: false });
      }
    }

    window.__mockSockets = [];
    window.__autoOpenSockets = true;
    window.WebSocket = MockWebSocket;
  });
});

test('planned drain keeps the old game socket usable until the replacement is fully ready', async ({ page }) => {
  const oldSocketIndex = await establishActiveGame(page);
  await sendCommandProbe(page, 'before-drain');
  const candidateSocketIndex = await beginDrain(page, oldSocketIndex);
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'candidate-open');

  await authenticateCandidate(page, candidateSocketIndex);
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'candidate-authenticated');

  await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'candidate-lobby-ready');

  // Keep the replacement incomplete beyond the client's stale watchdog while
  // proving that inbound authoritative traffic still flows through the old
  // socket. This is the realistic make-before-break interval the fast mock
  // previously skipped.
  for (const [streamSequence, tick] of [[11, 6], [12, 7], [13, 8]]) {
    await page.waitForTimeout(1_100);
    await emitServerMessage(page, oldSocketIndex, snapshot(streamSequence, tick));
    await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  }
  await expect.poll(() => page.evaluate(() => window.__drainGameEvents)).toEqual([
    { tick: 5, streamSequence: 10 },
    { tick: 6, streamSequence: 11 },
    { tick: 7, streamSequence: 12 },
    { tick: 8, streamSequence: 13 },
  ]);

  // A recovery-envelope bridge can precede the live replica snapshot and uses
  // stream_seq 0. It must not be replayed over newer old-socket state when the
  // candidate is eventually promoted.
  await emitServerMessage(page, candidateSocketIndex, snapshot(0, 4));
  await emitServerMessage(page, candidateSocketIndex, snapshot(14, 9));
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'candidate-snapshot-ready');
  expect(await page.evaluate(() => window.__drainGameEvents.at(-1))).toEqual({
    tick: 8,
    streamSequence: 13,
  });

  const oldCommandsBeforeBarrier = await socketMessages(page, oldSocketIndex, 'GameCommandV2');
  const candidateCommandsBeforeBarrier = await socketMessages(page, candidateSocketIndex, 'GameCommandV2');
  expect(oldCommandsBeforeBarrier.map((message) => message.GameCommandV2.probe)).toEqual([
    'before-drain',
    'candidate-open',
    'candidate-authenticated',
    'candidate-lobby-ready',
    'candidate-snapshot-ready',
  ]);
  expect(candidateCommandsBeforeBarrier).toEqual([]);

  await emitServerMessage(page, candidateSocketIndex, { CommandOutcomesComplete: { game_id: 42 } });
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await expect.poll(() => continuityPings(page, oldSocketIndex)).toHaveLength(1);
  expect(await page.evaluate(() => window.__drainGameEvents.at(-1))).toEqual({
    tick: 8,
    streamSequence: 13,
  });

  const [{ Ping: { client_time: continuityClientTime } }] = await continuityPings(
    page,
    oldSocketIndex,
  );
  await emitServerMessage(page, oldSocketIndex, {
    Pong: { client_time: continuityClientTime - 1, server_time: Date.now() },
  });
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  expect(await continuityPings(page, oldSocketIndex)).toHaveLength(1);

  // The candidate was ready through 14 when the probe was sent. The ordered
  // old stream advances through 15 before the matching pong, so 15 becomes
  // the fixed promotion frontier.
  await emitServerMessage(page, oldSocketIndex, snapshot(14, 9));
  await emitServerMessage(page, oldSocketIndex, snapshot(15, 10));
  expect(await continuityPings(page, oldSocketIndex)).toHaveLength(1);
  await emitPongThenQueuedOldMessage(
    page,
    oldSocketIndex,
    { Pong: { client_time: continuityClientTime, server_time: Date.now() } },
    snapshot(16, 11),
  );
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'post-pong-catch-up');
  await expect.poll(() => page.evaluate(() => window.__drainGameEvents.at(-1))).toEqual({
    tick: 11,
    streamSequence: 16,
  });

  // The candidate snapshot catches the frozen frontier, but it invalidates the
  // outcome barrier observed for the earlier recovery envelope. The old socket
  // and its state remain authoritative until the paired barrier arrives.
  await emitServerMessage(page, candidateSocketIndex, snapshot(15, 10));
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await sendCommandProbe(page, 'takeover-snapshot-awaiting-barrier');
  expect(await page.evaluate(() => window.__drainGameEvents)).toEqual([
    { tick: 5, streamSequence: 10 },
    { tick: 6, streamSequence: 11 },
    { tick: 7, streamSequence: 12 },
    { tick: 8, streamSequence: 13 },
    { tick: 9, streamSequence: 14 },
    { tick: 10, streamSequence: 15 },
    { tick: 11, streamSequence: 16 },
  ]);

  await emitServerMessage(page, candidateSocketIndex, {
    CommandOutcomesComplete: { game_id: 42 },
  });
  await expect.poll(() => page.evaluate(({ oldSocketIndex, candidateSocketIndex }) => ({
    activeSocket: window.__mockSockets.indexOf(window.__wsInstance),
    oldReadyState: window.__mockSockets[oldSocketIndex].readyState,
    oldCloseReasons: window.__mockSockets[oldSocketIndex].closeCalls.map((call) => call.reason),
    candidateReadyState: window.__mockSockets[candidateSocketIndex].readyState,
    connected: window.__wsContext?.isConnected,
    authenticated: window.__wsContext?.isSessionAuthenticated,
  }), { oldSocketIndex, candidateSocketIndex })).toEqual({
    activeSocket: candidateSocketIndex,
    oldReadyState: 3,
    oldCloseReasons: ['planned gateway handoff complete'],
    candidateReadyState: 1,
    connected: true,
    authenticated: true,
  });
  // Promotion discards the bridge and both buffered live candidate snapshots
  // because the old application stream already made 16 visible. No rollback
  // to stream 0, 14, or 15 is replayed.
  expect(await page.evaluate(() => window.__drainGameEvents)).toEqual([
    { tick: 5, streamSequence: 10 },
    { tick: 6, streamSequence: 11 },
    { tick: 7, streamSequence: 12 },
    { tick: 8, streamSequence: 13 },
    { tick: 9, streamSequence: 14 },
    { tick: 10, streamSequence: 15 },
    { tick: 11, streamSequence: 16 },
  ]);

  // Frames already covered by the old transport can arrive after the atomic
  // socket swap, not just in the candidate's initial buffer. Neither a delayed
  // live snapshot nor a stream-zero recovery bridge may roll visible state
  // backward while the promoted stream catches the old watermark.
  await emitServerMessage(page, candidateSocketIndex, snapshot(15, 10));
  await emitServerMessage(page, candidateSocketIndex, snapshot(0, 4));
  await page.waitForTimeout(50);
  expect(await page.evaluate(() => window.__drainGameEvents.at(-1))).toEqual({
    tick: 11,
    streamSequence: 16,
  });

  // Once the promoted transport advances beyond that floor, a later live
  // snapshot remains eligible to re-anchor ordinary crash recovery.
  await emitServerMessage(page, candidateSocketIndex, snapshot(17, 12));
  await expect.poll(() => page.evaluate(() => window.__drainGameEvents.at(-1))).toEqual({
    tick: 12,
    streamSequence: 17,
  });

  await sendCommandProbe(page, 'after-promotion');
  const oldCommandsAfterPromotion = await socketMessages(page, oldSocketIndex, 'GameCommandV2');
  const candidateCommandsAfterPromotion = await socketMessages(page, candidateSocketIndex, 'GameCommandV2');
  expect(oldCommandsAfterPromotion.map((message) => message.GameCommandV2.probe)).toEqual([
    'before-drain',
    'candidate-open',
    'candidate-authenticated',
    'candidate-lobby-ready',
    'candidate-snapshot-ready',
    'post-pong-catch-up',
    'takeover-snapshot-awaiting-barrier',
  ]);
  expect(candidateCommandsAfterPromotion.map((message) => message.GameCommandV2.probe)).toEqual([
    'after-promotion',
  ]);
  await expect(page.getByText('Connecting to game server…')).toHaveCount(0);
  await expect(page.getByText('CONNECTION LOST — RESYNCING')).toHaveCount(0);
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
});

test('a rebuilt game client anchors the snapshot stream sequence before its first delta', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);

  // The snapshot is stream_seq 10. Delivering 12 next must be recognized as
  // a gap and request a resync. If the web glue rebuilds from only game_state
  // and discards the Snapshot envelope, 12 is incorrectly accepted as the
  // first observed sequence and this assertion times out.
  await emitServerMessage(page, socketIndex, {
    GameEvent: {
      game_id: 42,
      tick: 5,
      sequence: 2,
      stream_seq: 12,
      user_id: null,
      event: { TickHash: { hash: 0, server_ts_ms: Date.now() } },
    },
  });

  await expect.poll(
    () => socketMessages(page, socketIndex, 'RequestResync'),
    { timeout: 1_500 },
  ).toHaveLength(1);
});

test('an unsynchronized browser clock ahead of the server still starts planned handoff', async ({ page }) => {
  await page.addInitScript(() => {
    const systemNow = Date.now.bind(Date);
    Date.now = () => systemNow() + 60_000;
  });
  const oldSocketIndex = await establishActiveGame(page);

  // The mock server deadline uses the host clock. From the page's unsynced
  // clock it is already 45 seconds in the past, but the notice must still
  // create a replacement socket using the bounded fallback window.
  const candidateSocketIndex = await beginDrain(page, oldSocketIndex);
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  expect(candidateSocketIndex).toBeGreaterThan(oldSocketIndex);
});

test('a warming replacement retries the game on the same authenticated socket', async ({ page }) => {
  const oldSocketIndex = await establishActiveGame(page);
  const candidateSocketIndex = await beginDrain(page, oldSocketIndex);
  await authenticateCandidate(page, candidateSocketIndex);
  await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);

  expect(await socketMessages(page, candidateSocketIndex, 'Token')).toHaveLength(1);
  expect(await socketMessages(page, candidateSocketIndex, 'JoinGame')).toHaveLength(1);
  const socketCount = await page.evaluate(() => window.__mockSockets.length);

  await emitServerMessage(page, candidateSocketIndex, {
    GameWarming: { game_id: 42, retry_after_ms: 100 },
  });
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await expect.poll(() => socketMessages(page, candidateSocketIndex, 'JoinGame'))
    .toHaveLength(2);
  expect(await page.evaluate(() => window.__mockSockets.length)).toBe(socketCount);
  expect(await page.evaluate((index) => ({
    readyState: window.__mockSockets[index].readyState,
    closeCount: window.__mockSockets[index].closeCalls.length,
  }), candidateSocketIndex)).toEqual({ readyState: 1, closeCount: 0 });
  expect(await socketMessages(page, candidateSocketIndex, 'Token')).toHaveLength(1);

  await emitServerMessage(page, candidateSocketIndex, snapshot(10, 6));
  await emitServerMessage(page, candidateSocketIndex, {
    CommandOutcomesComplete: { game_id: 42 },
  });
  await confirmContinuityProbe(page, oldSocketIndex);
  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets.indexOf(window.__wsInstance) === index
  ), candidateSocketIndex)).toBe(true);
});

test('a command with an ambiguous crash send is retried once with its stable identity', async ({ page }) => {
  const oldSocketIndex = await establishActiveGame(page);

  // Exercise GameArena -> useGameEngine -> useGameWebSocket -> the real
  // browser outbox. A raw WebSocketContext probe would bypass the identity
  // and retry behavior this test is meant to certify.
  await page.keyboard.press('ArrowUp');
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  const firstSend = (await socketMessages(page, oldSocketIndex, 'GameCommandV2'))[0]
    .GameCommandV2;
  expect(firstSend.command_id).toMatchObject({
    game_id: 42,
    user_id: 7,
    sequence: 1,
  });
  expect(firstSend.command_id.client_game_session_id).toEqual(expect.any(String));

  await page.evaluate(() => {
    window.__terminalCommandOutcomes = [];
    window.__terminalCommandOutcomeUnsubscribe = window.__wsContext.onMessage(
      'GameEvent',
      (message) => {
        const event = message.data?.event;
        const terminal = event?.CommandScheduledV2 ?? event?.CommandRejected;
        if (terminal?.command_id) {
          window.__terminalCommandOutcomes.push(terminal.command_id);
        }
      },
    );
  });

  const socketCountBeforeCrash = await page.evaluate(() => window.__mockSockets.length);
  await page.evaluate((index) => {
    window.__mockSockets[index].serverClose(1012, 'executor gateway crashed');
  }, oldSocketIndex);

  await expect.poll(() => page.evaluate(() => window.__mockSockets.length))
    .toBeGreaterThan(socketCountBeforeCrash);
  const replacementSocketIndex = socketCountBeforeCrash;
  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets[index].readyState
  ), replacementSocketIndex)).toBe(1);
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'Token')).toHaveLength(1);

  await emitServerMessage(page, replacementSocketIndex, {
    Authenticated: {
      task_boot_id: 'replacement-after-crash',
      protocol_version: 2,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 2,
    },
  });
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'JoinLobby'))
    .toHaveLength(1);
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'JoinGame'))
    .toHaveLength(1);

  await emitServerMessage(page, replacementSocketIndex, lobbyUpdate);
  await emitServerMessage(page, replacementSocketIndex, snapshot(11, 6));
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
  expect(await socketMessages(page, replacementSocketIndex, 'GameCommandV2')).toEqual([]);

  // No recovered outcome exists, so the explicit barrier makes the original
  // envelope eligible for retry. It must not mint a new client identity.
  await emitServerMessage(page, replacementSocketIndex, {
    CommandOutcomesComplete: { game_id: 42 },
  });
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  const retry = (await socketMessages(page, replacementSocketIndex, 'GameCommandV2'))[0]
    .GameCommandV2;
  expect(retry).toEqual(firstSend);

  // The executor-authored semantic result is the acknowledgement. Once it is
  // observed, neither the periodic retry loop nor replayed recovery metadata
  // may produce another send or another logical terminal game event.
  await emitServerMessage(page, replacementSocketIndex, {
    GameEvent: {
      game_id: 42,
      tick: 6,
      sequence: 2,
      stream_seq: 12,
      user_id: 7,
      event: {
        CommandRejected: {
          command_id: firstSend.command_id,
          reason: 'command resolved by replacement executor',
        },
      },
    },
  });
  await expect.poll(() => page.evaluate(() => window.__terminalCommandOutcomes))
    .toEqual([firstSend.command_id]);

  const recoveredOutcome = {
    CommandOutcomes: {
      game_id: 42,
      client_game_session_id: firstSend.command_id.client_game_session_id,
      contiguous_through: 1,
      outcomes: {},
    },
  };
  await emitServerMessage(page, replacementSocketIndex, recoveredOutcome);
  await emitServerMessage(page, replacementSocketIndex, recoveredOutcome);
  await page.waitForTimeout(1_500);

  expect(await socketMessages(page, replacementSocketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  expect(await page.evaluate(() => window.__terminalCommandOutcomes))
    .toEqual([firstSend.command_id]);
});

for (const failurePhase of [
  'connecting',
  'open',
  'authenticated',
  'lobby-restored',
  'snapshot-restored',
  'outcomes-before-continuity',
]) {
  test(`candidate failure at ${failurePhase} preserves the old command owner`, async ({ page }) => {
    const oldSocketIndex = await establishActiveGame(page);
    const candidateSocketIndex = await beginDrain(page, oldSocketIndex, {
      autoOpen: failurePhase !== 'connecting',
    });

    if (!['connecting', 'open'].includes(failurePhase)) {
      await authenticateCandidate(page, candidateSocketIndex);
    }
    if (['lobby-restored', 'snapshot-restored', 'outcomes-before-continuity'].includes(failurePhase)) {
      await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);
    }
    if (['snapshot-restored', 'outcomes-before-continuity'].includes(failurePhase)) {
      await emitServerMessage(page, candidateSocketIndex, snapshot(10, 90));
    }
    if (failurePhase === 'outcomes-before-continuity') {
      // Reach the final candidate watermark and outcome barrier, but withhold
      // the old-path Pong. Readiness alone must not promote or close the old
      // command owner.
      await emitServerMessage(page, oldSocketIndex, snapshot(11, 6));
      await emitServerMessage(page, candidateSocketIndex, snapshot(11, 91));
      await emitServerMessage(page, candidateSocketIndex, {
        CommandOutcomesComplete: { game_id: 42 },
      });
      await expect.poll(() => continuityPings(page, oldSocketIndex)).toHaveLength(1);
    }

    await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
    await sendCommandProbe(page, `before-${failurePhase}-failure`);
    await page.evaluate((index) => {
      // Prevent an automatic retry from obscuring which failed candidate is
      // under assertion. The active old socket remains unaffected.
      window.__autoOpenSockets = false;
      window.__mockSockets[index].serverClose();
    }, candidateSocketIndex);
    await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
    await sendCommandProbe(page, `after-${failurePhase}-failure`);

    const oldCommands = await socketMessages(page, oldSocketIndex, 'GameCommandV2');
    const candidateCommands = await socketMessages(page, candidateSocketIndex, 'GameCommandV2');
    expect(oldCommands.map((message) => message.GameCommandV2.probe)).toEqual([
      `before-${failurePhase}-failure`,
      `after-${failurePhase}-failure`,
    ]);
    expect(candidateCommands).toEqual([]);
    const visibleEvents = await page.evaluate(() => window.__drainGameEvents);
    expect(visibleEvents).toEqual(failurePhase === 'outcomes-before-continuity'
      ? [
        { tick: 5, streamSequence: 10 },
        { tick: 6, streamSequence: 11 },
      ]
      : [{ tick: 5, streamSequence: 10 }]);
    await expect(page.getByText('Connecting to game server…')).toHaveCount(0);
    await expect(page.getByText('CONNECTION LOST — RESYNCING')).toHaveCount(0);
    await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
  });
}

test('an old socket crash before continuity proof adopts an already-ready candidate', async ({ page }) => {
  const oldSocketIndex = await establishActiveGame(page);
  const candidateSocketIndex = await beginDrain(page, oldSocketIndex);
  await authenticateCandidate(page, candidateSocketIndex);
  await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);
  await emitServerMessage(page, candidateSocketIndex, snapshot(10, 90));
  await emitServerMessage(page, candidateSocketIndex, {
    CommandOutcomesComplete: { game_id: 42 },
  });

  await expect.poll(() => continuityPings(page, oldSocketIndex)).toHaveLength(1);
  await expectOldSocketUsableWithoutOverlay(page, oldSocketIndex);
  await page.evaluate((index) => {
    window.__mockSockets[index].serverClose(1012, 'old gateway crashed before Pong');
  }, oldSocketIndex);

  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets.indexOf(window.__wsInstance) === index
  ), candidateSocketIndex)).toBe(true);
  await sendCommandProbe(page, 'after-crash-promotion');
  expect((await socketMessages(page, candidateSocketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.probe)).toEqual(['after-crash-promotion']);
  await expect(page.getByText('Connecting to game server…')).toHaveCount(0);
  await expect(page.getByText('CONNECTION LOST — RESYNCING')).toHaveCount(0);
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
});

test('the drain deadline keeps an already-ready candidate when old close loses the race', async ({ page }) => {
  const oldSocketIndex = await establishActiveGame(page);
  const candidateSocketIndex = await beginDrain(page, oldSocketIndex, { deadlineMs: 1_500 });
  await authenticateCandidate(page, candidateSocketIndex);
  await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);
  await emitServerMessage(page, candidateSocketIndex, snapshot(10, 90));
  await emitServerMessage(page, candidateSocketIndex, {
    CommandOutcomesComplete: { game_id: 42 },
  });

  await expect.poll(() => continuityPings(page, oldSocketIndex)).toHaveLength(1);
  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets.indexOf(window.__wsInstance) === index
  ), candidateSocketIndex), { timeout: 4_000 }).toBe(true);
  await sendCommandProbe(page, 'after-deadline-promotion');
  expect((await socketMessages(page, candidateSocketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.probe)).toEqual(['after-deadline-promotion']);
  await expect(page.getByText('Connecting to game server…')).toHaveCount(0);
  await expect(page.getByText('CONNECTION LOST — RESYNCING')).toHaveCount(0);
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
});
