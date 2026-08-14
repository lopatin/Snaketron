const { test, expect } = require('@playwright/test');

const REQUIRED_CAPABILITIES = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
  'terminal-command-cutoff-v1',
];

const RETRYABLE_MATCHMAKING_ADMISSION_REASON =
  'Failed to queue lobby: Failed to add lobby to matchmaking queue';

// Comfortably past the 5s authentication watchdog plus a saturated 2s back-off,
// so a socket that survives this window is not merely between churn cycles.
const ANONYMOUS_SOCKET_OBSERVATION_MS = 8_000;

// Mirrors utils/connectionBanner.ts, which this CommonJS spec cannot import.
// The values themselves are pinned by tests/unit/connectionBanner.test.ts.
const CONNECTION_BANNER_SHOW_DELAY_MS = 800;
const CONNECTION_BANNER_MIN_VISIBLE_MS = 1_200;

const gameState = (tick = 5) => ({
  tick,
  status: { Started: { server_id: 1 } },
  arena: {
    width: 40,
    height: 40,
    snakes: [
      {
        body: [{ x: 20, y: 20 }, { x: 19, y: 20 }],
        direction: 'Right',
        is_alive: true,
        food: 0,
        team_id: null,
        speed_milli: 1000,
        movement_credit: 0,
        boost: { charge_ms: 3000, active: false, intent: false },
      },
    ],
    food: [],
    boost_pads: [],
    team_zone_config: null,
  },
  game_type: 'Solo',
  queue_mode: 'Quickmatch',
  properties: {
    available_food_target: 10,
    tick_duration_ms: 50,
    time_limit_ms: null,
    score_limit: null,
    boost: {
      speed_milli: 1500,
      capacity_ms: 3000,
      packet_charge_ms: 750,
      pad_respawn_ms: 8000,
      spot_layout_version: 0,
      rules_version: 2,
      unlimited: true,
    },
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
  player_action_counts: { 7: 0 },
  readiness: null,
  simulation_epoch_ms: null,
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

const boostSnapshot = (streamSequence, tick = 5) => {
  const state = gameState(tick);
  state.game_type = { TeamMatch: { per_team: 1 } };
  state.arena.width = 60;
  state.arena.height = 40;
  state.arena.snakes[0].body = [{ x: 30, y: 20 }, { x: 29, y: 20 }];
  state.arena.snakes[0].team_id = 0;
  state.arena.snakes[0].boost.charge_ms = 1000;
  state.arena.team_zone_config = { end_zone_depth: 10, goal_width: 9 };
  state.arena.boost_pads = [
    { id: 0, position: { x: 14, y: 4 }, charge_ms: 3000, size_cells: 2, respawn_at_tick: null },
    { id: 1, position: { x: 14, y: 34 }, charge_ms: 3000, size_cells: 2, respawn_at_tick: tick + 100 },
    { id: 2, position: { x: 44, y: 4 }, charge_ms: 3000, size_cells: 2, respawn_at_tick: null },
    { id: 3, position: { x: 44, y: 34 }, charge_ms: 3000, size_cells: 2, respawn_at_tick: null },
    { id: 4, position: { x: 26, y: 12 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 5, position: { x: 33, y: 12 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 6, position: { x: 37, y: 16 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 7, position: { x: 37, y: 23 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 8, position: { x: 33, y: 27 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 9, position: { x: 26, y: 27 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 10, position: { x: 22, y: 23 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
    { id: 11, position: { x: 22, y: 16 }, charge_ms: 750, size_cells: 1, respawn_at_tick: null },
  ];
  // Keep this snapshot on the live wall-clock boundary. The real WASM client
  // can then execute and visibly reconcile a predicted activation instead of
  // merely serializing a command against a pre-start fixture.
  state.start_ms = Date.now() - tick * 50;
  state.properties = {
    available_food_target: 10,
    tick_duration_ms: 50,
    // Team matches are raced to a score, never against a clock; the target
    // must match the queue this snapshot claims (Quickmatch).
    time_limit_ms: null,
    score_limit: 25,
    boost: {
      speed_milli: 1500,
      capacity_ms: 3000,
      packet_charge_ms: 750,
      pad_respawn_ms: 8000,
      spot_layout_version: 3,
      rules_version: 2,
      unlimited: false,
    },
  };

  return {
    GameEvent: {
      game_id: 42,
      tick,
      sequence: 1,
      stream_seq: streamSequence,
      user_id: 7,
      event: { Snapshot: { game_state: state } },
    },
  };
};

const completedBoostSnapshot = (streamSequence, tick = 7) => {
  const frame = boostSnapshot(streamSequence, tick);
  const state = frame.GameEvent.event.Snapshot.game_state;
  state.status = { Complete: { winning_snake_id: 0 } };
  state.arena.snakes = [
    state.arena.snakes[0],
    {
      body: [{ x: 30, y: 25 }, { x: 31, y: 25 }],
      direction: 'Left',
      is_alive: true,
      food: 0,
      team_id: 1,
      speed_milli: 1000,
      movement_credit: 0,
      boost: { charge_ms: 0, active: false, intent: false },
    },
  ];
  state.players = {
    7: { user_id: 7, snake_id: 0 },
    8: { user_id: 8, snake_id: 1 },
  };
  state.usernames = { 7: 'drain-tester', 8: 'Tron' };
  state.spectators = [20, 21];
  state.scores = { 0: 2, 1: 1 };
  state.team_scores = { 0: 1, 1: 0 };
  state.player_xp = { 7: 110, 8: 20 };
  state.player_action_counts = { 7: 36, 8: 31 };
  return frame;
};

const tutorialSnapshot = (streamSequence = 10) => {
  const frame = completedBoostSnapshot(streamSequence, 0);
  const state = frame.GameEvent.event.Snapshot.game_state;
  const now = Date.now();
  state.tick = 0;
  state.status = { Started: { server_id: 1 } };
  state.start_ms = now;
  state.readiness = { deadline_ms: now + 15_000, ready_user_ids: [] };
  state.simulation_epoch_ms = null;
  state.scores = { 0: 0, 1: 0 };
  state.team_scores = { 0: 0, 1: 0 };
  frame.GameEvent.tick = 0;
  return frame;
};

const liveBoostSnapshotForLocalTeam = (streamSequence, tick, teamId) => {
  const frame = completedBoostSnapshot(streamSequence, tick);
  const state = frame.GameEvent.event.Snapshot.game_state;
  state.status = { Started: { server_id: 1 } };
  state.start_ms = Date.now() - tick * 50;

  if (teamId === 1) {
    state.players = {
      7: { user_id: 7, snake_id: 1 },
      8: { user_id: 8, snake_id: 0 },
    };
    state.arena.snakes[1].boost = { charge_ms: 1000, active: false, intent: false };
  }

  return frame;
};

const fourSnakeBoostState = (tick = 6) => {
  const state = boostSnapshot(1, tick).GameEvent.event.Snapshot.game_state;
  state.game_type = { TeamMatch: { per_team: 2 } };
  state.properties.available_food_target = 20;
  state.properties.boost.speed_milli = 2000;
  state.start_ms = 0;
  state.arena.snakes = [
    { body: [{ x: 15, y: 5 }, { x: 12, y: 5 }], direction: 'Right', team_id: 0 },
    { body: [{ x: 30, y: 12 }, { x: 33, y: 12 }], direction: 'Left', team_id: 1 },
    { body: [{ x: 15, y: 27 }, { x: 12, y: 27 }], direction: 'Right', team_id: 0 },
    { body: [{ x: 30, y: 34 }, { x: 33, y: 34 }], direction: 'Left', team_id: 1 },
  ].map((snake) => ({
    ...snake,
    is_alive: true,
    food: 0,
    speed_milli: 2000,
    movement_credit: 0,
    boost: { charge_ms: 3000, active: true, intent: true },
  }));
  state.players = {
    7: { user_id: 7, snake_id: 0 },
    8: { user_id: 8, snake_id: 1 },
    9: { user_id: 9, snake_id: 2 },
    10: { user_id: 10, snake_id: 3 },
  };
  state.usernames = { 7: 'one', 8: 'two', 9: 'three', 10: 'four' };
  state.scores = { 0: 0, 1: 0, 2: 0, 3: 0 };
  state.team_scores = { 0: 0, 1: 0 };
  state.player_xp = { 7: 0, 8: 0, 9: 0, 10: 0 };
  state.player_action_counts = { 7: 0, 8: 0, 9: 0, 10: 0 };
  return state;
};

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

const NOS_CANVAS_PALETTE = {
  ink: [23, 32, 51],
  blue: [59, 130, 246],
  highlight: [147, 197, 253],
  shade: [37, 99, 235],
  label: [248, 250, 252],
  steelDark: [71, 85, 105],
  steelLight: [203, 213, 225],
  orange: [255, 100, 30],
};

async function readNosCanvasRegions(page, effectiveGridWidth, regions, includeWholeArena = true) {
  return page.evaluate(({ effectiveGridWidth, regions, palette, includeWholeArena }) => {
    const canvas = document.querySelector('.game-arena-panel canvas');
    const context = canvas?.getContext('2d');
    if (!canvas || !context) {
      throw new Error('arena canvas is unavailable');
    }

    const cellSize = (canvas.width - 2) / effectiveGridWidth;
    const paletteEntries = Object.entries(palette);
    const countPixels = (imageData) => {
      const counts = Object.fromEntries(paletteEntries.map(([name]) => [name, 0]));
      counts.antialiased = 0;
      let oldLightningYellow = 0;
      for (let index = 0; index < imageData.data.length; index += 4) {
        if (imageData.data[index + 3] === 0) continue;
        const red = imageData.data[index];
        const green = imageData.data[index + 1];
        const blue = imageData.data[index + 2];
        const minimum = Math.min(red, green, blue);
        const maximum = Math.max(red, green, blue);
        const exactTone = paletteEntries.some(([, [expectedRed, expectedGreen, expectedBlue]]) => (
          red === expectedRed && green === expectedGreen && blue === expectedBlue
        ));
        const blueCore = blue >= 120 && blue - Math.max(red, green) >= 40;
        const orangeMark = red >= 180 && red - green >= 70 && green - blue >= 20;
        const darkOutline = maximum <= 150;
        const lightLabel = red >= 240 && green >= 240 && blue >= 240 && blue - red >= 3;

        if (darkOutline) counts.ink += 1;
        if (blueCore) counts.blue += 1;
        if (lightLabel) counts.label += 1;
        if (orangeMark) counts.orange += 1;
        if (!exactTone && maximum - minimum >= 12 && (blueCore || orangeMark || darkOutline)) {
          counts.antialiased += 1;
        }
        if (red === 248 && green === 200 && blue === 74) {
          oldLightningYellow += 1;
        }
      }
      return {
        counts,
        total: Object.values(counts).reduce((sum, count) => sum + count, 0),
        oldLightningYellow,
      };
    };

    const samples = Object.fromEntries(regions.map((region) => {
      const overscan = region.overscanPixels ?? 2;
      const left = Math.max(0, 1 + region.x * cellSize - overscan);
      const top = Math.max(0, 1 + region.y * cellSize - overscan);
      const size = region.sizeCells * cellSize + overscan * 2;
      return [
        region.name,
        countPixels(context.getImageData(left, top, size, size)),
      ];
    }));
    if (includeWholeArena) {
      samples.wholeArena = countPixels(context.getImageData(0, 0, canvas.width, canvas.height));
    }

    return { cellSize, samples };
  }, {
    effectiveGridWidth,
    regions,
    palette: NOS_CANVAS_PALETTE,
    includeWholeArena,
  });
}

async function readGridIntersectionInk(page, effectiveGridWidth, intersections) {
  return page.evaluate(({ effectiveGridWidth, intersections }) => {
    const canvas = document.querySelector('.game-arena-panel canvas');
    const context = canvas?.getContext('2d');
    if (!canvas || !context) {
      throw new Error('arena canvas is unavailable');
    }
    const cellSize = (canvas.width - 2) / effectiveGridWidth;
    const neutralDotPixels = ({ x, y }) => {
      const centerX = Math.round(1 + x * cellSize);
      const centerY = Math.round(1 + y * cellSize);
      const imageData = context.getImageData(centerX - 1, centerY - 1, 3, 3);
      const values = [];
      for (let index = 0; index < imageData.data.length; index += 4) {
        const red = imageData.data[index];
        const green = imageData.data[index + 1];
        const blue = imageData.data[index + 2];
        if (
          Math.max(red, green, blue) - Math.min(red, green, blue) <= 2 &&
          red >= 140 && red <= 240
        ) {
          values.push(red);
        }
      }
      return values;
    };
    const samples = intersections.map((intersection) => neutralDotPixels(intersection));
    return {
      cellSize,
      counts: samples.map((sample) => sample.length),
      samples,
    };
  }, { effectiveGridWidth, intersections });
}

function expectNosPickupIdentity(sample, requireExactSeparatorPixel = false) {
  expect(sample.counts.ink).toBeGreaterThan(0);
  expect(sample.counts.blue).toBeGreaterThan(0);
  if (requireExactSeparatorPixel) {
    expect(sample.counts.label).toBeGreaterThan(0);
  }
  expect(sample.counts.orange).toBeGreaterThan(0);
  expect(sample.counts.antialiased).toBeGreaterThan(0);
}

function hasNosPickupIdentity(sample, requireExactSeparatorPixel = false) {
  return sample.counts.ink > 0
    && sample.counts.blue > 0
    && (!requireExactSeparatorPixel || sample.counts.label > 0)
    && sample.counts.orange > 0
    && sample.counts.antialiased > 0;
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

async function establishActiveGame(page, initialFrame = snapshot(10, 5)) {
  await page.goto('/play/42');
  await expect.poll(() => page.evaluate(() => (
    window.__wsInstance ? window.__mockSockets.indexOf(window.__wsInstance) : -1
  ))).toBeGreaterThanOrEqual(0);
  const oldSocketIndex = await page.evaluate(() => window.__mockSockets.indexOf(window.__wsInstance));
  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets[index].readyState
  ), oldSocketIndex)).toBe(1);
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'Authenticate')).toHaveLength(1);
  await emitServerMessage(page, oldSocketIndex, {
    Authenticated: {
      task_boot_id: 'old-task',
      protocol_version: 7,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 1,
    },
  });
  await expect.poll(() => page.evaluate(() => (
    window.__wsContext?.isConnected && window.__wsContext?.isSessionAuthenticated
  ))).toBe(true);
  // Initial mount has both transport-context restoration and the persisted
  // lobby state request. Complete the latter so later reconnects issue one
  // transport restoration request instead of racing its five-second timeout.
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'JoinLobby')).toHaveLength(2);
  await emitServerMessage(page, oldSocketIndex, {
    JoinedLobby: { lobby_code: 'LOBBY1' },
  });
  await expect.poll(() => page.evaluate(() => window.__wsContext?.currentLobby?.code))
    .toBe('LOBBY1');
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'JoinGame')).not.toHaveLength(0);
  // The JoinGame send and the GameArena message-handler effect are separate
  // React commits. Yield once so the initial snapshot cannot outrun the
  // consumer registration in a fast headless browser.
  await page.waitForTimeout(800);
  await emitServerMessage(page, oldSocketIndex, lobbyUpdate);
  await emitServerMessage(page, oldSocketIndex, initialFrame);
  await emitServerMessage(page, oldSocketIndex, { CommandOutcomesComplete: { game_id: 42 } });
  await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
  const initialMarker = {
    tick: initialFrame.GameEvent.tick,
    streamSequence: initialFrame.GameEvent.stream_seq,
  };
  await page.evaluate((marker) => {
    window.__drainGameEvents = [marker];
    window.__drainEventUnsubscribe = window.__wsContext.onMessage('GameEvent', (message) => {
      window.__drainGameEvents.push({
        tick: message.data.tick,
        streamSequence: message.data.stream_seq,
      });
    });
  }, initialMarker);
  return oldSocketIndex;
}

async function establishAuthenticatedLobby(page) {
  await page.goto('/');
  await expect.poll(() => page.evaluate(() => (
    window.__wsInstance ? window.__mockSockets.indexOf(window.__wsInstance) : -1
  ))).toBeGreaterThanOrEqual(0);
  const socketIndex = await page.evaluate(() => window.__mockSockets.indexOf(window.__wsInstance));
  await expect.poll(() => socketMessages(page, socketIndex, 'Authenticate')).toHaveLength(1);
  await emitServerMessage(page, socketIndex, {
    Authenticated: {
      task_boot_id: 'lobby-task',
      protocol_version: 7,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 1,
    },
  });
  await expect.poll(() => socketMessages(page, socketIndex, 'JoinLobby')).toHaveLength(2);
  await emitServerMessage(page, socketIndex, {
    JoinedLobby: { lobby_code: 'LOBBY1' },
  });
  await expect.poll(() => page.evaluate(() => window.__wsContext?.currentLobby?.code))
    .toBe('LOBBY1');
  await emitServerMessage(page, socketIndex, lobbyUpdate);
  await expect.poll(() => page.evaluate(() => (
    window.__wsContext?.isConnected && window.__wsContext?.isSessionAuthenticated
  ))).toBe(true);
  return socketIndex;
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
    await expect.poll(() => socketMessages(page, candidateSocketIndex, 'Authenticate')).toHaveLength(1);
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
      protocol_version: 7,
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
    localStorage.removeItem('snaketron:tutorial-seen:v1');
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
      } else if (url.includes('/api/leaderboard/me?') && window.__ratingFixture) {
        const fixture = window.__ratingFixture;
        fixture.requests.push(url);
        if (fixture.requests.length === 1) {
          payload = fixture.before;
        } else {
          while (!fixture.release) {
            await new Promise((resolve) => setTimeout(resolve, 10));
          }
          payload = fixture.after;
        }
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
        if (code !== 1000 && (code < 3000 || code > 4999)) {
          throw new DOMException(
            `Invalid client WebSocket close code: ${code}`,
            'InvalidAccessError',
          );
        }
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

test('logout explicitly leaves the lobby before retiring the authenticated socket', async ({ page }) => {
  const oldSocketIndex = await establishAuthenticatedLobby(page);
  await emitServerMessage(page, oldSocketIndex, {
    LobbyChatMessage: {
      lobby_id: 1,
      message_id: 'logout-regression-message',
      user_id: 7,
      username: 'drain-tester',
      message: 'This must not leak into the next session.',
      timestamp_ms: Date.now(),
    },
  });

  await expect.poll(() => page.evaluate(() => ({
    lobbyCode: window.__wsContext?.currentLobby?.code,
    memberCount: window.__wsContext?.lobbyMembers.length,
    chatCount: window.__wsContext?.lobbyChatMessages.length,
  }))).toEqual({ lobbyCode: 'LOBBY1', memberCount: 1, chatCount: 1 });
  const socketCountBeforeLogout = await page.evaluate(() => window.__mockSockets.length);

  await page.getByRole('button', { name: 'drain-tester' }).click();
  await page.getByRole('menuitem', { name: 'Logout' }).click();

  await expect.poll(() => page.evaluate((socketIndex) => {
    const oldSocket = window.__mockSockets[socketIndex];
    const decodedMessages = oldSocket.sent.map((raw) => JSON.parse(raw));
    return {
      leaveCount: decodedMessages.filter((message) => message === 'LeaveLobby').length,
      closeCount: oldSocket.closeCalls.length,
      lobby: window.__wsContext?.currentLobby ?? null,
      memberCount: window.__wsContext?.lobbyMembers.length,
      chatCount: window.__wsContext?.lobbyChatMessages.length,
      token: localStorage.getItem('token'),
      storedLobby: localStorage.getItem('snaketron:lastLobby'),
    };
  }, oldSocketIndex)).toEqual({
    leaveCount: 1,
    closeCount: 1,
    lobby: null,
    memberCount: 0,
    chatCount: 0,
    token: null,
    storedLobby: null,
  });

  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.__mockSockets.length))
    .toBeGreaterThan(socketCountBeforeLogout);
  await expect.poll(() => page.evaluate((index) => window.__mockSockets
    .slice(index)
    .flatMap((socket) => socket.sent.map((raw) => JSON.parse(raw)))
    .filter((message) => (
      message &&
      typeof message === 'object' &&
      ('Authenticate' in message || 'JoinLobby' in message)
    )).length, socketCountBeforeLogout)).toBe(0);

  // The post-logout socket carries no token, so it is the same shape as an
  // anonymous visitor's. It must simply stay open rather than being torn down
  // by a handshake deadline for a handshake it never started.
  const socketCountAfterLogout = await page.evaluate(() => window.__mockSockets.length);
  await page.waitForTimeout(ANONYMOUS_SOCKET_OBSERVATION_MS);
  expect(await page.evaluate((index) => ({
    socketsOpenedSince: window.__mockSockets.length - index,
    lastSocketCloseCalls: window.__mockSockets[index - 1].closeCalls,
    lastSocketReadyState: window.__mockSockets[index - 1].readyState,
  }), socketCountAfterLogout)).toEqual({
    socketsOpenedSince: 0,
    lastSocketCloseCalls: [],
    lastSocketReadyState: 1,
  });
});

// Regression: the client armed its 5s authentication watchdog on every socket
// but sent `Authenticate` only when a token existed, so a visitor with no
// account waited for a reply to a message it never sent, closed its own healthy
// socket with 4013, reconnected, and flashed the connecting banner forever.
test('an anonymous visitor keeps one socket open and never shows the connecting banner', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.removeItem('token');
    localStorage.removeItem('snaketron:lastLobby');
  });

  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(true);
  // Startup opens more than one socket while region detection settles. That
  // race is separate and pre-existing; what matters here is that it settles.
  const settledSocketCount = await page.evaluate(() => window.__mockSockets.length);

  // Long enough to have covered the old 5s watchdog plus a full back-off cycle.
  await page.waitForTimeout(ANONYMOUS_SOCKET_OBSERVATION_MS);

  expect(await page.evaluate((baseline) => {
    const live = window.__mockSockets[baseline - 1];
    return {
      socketsOpenedSince: window.__mockSockets.length - baseline,
      closeCalls: live.closeCalls,
      readyState: live.readyState,
      authenticateFrames: live.sent
        .map((raw) => JSON.parse(raw))
        .filter((message) => message && typeof message === 'object' && 'Authenticate' in message)
        .length,
      connected: window.__wsContext?.isConnected,
    };
  }, settledSocketCount)).toEqual({
    socketsOpenedSince: 0,
    closeCalls: [],
    readyState: 1,
    authenticateFrames: 0,
    connected: true,
  });
  await expect(page.getByText('Connecting to game server…')).toHaveCount(0);
});

// The badge explains a problem the player can act on. A gap short enough to be
// invisible is not one, and painting it is the flicker itself.
test('a brief transport gap stays silent while a sustained one is announced', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.removeItem('token');
    localStorage.removeItem('snaketron:lastLobby');
  });

  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(true);
  const banner = page.getByText('Connecting to game server…');
  await expect(banner).toHaveCount(0);

  // Watch continuously: a badge that appears and vanishes between polls is
  // exactly the flicker being fixed, so sampling from the test is not enough.
  await page.evaluate(() => {
    window.__bannerEverShown = false;
    new MutationObserver(() => {
      if (document.body.innerText.toUpperCase().includes('CONNECTING TO GAME SERVER')) {
        window.__bannerEverShown = true;
      }
    }).observe(document.body, { childList: true, subtree: true, characterData: true });
  });

  // A real gap, held open below the show delay, then closed.
  await page.evaluate(() => {
    window.__autoOpenSockets = false;
    window.__mockSockets[window.__mockSockets.length - 1].serverClose(1012, 'brief blip');
  });
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(false);
  await page.waitForTimeout(CONNECTION_BANNER_SHOW_DELAY_MS / 2);
  await page.evaluate(() => {
    window.__autoOpenSockets = true;
    window.__mockSockets[window.__mockSockets.length - 1].serverOpen();
  });
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(true);

  await page.waitForTimeout(CONNECTION_BANNER_SHOW_DELAY_MS + 400);
  expect(await page.evaluate(() => window.__bannerEverShown)).toBe(false);
  await expect(banner).toHaveCount(0);

  // A gap that does not close: the replacement socket is left connecting.
  await page.evaluate(() => {
    window.__autoOpenSockets = false;
    window.__mockSockets[window.__mockSockets.length - 1].serverClose(1012, 'sustained outage');
  });
  await expect(banner).toHaveCount(1, { timeout: 5_000 });

  // Recovery takes the badge down, but only after it has been readable.
  const shownAtMs = Date.now();
  await page.evaluate(() => {
    window.__autoOpenSockets = true;
    window.__mockSockets[window.__mockSockets.length - 1].serverOpen();
  });
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(true);
  await expect(banner).toHaveCount(0, { timeout: 5_000 });
  expect(Date.now() - shownAtMs).toBeGreaterThanOrEqual(CONNECTION_BANNER_MIN_VISIBLE_MS);
});

// The watchdog still has to cover the handshake a visitor reaches by pressing
// Play: an anonymous socket that only now has an identity to present.
test('an identity acquired on an open anonymous socket still gets a handshake deadline', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.removeItem('token');
    localStorage.removeItem('snaketron:lastLobby');
  });

  await page.goto('/');
  await expect.poll(() => page.evaluate(() => window.__wsContext?.isConnected)).toBe(true);
  const liveSocketIndex = await page.evaluate(() => window.__mockSockets.length - 1);
  expect(await socketMessages(page, liveSocketIndex, 'Authenticate')).toHaveLength(0);

  // A guest session appears while the socket is already open.
  await page.evaluate(() => {
    localStorage.setItem('token', 'late-guest-token');
    window.__wsContext?.waitForSessionReady().catch(() => {});
  });
  await expect.poll(() => socketMessages(page, liveSocketIndex, 'Authenticate')).toHaveLength(1);

  // The server never answers, so the socket must be retired for retry rather
  // than left hanging on a handshake that will never complete.
  await expect.poll(
    () => page.evaluate((index) => window.__mockSockets[index].closeCalls, liveSocketIndex),
    { timeout: 10_000 },
  ).toEqual([{ code: 4013, reason: 'authentication timed out' }]);
});

test('a returning player is auto-readied without mounting the briefing', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem(
      'snaketron:tutorial-seen:v1',
      JSON.stringify({ 'duel:casual': true }),
    );

    window.__briefingMountCount = 0;
    const briefingSelector = '[data-testid="tutorial-modal"][data-variant="briefing"]';
    new MutationObserver((records) => {
      for (const record of records) {
        for (const node of record.addedNodes) {
          if (!(node instanceof Element)) continue;
          if (node.matches(briefingSelector)) {
            window.__briefingMountCount += 1;
          }
          window.__briefingMountCount += node.querySelectorAll(briefingSelector).length;
        }
      }
    }).observe(document, { childList: true, subtree: true });
  });

  const initialTutorialFrame = tutorialSnapshot();
  initialTutorialFrame.GameEvent.event.Snapshot.game_state.readiness.ready_user_ids = [8];
  const socketIndex = await establishActiveGame(page, initialTutorialFrame);

  await expect.poll(() => socketMessages(page, socketIndex, 'PlayerReady')).toHaveLength(1);
  await expect(page.getByTestId('tutorial-modal')).toHaveCount(0);
  expect(await page.evaluate(() => window.__briefingMountCount)).toBe(0);
});

test('the pre-match guide reveals one animated real-arena step at a time', async ({ page }) => {
  await page.addInitScript(() => {
    window.__tutorialScoreReadouts = [];
    const originalFillText = CanvasRenderingContext2D.prototype.fillText;
    CanvasRenderingContext2D.prototype.fillText = function recordTutorialScore(text, ...args) {
      if (text === '+1') window.__tutorialScoreReadouts.push(text);
      return originalFillText.call(this, text, ...args);
    };
  });
  const initialTutorialFrame = tutorialSnapshot();
  initialTutorialFrame.GameEvent.event.Snapshot.game_state.readiness.ready_user_ids = [8];
  const socketIndex = await establishActiveGame(page, initialTutorialFrame);
  const modal = page.getByTestId('tutorial-modal');
  const canvas = page.getByTestId('tutorial-scene-canvas');
  const readyButton = page.getByTestId('tutorial-ready');
  const localRoster = page.locator(
    '.game-roster-snake-canvas[data-player-name="You"]',
  );
  const rivalRoster = page.locator('.game-roster-snake-canvas[data-player-name="Tron"]');

  await expect(modal).toBeVisible();
  await expect(modal).toHaveAttribute('data-step', '1');
  await expect(modal.getByRole('heading', { name: 'Duel' })).toBeVisible();
  await expect(modal).toContainText('Return food to base to score points.');
  await expect(modal.locator('.tutorial-step-index')).toHaveCount(0);
  await expect(modal.locator('.tutorial-step-title')).toHaveCount(0);
  await expect(modal.locator('.tutorial-step-instruction')).toHaveCount(1);
  await expect(page.getByTestId('tutorial-visual')).toHaveAccessibleName(
    'A snake returns through the gate labeled YOU; the team score increases.',
  );
  await expect(modal).toHaveAccessibleDescription(
    /Each lesson advances automatically after five seconds.*match starts automatically/i,
  );
  await expect(page.getByTestId('tutorial-auto-start')).toHaveAccessibleName(
    /Automatic match start in 1[5-8] seconds/,
  );
  await expect(page.locator('.game-roster-ready-mark')).toHaveCount(0);
  await expect(localRoster).toHaveAttribute('data-ready', 'false');
  await expect(rivalRoster).toHaveAttribute('data-ready', 'true');
  await expect(canvas).toHaveCount(1);
  await expect(canvas).toHaveAttribute('data-scene', 'team-carry');
  await expect(canvas).toHaveAttribute('data-motion', 'animated');
  await expect.poll(() => canvas.evaluate((element) => (
    element.width > 1 && element.height > 1 && element.dataset.playback
  ))).toMatch(/playing|complete/);

  const stepTimer = page.getByTestId('tutorial-step-timer');
  await expect(stepTimer).toHaveCSS('animation-duration', '5s');

  // The scoring beat uses the production +1 renderer, and the step changes at
  // the exact end of the progress rail without stealing control focus.
  await expect.poll(() => page.evaluate(() => window.__tutorialScoreReadouts.length), {
    timeout: 3_500,
  }).toBeGreaterThan(0);
  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/duel-briefing.jpg`,
      fullPage: true,
    });
  }
  await readyButton.focus();
  await expect.poll(() => modal.getAttribute('data-step'), { timeout: 6_500 }).toBe('2');
  await expect(readyButton).toBeFocused();
  await expect(modal).toContainText('Collect NOS, then hold Space to boost.');
  await expect(canvas).toHaveAttribute('data-scene', 'team-boost');

  await page.keyboard.press('Escape');
  await expect(modal).toBeVisible();

  const autoplayToggle = page.getByTestId('tutorial-autoplay-toggle');
  await stepTimer.evaluate((element) => {
    const [animation] = element.getAnimations();
    if (!animation) throw new Error('Active tutorial timer has no animation');
    animation.currentTime = 4_700;
  });
  await autoplayToggle.click();
  await expect(modal).toHaveAttribute('data-autoplay', 'paused');
  await expect(stepTimer).toHaveCSS('animation-play-state', 'paused');
  await expect(autoplayToggle).toHaveAccessibleName('Resume tutorial');
  await expect(autoplayToggle).not.toHaveAttribute('aria-pressed', /.+/);
  await page.waitForTimeout(700);
  await expect(modal).toHaveAttribute('data-step', '2');
  await autoplayToggle.click();
  await expect(modal).toHaveAttribute('data-autoplay', 'playing');
  await expect.poll(() => modal.getAttribute('data-step'), { timeout: 1_500 }).toBe('3');
  await expect(autoplayToggle).toBeFocused();
  await expect(modal).toContainText('Avoid the rival base. First to 25 wins.');

  // Arrow navigation belongs to the guide while it is open and must not leak
  // a turn or a Boost edge into the match beneath it.
  await page.keyboard.press('ArrowLeft');
  await expect(modal).toHaveAttribute('data-step', '2');
  await page.keyboard.press('ArrowRight');
  await expect(modal).toHaveAttribute('data-step', '3');
  await expect.poll(() => page.evaluate(() => (
    document.activeElement?.getAttribute('data-tutorial-step-control')
  ))).toBe('2');
  await page.keyboard.press('Space');
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2')).toHaveLength(0);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.setViewportSize({ width: 390, height: 844 });
    await modal.getByRole('button', { name: /Step 2 of 3: BOOST/i }).click();
    await page.waitForTimeout(650);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/mobile-duel-briefing.jpg`,
      fullPage: true,
    });
  }
  const rosterBeforeReady = await localRoster.evaluate((element) => element.toDataURL());
  await readyButton.click();
  await expect(readyButton).toBeDisabled();
  await expect(page.getByTestId('tutorial-status')).toContainText('All players ready');
  await expect(page.getByTestId('tutorial-auto-start')).toBeVisible();
  const readyFrame = tutorialSnapshot(11);
  readyFrame.GameEvent.event.Snapshot.game_state.readiness.ready_user_ids = [7, 8];
  await emitServerMessage(page, socketIndex, readyFrame);
  await expect(localRoster).toHaveAttribute('data-ready', 'true');
  await expect(rivalRoster).toHaveAttribute('data-ready', 'true');
  await expect.poll(() => localRoster.evaluate((element) => element.toDataURL()))
    .not.toBe(rosterBeforeReady);
  await page.keyboard.press('Tab');
  await expect.poll(() => page.evaluate(() => {
    const tutorialModal = document.querySelector('[data-testid="tutorial-modal"]');
    return Boolean(tutorialModal?.contains(document.activeElement));
  })).toBe(true);
  await expect.poll(() => socketMessages(page, socketIndex, 'PlayerReady')).toHaveLength(1);
  await expect.poll(() => page.evaluate(() => (
    JSON.parse(localStorage.getItem('snaketron:tutorial-seen:v1') || '{}')['duel:casual']
  ))).toBe(true);
});

test('reduced motion holds tutorial scenes on their authored poster frame', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.setViewportSize({ width: 320, height: 568 });
  await establishActiveGame(page, tutorialSnapshot());

  const modal = page.getByTestId('tutorial-modal');
  const canvas = page.getByTestId('tutorial-scene-canvas');
  await expect(modal).toBeVisible();
  await expect(modal).toHaveAttribute('data-autoplay', 'off');
  await expect(modal).toHaveAttribute('data-step', '1');
  await expect(modal).toContainText('Return food to base to score points.');
  await expect(canvas).toHaveAttribute('data-motion', 'reduced');
  await expect(canvas).toHaveAttribute('data-playback', 'complete');
  await expect(modal.locator('.tutorial-replay')).toBeHidden();
  await expect(modal.getByTestId('tutorial-autoplay-toggle')).toHaveCount(0);
  await expect(modal.getByTestId('tutorial-ready')).toBeVisible();
  const poster = await canvas.evaluate((element) => element.toDataURL());
  await page.waitForTimeout(350);
  expect(await canvas.evaluate((element) => element.toDataURL())).toBe(poster);
  await page.waitForTimeout(5_000);
  await expect(modal).toHaveAttribute('data-step', '1');

  const bounds = await modal.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    const visualRect = element.querySelector('[data-testid="tutorial-visual"]')
      .getBoundingClientRect();
    const footerRect = element.querySelector('.tutorial-footer').getBoundingClientRect();
    return {
      top: rect.top,
      right: rect.right,
      bottom: rect.bottom,
      left: rect.left,
      visualBottom: visualRect.bottom,
      footerTop: footerRect.top,
      viewportWidth: window.innerWidth,
      viewportHeight: window.innerHeight,
    };
  });
  expect(bounds.top).toBeGreaterThanOrEqual(0);
  expect(bounds.left).toBeGreaterThanOrEqual(0);
  expect(bounds.right).toBeLessThanOrEqual(bounds.viewportWidth);
  expect(bounds.bottom).toBeLessThanOrEqual(bounds.viewportHeight);
  expect(bounds.visualBottom).toBeLessThanOrEqual(bounds.footerTop);

  await modal.getByRole('button', { name: 'Next' }).click();
  await expect.poll(() => page.evaluate(() => (
    document.activeElement?.getAttribute('data-tutorial-step-control')
  ))).toBe('1');
  await expect(canvas).toHaveAttribute('data-scene', 'team-boost');
  await expect(canvas).toHaveAttribute('data-playback', 'complete');
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

  expect(await socketMessages(page, candidateSocketIndex, 'Authenticate')).toHaveLength(1);
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
  expect(await socketMessages(page, candidateSocketIndex, 'Authenticate')).toHaveLength(1);

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
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'Authenticate')).toHaveLength(1);

  await emitServerMessage(page, replacementSocketIndex, {
    Authenticated: {
      task_boot_id: 'replacement-after-crash',
      protocol_version: 7,
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

test('Boost HUD touch control starts and stops the local snake Boost', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));

  await expect(page.getByTestId('boost-hud')).toContainText('33%');
  const boostButton = page.getByRole('button', { name: 'Hold to Boost, 33% charged' });
  await expect(boostButton).toBeEnabled();
  await expect(boostButton).toHaveAttribute('aria-keyshortcuts', 'Space');
  await expect(boostButton.locator('.game-boost-meter__canister')).toHaveCount(1);
  await expect(boostButton.locator('.game-boost-meter__bolt')).toHaveCount(0);
  await expect(page.getByRole('progressbar', { name: 'Stored Boost charge' }))
    .toHaveAttribute('aria-valuenow', '33');
  await boostButton.hover();
  await page.mouse.down();

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  await page.mouse.up();

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(2);
  const commands = (await socketMessages(page, socketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.command.command);
  expect(commands).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
});

test('the focused Hold Boost control starts and stops on one Space hold', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));
  const boostButton = page.getByTestId('boost-button');
  await boostButton.focus();
  await expect(boostButton).toBeFocused();

  await page.keyboard.down('Space');
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  await page.keyboard.up('Space');
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(2);

  expect((await socketMessages(page, socketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.command.command)).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
  await page.evaluate(() => {
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  });
});

test('arena route teardown orders Boost deactivation before LeaveGame', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));

  const boostButton = page.getByTestId('boost-button');
  await boostButton.hover();
  await page.mouse.down();
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);

  await page.evaluate(() => {
    const state = {
      ...history.state,
      idx: (history.state?.idx ?? 0) + 1,
      key: 'boost-teardown-test',
    };
    history.pushState(state, '', '/play/43');
  });
  await page.goBack();
  await expect(page).toHaveURL(/\/play\/42$/);
  await page.goForward();
  await expect(page).toHaveURL(/\/play\/43$/);

  await expect.poll(() => page.evaluate((index) => {
    const sent = window.__mockSockets[index].sent.map((raw) => JSON.parse(raw));
    const deactivateIndex = sent.findIndex((message) => (
      message.GameCommandV2?.command?.command?.DeactivateBoost?.snake_id === 0
    ));
    const leaveIndex = sent.findIndex((message) => message === 'LeaveGame');
    return deactivateIndex >= 0 && leaveIndex > deactivateIndex;
  }, socketIndex)).toBe(true);

  const order = await page.evaluate((index) => {
    const sent = window.__mockSockets[index].sent.map((raw) => JSON.parse(raw));
    return {
      deactivateIndex: sent.findIndex((message) => (
        message.GameCommandV2?.command?.command?.DeactivateBoost?.snake_id === 0
      )),
      leaveIndex: sent.findIndex((message) => message === 'LeaveGame'),
    };
  }, socketIndex);
  expect(order.deactivateIndex).toBeGreaterThanOrEqual(0);
  expect(order.leaveIndex).toBeGreaterThan(order.deactivateIndex);
  await page.mouse.up();
  await expect.poll(() => page.evaluate((index) => {
    const sent = window.__mockSockets[index].sent.map((raw) => JSON.parse(raw));
    return {
      deactivations: sent.filter((message) => (
        message.GameCommandV2?.command?.command?.DeactivateBoost?.snake_id === 0
      )).length,
      leaves: sent.filter((message) => message === 'LeaveGame').length,
    };
  }, socketIndex)).toEqual({ deactivations: 1, leaves: 1 });
});

test('Boost fuel instrument keeps the Snaketron hierarchy across charge states', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1200 });
  // This visual-state test publishes an already-active authoritative snapshot.
  // Toggle intentionally preserves that durable state; default Hold repairs it.
  await page.addInitScript(() => {
    localStorage.setItem('snaketron:boost-input-mode:v1', 'toggle');
  });

  const emptyFrame = boostSnapshot(10, 5);
  emptyFrame.GameEvent.event.Snapshot.game_state.arena.snakes[0].boost.charge_ms = 0;
  const socketIndex = await establishActiveGame(page, emptyFrame);
  const hud = page.getByTestId('boost-hud');
  const button = page.getByTestId('boost-button');
  const bottle = button.getByTestId('boost-nos-bottle');
  const readBottleSkin = () => bottle.evaluate((element) => {
    const tilt = element.querySelector('.game-boost-meter__canister-tilt');
    const base = element.querySelector('.game-boost-meter__canister-base');
    const body = element.querySelector('.game-boost-meter__canister-body');
    const highlight = element.querySelector('.game-boost-meter__canister-highlight');
    const shade = element.querySelector('.game-boost-meter__canister-shade');
    const separator = element.querySelector('.game-boost-meter__pressure-plate-separator');
    const plate = element.querySelector('.game-boost-meter__pressure-plate');
    if (
      !tilt || !base || !body || !highlight || !shade || !separator || !plate
    ) {
      throw new Error('NOS canister skin is incomplete');
    }
    const transform = tilt.transform.baseVal.consolidate();
    if (!transform) {
      throw new Error('NOS canister slant is missing');
    }
    const bodyBox = body.getBBox();
    const highlightBox = highlight.getBBox();
    const shadeBox = shade.getBBox();
    const separatorBox = separator.getBBox();
    const plateBox = plate.getBBox();
    return {
      angle: Math.atan2(transform.matrix.b, transform.matrix.a) * 180 / Math.PI,
      bodyAspect: bodyBox.width / bodyBox.height,
      bodyFill: getComputedStyle(body).fill,
      separatorFill: getComputedStyle(separator).fill,
      plateFill: getComputedStyle(plate).fill,
      baseFill: getComputedStyle(base).fill,
      highlightFill: getComputedStyle(highlight).fill,
      shadeFill: getComputedStyle(shade).fill,
      facetsHaveArea:
        highlightBox.width > 0 && highlightBox.height > 0
        && shadeBox.width > 0 && shadeBox.height > 0,
      hasEnclosingStroke: element.querySelector('[stroke]') !== null,
      fillPalette: [...new Set(
        [...element.querySelectorAll('[fill]')]
          .map((node) => getComputedStyle(node).fill)
          .filter((fill) => fill !== 'none'),
      )].sort(),
      separatorContainsPlate:
        separatorBox.x < plateBox.x
        && separatorBox.y < plateBox.y
        && separatorBox.x + separatorBox.width > plateBox.x + plateBox.width
        && separatorBox.y + separatorBox.height > plateBox.y + plateBox.height,
    };
  });

  await expect(hud.locator('.game-boost-meter__value')).toHaveText('0%');
  await expect(hud.locator('.game-boost-meter__reservoir')).toHaveCount(1);
  await expect(hud.locator('.game-boost-meter__copy')).toHaveCount(0);
  await expect(hud.locator('.game-boost-meter__charge')).toHaveCount(0);
  await expect(hud).not.toContainText('Boost');
  await expect(hud).not.toContainText('Charge');
  await expect(hud).toHaveAttribute('data-location', 'arena-bottom');
  await expect(hud).toHaveAttribute('data-ready', 'false');
  await expect(hud).not.toHaveClass(/is-ready/);
  // Empty charge still accepts held intent so the next NOS packet can resume
  // Boost without requiring the player to release and press again.
  await expect(button).toBeEnabled();
  await expect(button.locator('.game-boost-meter__canister-dock')).toHaveCount(1);
  await expect(bottle).toHaveAttribute('viewBox', '0 0 34 24');
  await expect(bottle).toHaveAttribute('aria-hidden', 'true');
  await expect(bottle.locator('.game-boost-meter__pressure-plate-separator')).toHaveCount(1);
  await expect(bottle.locator('.game-boost-meter__pressure-plate')).toHaveCount(1);
  await expect(bottle.locator('.game-boost-meter__nos-wordmark')).toHaveText('NOS');
  await expect(bottle.locator('.game-boost-meter__nos-wordmark'))
    .toHaveAttribute('font-style', 'normal');
  await expect(bottle.locator('.game-boost-meter__nos-wordmark'))
    .toHaveAttribute('font-weight', '900');

  const bottleSkin = await readBottleSkin();
  expect(bottleSkin.angle).toBeCloseTo(-24, 1);
  expect(bottleSkin.bodyAspect).toBeGreaterThan(1.5);
  expect(bottleSkin).toMatchObject({
    bodyFill: 'rgb(59, 130, 246)',
    separatorFill: 'rgb(248, 250, 252)',
    plateFill: 'rgb(255, 100, 30)',
    baseFill: 'rgb(59, 130, 246)',
    highlightFill: 'rgb(147, 197, 253)',
    shadeFill: 'rgb(37, 99, 235)',
    facetsHaveArea: true,
    hasEnclosingStroke: false,
    separatorContainsPlate: true,
  });
  expect(bottleSkin.fillPalette).toEqual([
    'rgb(147, 197, 253)',
    'rgb(248, 250, 252)',
    'rgb(255, 100, 30)',
    'rgb(255, 255, 255)',
    'rgb(37, 99, 235)',
    'rgb(59, 130, 246)',
  ]);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await hud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-empty-blue.png`,
    });
  }

  const partialFrame = boostSnapshot(11, 6);
  await emitServerMessage(page, socketIndex, partialFrame);
  await expect(hud).toContainText('33%');
  await expect(hud).toHaveAttribute('data-ready', 'false');
  await expect(hud).not.toHaveClass(/is-ready/);
  await expect(button).toBeEnabled();
  expect(await readBottleSkin()).toEqual(bottleSkin);

  await expect.poll(() => hud.evaluate((element) => {
    const track = element.querySelector('.game-boost-meter__track');
    const fill = element.querySelector('.game-boost-meter__fill');
    if (!track || !fill) {
      throw new Error('Boost fill is incomplete');
    }
    return fill.getBoundingClientRect().width / track.getBoundingClientRect().width;
  })).toBeCloseTo(1 / 3, 2);

  const partialStyle = await hud.evaluate((element) => {
    const button = element.querySelector('.game-boost-meter');
    const percentage = element.querySelector('.game-boost-meter__value');
    const reservoir = element.querySelector('.game-boost-meter__reservoir');
    const track = element.querySelector('.game-boost-meter__track');
    const fill = element.querySelector('.game-boost-meter__fill');
    const dock = element.querySelector('.game-boost-meter__canister-dock');
    const bottleMark = element.querySelector('.game-boost-meter__canister');
    const wordmark = element.querySelector('.game-boost-meter__nos-wordmark');
    const separator = element.querySelector('.game-boost-meter__pressure-plate-separator');
    const pressurePlate = element.querySelector('.game-boost-meter__pressure-plate');
    if (
      !button || !percentage || !reservoir || !track || !fill || !dock ||
      !bottleMark || !wordmark || !separator || !pressurePlate
    ) {
      throw new Error('Boost instrument is incomplete');
    }

    const buttonStyle = getComputedStyle(button);
    const trackStyle = getComputedStyle(track);
    const percentageStyle = getComputedStyle(percentage);
    const reservoirStyle = getComputedStyle(reservoir);
    const buttonRect = button.getBoundingClientRect();
    const trackRect = track.getBoundingClientRect();
    const fillRect = fill.getBoundingClientRect();
    const dockRect = dock.getBoundingClientRect();
    const hudRect = element.getBoundingClientRect();
    const frameRect = element.parentElement.getBoundingClientRect();
    const panelRect = element.parentElement
      .querySelector('.game-arena-panel').getBoundingClientRect();
    const separatorBox = separator.getBBox();
    const pressurePlateBox = pressurePlate.getBBox();
    return {
      background: buttonStyle.backgroundColor,
      border: buttonStyle.borderTopColor,
      boxShadow: buttonStyle.boxShadow,
      backdropFilter: buttonStyle.backdropFilter,
      percentageColor: percentageStyle.color,
      percentageSize: percentageStyle.fontSize,
      percentageStyle: percentageStyle.fontStyle,
      percentageWeight: percentageStyle.fontWeight,
      reservoirDisplay: reservoirStyle.display,
      reservoirOpacity: reservoirStyle.opacity,
      reservoirBackground: reservoirStyle.backgroundImage,
      horizontalInset: frameRect.width - hudRect.width,
      attachmentDelta: hudRect.top - panelRect.bottom,
      trackHeight: getComputedStyle(track).height,
      trackBackground: trackStyle.backgroundImage,
      trackLeftDelta: Math.abs(trackRect.left - buttonRect.left),
      trackRightDelta: Math.abs(trackRect.right - buttonRect.right),
      trackHeightDelta: Math.abs(trackRect.height - buttonRect.height),
      fillStartsAtButtonEdge: Math.abs(fillRect.left - trackRect.left),
      fillCoversBottleDock: fillRect.right >= dockRect.right - 1,
      fillRatio: fillRect.width / trackRect.width,
      fillBackground: getComputedStyle(fill).backgroundImage,
      dockColor: getComputedStyle(dock).backgroundColor,
      imageRendering: getComputedStyle(bottleMark).imageRendering,
      bottleWidth: bottleMark.getBoundingClientRect().width,
      wordmarkWidth: wordmark.getBoundingClientRect().width,
      wordmarkFontStyle: getComputedStyle(wordmark).fontStyle,
      wordmarkFontWeight: getComputedStyle(wordmark).fontWeight,
      separatorSpansPlateWidth: separatorBox.width > pressurePlateBox.width,
      separatorSpansPlateHeight: separatorBox.height > pressurePlateBox.height,
    };
  });
  expect(partialStyle).toMatchObject({
    background: 'rgba(0, 0, 0, 0)',
    border: 'rgba(117, 137, 162, 0.48)',
    backdropFilter: 'none',
    percentageColor: 'rgb(23, 32, 51)',
    percentageSize: '16px',
    percentageStyle: 'italic',
    percentageWeight: '950',
    reservoirDisplay: 'block',
    reservoirOpacity: '0.36',
    trackHeight: '40px',
    dockColor: 'rgba(255, 255, 255, 0.44)',
    imageRendering: 'auto',
    wordmarkFontStyle: 'normal',
    wordmarkFontWeight: '900',
    separatorSpansPlateWidth: true,
    separatorSpansPlateHeight: true,
  });
  expect(partialStyle.boxShadow).not.toBe('none');
  expect(partialStyle.reservoirBackground).toContain('repeating-linear-gradient');
  expect(partialStyle.horizontalInset).toBeCloseTo(20, 1);
  expect(partialStyle.attachmentDelta).toBeCloseTo(-2, 1);
  expect(partialStyle.bottleWidth).toBeGreaterThanOrEqual(37);
  expect(partialStyle.wordmarkWidth).toBeGreaterThanOrEqual(11);
  expect(partialStyle.trackLeftDelta).toBeLessThanOrEqual(1.5);
  expect(partialStyle.trackRightDelta).toBeLessThanOrEqual(1.5);
  expect(partialStyle.trackHeightDelta).toBeLessThanOrEqual(1);
  expect(partialStyle.fillStartsAtButtonEdge).toBeLessThanOrEqual(0.5);
  expect(partialStyle.fillCoversBottleDock).toBe(true);
  expect(partialStyle.fillRatio).toBeCloseTo(1 / 3, 2);
  expect(partialStyle.fillBackground).toContain('linear-gradient');
  expect(partialStyle.trackBackground).toContain('linear-gradient');

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await hud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-partial-blue.png`,
    });
  }

  await button.hover();
  await expect.poll(() => button.evaluate((element) => ({
    background: getComputedStyle(element).backgroundColor,
    border: getComputedStyle(element).borderTopColor,
    dock: getComputedStyle(
      element.querySelector('.game-boost-meter__canister-dock'),
    ).backgroundColor,
  }))).toEqual({
    background: 'rgba(0, 0, 0, 0)',
    border: 'rgba(117, 137, 162, 0.48)',
    dock: 'rgba(255, 255, 255, 0.44)',
  });

  await page.mouse.move(0, 0);
  const fullFrame = boostSnapshot(12, 7);
  const fullState = fullFrame.GameEvent.event.Snapshot.game_state;
  fullState.properties.boost.speed_milli = 2000;
  fullState.arena.snakes[0].boost.charge_ms = 3000;
  await emitServerMessage(page, socketIndex, fullFrame);
  await expect(hud.locator('.game-boost-meter__value')).toHaveText('100%');
  await expect(hud).not.toContainText('2×');
  await expect(hud.locator('.game-boost-meter__charge-label')).toHaveCount(0);
  await expect(hud).toHaveAttribute('data-ready', 'true');
  await expect(hud).toHaveClass(/is-ready/);
  await expect(hud).not.toHaveClass(/is-active/);
  await expect(button).toBeEnabled();
  expect(await readBottleSkin()).toEqual(bottleSkin);

  await expect.poll(() => hud.evaluate((element) => {
    const button = element.querySelector('.game-boost-meter');
    const percentage = element.querySelector('.game-boost-meter__value');
    const fill = element.querySelector('.game-boost-meter__fill');
    const dock = element.querySelector('.game-boost-meter__canister-dock');
    const track = element.querySelector('.game-boost-meter__track');
    if (!button || !percentage || !fill || !dock || !track) {
      throw new Error('Ready Boost instrument is incomplete');
    }

    const buttonStyle = getComputedStyle(button);
    const fillRect = fill.getBoundingClientRect();
    const trackRect = track.getBoundingClientRect();
    return {
      background: buttonStyle.backgroundColor,
      border: buttonStyle.borderTopColor,
      dock: getComputedStyle(dock).backgroundColor,
      percentageColor: getComputedStyle(percentage).color,
      fillRatio: fillRect.width / trackRect.width,
      fillBackground: getComputedStyle(fill).backgroundImage,
    };
  })).toMatchObject({
    background: 'rgba(0, 0, 0, 0)',
    border: 'rgba(117, 137, 162, 0.48)',
    dock: 'rgba(255, 255, 255, 0.28)',
    percentageColor: 'rgb(23, 32, 51)',
    fillRatio: 1,
  });

  await button.hover();
  await expect.poll(() => button.evaluate((element) => ({
    background: getComputedStyle(element).backgroundColor,
    border: getComputedStyle(element).borderTopColor,
    dock: getComputedStyle(
      element.querySelector('.game-boost-meter__canister-dock'),
    ).backgroundColor,
  }))).toEqual({
    background: 'rgba(0, 0, 0, 0)',
    border: 'rgba(117, 137, 162, 0.48)',
    dock: 'rgba(255, 255, 255, 0.28)',
  });

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await hud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-ready-hover.png`,
    });
  }

  await button.focus();
  await expect(button).toBeFocused();
  await expect.poll(() => button.evaluate((element) => (
    getComputedStyle(element).outlineStyle
  ))).toBe('solid');

  await page.mouse.move(0, 0);
  if (process.env.SNAKETRON_VISUAL_DIR) {
    await hud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-ready-yellow.png`,
    });
  }

  // Toggle mode must own the active state this visual fixture is about; an
  // unsolicited active snapshot is correctly repaired back to the local
  // toggle latch instead of remaining active by accident.
  await button.click();
  await page.mouse.move(0, 0);
  const activeFrame = boostSnapshot(13, 8);
  const activeState = activeFrame.GameEvent.event.Snapshot.game_state;
  activeState.properties.boost.speed_milli = 2000;
  activeState.arena.snakes[0].speed_milli = 2000;
  activeState.arena.snakes[0].boost = { charge_ms: 3000, active: true, intent: true };
  await emitServerMessage(page, socketIndex, activeFrame);
  await expect(hud).toHaveClass(/is-active/);
  await expect(hud).not.toHaveClass(/is-ready/);
  await expect(hud).toHaveAttribute('data-ready', 'false');
  await expect(hud.locator('.game-boost-meter__charge-label')).toHaveCount(0);
  await expect(hud.locator('.game-boost-meter__value')).toHaveText(/^\d+%$/);
  await expect(button).toBeEnabled();
  await expect(button).toHaveAccessibleName(/^Stop Boost, \d+% remaining$/);
  expect(await readBottleSkin()).toEqual(bottleSkin);
  await page.waitForTimeout(180);
  const activeStyle = await hud.evaluate((element) => {
    const button = element.querySelector('.game-boost-meter');
    const track = element.querySelector('.game-boost-meter__track');
    const fill = element.querySelector('.game-boost-meter__fill');
    if (!button || !track || !fill) {
      throw new Error('Active Boost instrument is incomplete');
    }
    return {
      background: getComputedStyle(button).backgroundColor,
      trackBackground: getComputedStyle(track).backgroundImage,
      dock: getComputedStyle(
        element.querySelector('.game-boost-meter__canister-dock'),
      ).backgroundColor,
      fillRatio: fill.getBoundingClientRect().width / track.getBoundingClientRect().width,
    };
  });
  expect(activeStyle).toMatchObject({
    background: 'rgba(0, 0, 0, 0)',
    dock: 'rgba(255, 255, 255, 0.44)',
  });
  expect(activeStyle.trackBackground).toContain('linear-gradient');
  expect(activeStyle.fillRatio).toBeGreaterThan(0.5);
  expect(activeStyle.fillRatio).toBeLessThanOrEqual(1);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await hud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-active.png`,
    });
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-snake-active-desktop.png`,
      fullPage: true,
    });
  }
});

test('competitive results settle on the persisted regional ranking and remain visible', async ({ page }) => {
  await page.addInitScript(() => {
    window.__ratingFixture = {
      before: { rank: 18, mmr: 1000, wins: 4, losses: 2, winRate: 66.7 },
      after: { rank: 14, mmr: 1025, wins: 5, losses: 2, winRate: 71.4 },
      requests: [],
      release: false,
    };
  });

  const liveFrame = completedBoostSnapshot(10, 1200);
  const liveState = liveFrame.GameEvent.event.Snapshot.game_state;
  liveState.status = { Started: { server_id: 1 } };
  liveState.queue_mode = 'Competitive';
  liveState.properties.score_limit = 50;
  const socketIndex = await establishActiveGame(page, liveFrame);

  await expect.poll(() => page.evaluate(() => window.__ratingFixture.requests.length)).toBe(1);
  const [baselineUrl] = await page.evaluate(() => window.__ratingFixture.requests);
  const baselineParams = new URL(baselineUrl).searchParams;
  expect(Object.fromEntries(baselineParams)).toEqual({
    queue_mode: 'competitive',
    game_type: 'duel',
    region: 'test-region',
  });

  const finalFrame = completedBoostSnapshot(11, 1201);
  const finalState = finalFrame.GameEvent.event.Snapshot.game_state;
  finalState.start_ms = liveState.start_ms;
  finalState.queue_mode = 'Competitive';
  finalState.properties.score_limit = 50;
  await emitServerMessage(page, socketIndex, finalFrame);

  const scoreCard = page.getByTestId('game-over-card');
  const rating = scoreCard.getByTestId('rating-reveal');
  await expect(scoreCard).toBeVisible();
  await expect(rating).toHaveAttribute('data-phase', 'pending');
  await expect.poll(() => page.evaluate(() => window.__ratingFixture.requests.length)).toBe(2);

  await page.evaluate(() => {
    window.__ratingFixture.release = true;
  });
  await expect(rating).toHaveAttribute('data-phase', 'settled');
  await expect(rating.getByTestId('rating-reveal-value')).toHaveText('1025');
  await expect(rating.getByTestId('rating-reveal-delta')).toHaveText('+25');
  await expect(rating).toBeVisible();
});

test('Snaketron game shell restores the original scoreboard language and free-floating roster', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 1200 });
  const liveFrame = completedBoostSnapshot(10, 1200);
  liveFrame.GameEvent.event.Snapshot.game_state.status = { Started: { server_id: 1 } };
  const socketIndex = await establishActiveGame(page, liveFrame);

  await expect(page.getByTestId('game-scoreboard')).toBeVisible();
  const liveRoster = page.getByTestId('game-roster-band');
  const liveMatchup = page.getByTestId('game-roster-matchup');
  await expect(liveRoster.locator('.game-roster-player[aria-label="You"]')).toBeVisible();
  await expect(liveRoster.locator('.game-roster-player[aria-label="Tron"]')).toBeVisible();
  await expect(liveRoster.getByRole('img', { name: 'You' })).toHaveAttribute('title', 'You');
  await expect(liveRoster.getByRole('img', { name: 'Tron' })).toHaveAttribute('title', 'Tron');
  await expect(liveRoster.locator('.game-roster-spectators')).toHaveCount(0);
  await expect(liveRoster.getByText('Your team', { exact: true }))
    .toHaveCount(0);
  await expect(liveRoster.getByText('Opponents', { exact: true }))
    .toHaveCount(0);
  await expect(liveRoster)
    .toHaveAttribute('aria-label', 'Players and match status');
  await expect(liveMatchup.getByRole('group', { name: 'Your team' }))
    .toBeVisible();
  await expect(liveMatchup.getByRole('group', { name: 'Opponents' }))
    .toBeVisible();
  await expect(liveMatchup.locator('.game-roster-versus-label')).toHaveText('VS');
  await expect(liveRoster.locator('.game-roster-side-score')).toHaveCount(0);
  await expect(liveRoster.locator('.game-roster-player[title]')).toHaveCount(2);
  await expect(liveRoster.locator('.game-roster-snake')).toHaveCount(2);
  await expect(liveRoster.locator('.game-roster-action-slot')).toHaveCount(0);
  await expect(liveRoster.getByRole('button')).toHaveCount(0);
  await expect(page.getByTestId('game-scoreboard').getByRole('button', { name: 'Main menu' }))
    .toHaveCount(0);
  expect(await page.getByTestId('game-scoreboard').locator('.game-scoreboard-brand')
    .evaluate((brand) => brand.tagName)).toBe('DIV');
  await expect(page.getByTestId('game-controls-hint')).toContainText('Move');
  await expect(page.getByTestId('game-controls-hint')).toContainText('Boost');
  await expect(page.locator('.arrow-key-cluster kbd')).toHaveCount(4);
  await expect(page.locator('.control-key--space')).toHaveCount(1);
  await expect(page.getByRole('checkbox', { name: 'Hold to boost' })).toBeVisible();
  await expect(page.getByRole('checkbox', { name: 'Hold to boost' })).toBeChecked();
  await expect(page.getByTestId('boost-hud')).not.toContainText('Space');
  await page.waitForTimeout(240);

  const teamRosterLayout = await page.getByTestId('game-roster-matchup').evaluate((matchup) => {
    const band = matchup.closest('[data-testid="game-roster-band"]');
    const left = matchup.querySelector('.game-roster-side.is-blue');
    const right = matchup.querySelector('.game-roster-side.is-red');
    const versus = matchup.querySelector('.game-roster-versus');
    const leftPlayers = left?.querySelector('.game-roster-players');
    const rightPlayers = right?.querySelector('.game-roster-players');
    const leftPlayer = leftPlayers?.querySelector('.game-roster-player');
    const rightPlayer = rightPlayers?.querySelector('.game-roster-player');
    const leftSnake = leftPlayer?.querySelector('.game-roster-snake');
    const rightSnake = rightPlayer?.querySelector('.game-roster-snake');
    const leftCanvas = leftSnake?.querySelector('.game-roster-snake-canvas');
    const rightCanvas = rightSnake?.querySelector('.game-roster-snake-canvas');
    if (
      !band || !left || !right || !versus || !leftPlayers || !rightPlayers ||
      !leftPlayer || !rightPlayer || !leftSnake || !rightSnake || !leftCanvas || !rightCanvas
    ) {
      throw new Error('symmetric team roster is incomplete');
    }
    const bandRect = band.getBoundingClientRect();
    const matchupRect = matchup.getBoundingClientRect();
    const leftRect = left.getBoundingClientRect();
    const rightRect = right.getBoundingClientRect();
    const versusRect = versus.getBoundingClientRect();
    const versusRule = getComputedStyle(versus, '::before');
    const skin = (snake) => {
      const style = getComputedStyle(snake);
      return [
        style.getPropertyValue('--snake-fill').trim(),
        style.getPropertyValue('--snake-outline').trim(),
      ];
    };
    const canvasProbe = (canvas, facing) => {
      const context = canvas.getContext('2d');
      if (!context) throw new Error('roster canvas has no 2D context');
      const cssWidth = canvas.getBoundingClientRect().width;
      const cssHeight = canvas.getBoundingClientRect().height;
      const scaleX = canvas.width / cssWidth;
      const scaleY = canvas.height / cssHeight;
      const headCenterX = facing === 'right'
        ? cssWidth - cssHeight / 2
        : cssHeight / 2;
      const sample = context.getImageData(
        Math.round(headCenterX * scaleX),
        Math.round((cssHeight / 2) * scaleY),
        1,
        1,
      ).data;
      return {
        backingWidth: canvas.width,
        backingHeight: canvas.height,
        name: canvas.dataset.playerName,
        facing: canvas.dataset.facing,
        headInk: [...sample],
      };
    };
    const leftCanvasProbe = canvasProbe(leftCanvas, 'right');
    const rightCanvasProbe = canvasProbe(rightCanvas, 'left');
    return {
      leftJustification: getComputedStyle(left).justifyContent,
      rightJustification: getComputedStyle(right).justifyContent,
      leftTextAlignment: getComputedStyle(left).textAlign,
      rightTextAlignment: getComputedStyle(right).textAlign,
      matchupCenterDelta: Math.abs(
        (matchupRect.left + matchupRect.width / 2) -
        (bandRect.left + bandRect.width / 2),
      ),
      versusCenterDelta: Math.abs(
        (versusRect.left + versusRect.width / 2) -
        (matchupRect.left + matchupRect.width / 2),
      ),
      sideWidthDelta: Math.abs(leftRect.width - rightRect.width),
      separatorWidth: versusRule.width,
      separatorHeight: Number.parseFloat(versusRule.height),
      scoreCount: matchup.querySelectorAll('.game-roster-side-score').length,
      titledPlayerCount: matchup.querySelectorAll('.game-roster-player[title]').length,
      titledPlayerNames: [...matchup.querySelectorAll('.game-roster-player[title]')]
        .map((player) => player.getAttribute('title')),
      spectatorCount: matchup.querySelectorAll('.game-roster-spectators').length,
      names: [leftCanvasProbe.name, rightCanvasProbe.name],
      leftFacesIn: leftSnake.classList.contains('is-facing-right'),
      rightFacesIn: rightSnake.classList.contains('is-facing-left'),
      snakeWidth: leftSnake.getBoundingClientRect().width,
      snakeHeight: leftSnake.getBoundingClientRect().height,
      canvasCount: matchup.querySelectorAll('.game-roster-snake-canvas').length,
      canvasProbes: [leftCanvasProbe, rightCanvasProbe],
      leftSkin: skin(leftSnake),
      rightSkin: skin(rightSnake),
    };
  });
  expect(teamRosterLayout.leftJustification).toBe('flex-end');
  expect(teamRosterLayout.rightJustification).toBe('flex-start');
  expect(teamRosterLayout.leftTextAlignment).toBe('right');
  expect(teamRosterLayout.rightTextAlignment).toBe('left');
  expect(teamRosterLayout.matchupCenterDelta).toBeLessThanOrEqual(0.5);
  expect(teamRosterLayout.versusCenterDelta).toBeLessThanOrEqual(0.5);
  expect(teamRosterLayout.sideWidthDelta).toBeLessThanOrEqual(0.5);
  expect(teamRosterLayout.separatorWidth).toBe('1px');
  expect(teamRosterLayout.separatorHeight).toBeGreaterThan(24);
  expect(teamRosterLayout.scoreCount).toBe(0);
  expect(teamRosterLayout.titledPlayerCount).toBe(2);
  expect(teamRosterLayout.titledPlayerNames).toEqual(['You', 'Tron']);
  expect(teamRosterLayout.spectatorCount).toBe(0);
  expect(teamRosterLayout.names).toEqual(['You', 'Tron']);
  expect(teamRosterLayout.leftFacesIn).toBe(true);
  expect(teamRosterLayout.rightFacesIn).toBe(true);
  expect(teamRosterLayout.snakeWidth).toBeGreaterThanOrEqual(88);
  expect(teamRosterLayout.snakeHeight).toBeGreaterThanOrEqual(19);
  expect(teamRosterLayout.canvasCount).toBe(2);
  expect(teamRosterLayout.canvasProbes.map((probe) => probe.facing)).toEqual(['right', 'left']);
  expect(teamRosterLayout.canvasProbes.every((probe) => probe.backingWidth > 1)).toBe(true);
  expect(teamRosterLayout.canvasProbes.every((probe) => probe.backingHeight > 1)).toBe(true);
  for (const probe of teamRosterLayout.canvasProbes) {
    expect(probe.headInk[3]).toBeGreaterThan(0);
    expect(probe.headInk.slice(0, 3).every((channel) => channel >= 35 && channel <= 75))
      .toBe(true);
  }
  expect(teamRosterLayout.leftSkin).toEqual(['#70bfe3', '#5299bb']);
  expect(teamRosterLayout.rightSkin).toEqual(['#ff6b6b', '#b84444']);

  const liveLayout = await page.evaluate(() => {
    const scoreboard = document.querySelector('[data-testid="game-scoreboard"]');
    const utilityAnchor = document.querySelector('[data-testid="game-arena-utility-anchor"]');
    const utilityRail = document.querySelector('[data-testid="game-utility-rail"]');
    const roster = document.querySelector('[data-testid="game-roster-band"]');
    const rosterViewport = roster?.querySelector('[data-roster-viewport="true"]');
    const panel = document.querySelector('.game-arena-panel');
    const arenaFrame = panel?.closest('.game-arena-frame');
    const meter = document.querySelector('[data-testid="boost-hud"]');
    const controls = document.querySelector('[data-testid="game-controls-hint"]');
    const controlKey = controls?.querySelector('.control-key');
    const brand = scoreboard?.querySelector('.game-scoreboard-brand');
    const logo = scoreboard?.querySelector('.game-scoreboard-brand img');
    const match = scoreboard?.querySelector('.game-scoreboard-match');
    const clock = scoreboard?.querySelector('.game-scoreboard-time');
    const mode = scoreboard?.querySelector('.game-scoreboard-mode');
    const modeText = mode?.querySelector('strong');
    if (
      !scoreboard || !utilityAnchor || !utilityRail || !roster || !rosterViewport || !panel ||
      !arenaFrame || !meter || !controls || !controlKey || !brand || !logo || !match || !clock ||
      !mode || !modeText
    ) {
      throw new Error('game shell is incomplete');
    }
    const scoreStyle = getComputedStyle(scoreboard);
    const anchorStyle = getComputedStyle(utilityAnchor);
    const rosterStyle = getComputedStyle(roster);
    const panelStyle = getComputedStyle(panel);
    const scoreboardRect = scoreboard.getBoundingClientRect();
    const anchorRect = utilityAnchor.getBoundingClientRect();
    const utilityRect = utilityRail.getBoundingClientRect();
    const rosterRect = roster.getBoundingClientRect();
    const rosterViewportRect = rosterViewport.getBoundingClientRect();
    const panelRect = panel.getBoundingClientRect();
    const meterRect = meter.getBoundingClientRect();
    const controlsRect = controls.getBoundingClientRect();
    const brandRect = brand.getBoundingClientRect();
    const matchRect = match.getBoundingClientRect();
    const clockRect = clock.getBoundingClientRect();
    const modeRect = mode.getBoundingClientRect();
    const modeTextRect = modeText.getBoundingClientRect();
    return {
      scoreboardTop: scoreboardRect.top,
      scoreboardWidth: scoreboardRect.width,
      scoreboardHeight: scoreboardRect.height,
      panelWidth: panelRect.width,
      scoreboardBackground: scoreStyle.backgroundColor,
      scoreboardTopBorder: scoreStyle.borderTopWidth,
      logoWidth: logo.getBoundingClientRect().width,
      outerTrackWidthDelta: Math.abs(brandRect.width - modeRect.width),
      matchCenterDelta: Math.abs(
        (matchRect.left + matchRect.width / 2) -
        (scoreboardRect.left + scoreboardRect.width / 2),
      ),
      clockCenterDelta: Math.abs(
        (clockRect.left + clockRect.width / 2) -
        (scoreboardRect.left + scoreboardRect.width / 2),
      ),
      rosterBackground: rosterStyle.backgroundColor,
      rosterBorder: rosterStyle.borderTopWidth,
      rosterShadow: rosterStyle.boxShadow,
      panelShadow: panelStyle.boxShadow,
      panelBorderWidth: panelStyle.borderTopWidth,
      panelBorderColor: panelStyle.borderTopColor,
      modeCenterDelta: Math.abs(
        (modeRect.left + modeRect.width / 2) -
        (modeTextRect.left + modeTextRect.width / 2),
      ),
      utilityWidthDelta: Math.abs(utilityRect.width - panelRect.width),
      utilityLeftDelta: Math.abs(utilityRect.left - panelRect.left),
      utilityTopGap: utilityRect.top - scoreboardRect.bottom,
      utilityPanelGap: panelRect.top - utilityRect.bottom,
      utilityOwnedByArena: utilityRail.parentElement === utilityAnchor,
      utilityInsideScoreboardShell: Boolean(utilityRail.closest('[data-testid="game-hud-shell"]')),
      anchorMarginTop: Number.parseFloat(anchorStyle.marginTop),
      anchorMarginBottom: Number.parseFloat(anchorStyle.marginBottom),
      anchorPanelGap: panelRect.top - anchorRect.bottom,
      rosterBandCenterDelta: Math.abs(
        (rosterRect.left + rosterRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      ),
      rosterViewportCenterDelta: Math.abs(
        (rosterViewportRect.left + rosterViewportRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      ),
      liveActionCount: roster.querySelectorAll('.game-roster-action-slot').length,
      meterWidthDelta: Math.abs(panelRect.width - meterRect.width),
      meterAttachmentDelta: Math.abs(meterRect.top - panelRect.bottom),
      meterLocation: meter.getAttribute('data-location'),
      meterOwnedByArenaFrame: meter.parentElement === arenaFrame,
      meterOutsideArenaPanel: !meter.closest('.game-arena-panel'),
      meterHeight: meterRect.height,
      meterWidth: meterRect.width,
      controlsBottomGap: window.innerHeight - controlsRect.bottom,
      controlsTopGap: controlsRect.top - meterRect.bottom,
      controlKeyHeight: controlKey.getBoundingClientRect().height,
      controlsColor: getComputedStyle(controls).color,
    };
  });
  expect(liveLayout.scoreboardTop).toBeLessThanOrEqual(-2);
  expect(Math.abs(liveLayout.scoreboardWidth - liveLayout.panelWidth)).toBeLessThanOrEqual(1);
  expect(liveLayout.scoreboardHeight).toBeGreaterThanOrEqual(63);
  expect(liveLayout.scoreboardBackground).toBe('rgb(255, 255, 255)');
  expect(liveLayout.scoreboardTopBorder).toBe('0px');
  expect(liveLayout.logoWidth).toBeGreaterThan(90);
  expect(liveLayout.outerTrackWidthDelta).toBeLessThanOrEqual(0.5);
  expect(liveLayout.matchCenterDelta).toBeLessThanOrEqual(0.5);
  expect(liveLayout.clockCenterDelta).toBeLessThanOrEqual(0.5);
  expect(liveLayout.rosterBackground).toBe('rgba(0, 0, 0, 0)');
  expect(liveLayout.rosterBorder).toBe('0px');
  expect(liveLayout.rosterShadow).toBe('none');
  expect(liveLayout.panelShadow).toBe('none');
  expect(liveLayout.panelBorderWidth).toBe('1px');
  expect(liveLayout.panelBorderColor).toBe('rgba(0, 0, 0, 0.8)');
  expect(liveLayout.modeCenterDelta).toBeLessThanOrEqual(1);
  expect(liveLayout.utilityWidthDelta).toBeLessThanOrEqual(1);
  expect(liveLayout.utilityLeftDelta).toBeLessThanOrEqual(1);
  expect(liveLayout.utilityTopGap).toBeGreaterThanOrEqual(6);
  expect(liveLayout.utilityPanelGap).toBeGreaterThanOrEqual(0);
  expect(liveLayout.utilityOwnedByArena).toBe(true);
  expect(liveLayout.utilityInsideScoreboardShell).toBe(false);
  expect(liveLayout.anchorMarginTop).toBe(liveLayout.anchorMarginBottom);
  expect(Math.abs(liveLayout.anchorPanelGap - liveLayout.anchorMarginBottom)).toBeLessThanOrEqual(1);
  expect(liveLayout.rosterBandCenterDelta).toBeLessThanOrEqual(0.5);
  expect(liveLayout.rosterViewportCenterDelta).toBeLessThanOrEqual(0.5);
  expect(liveLayout.liveActionCount).toBe(0);
  expect(liveLayout.meterLocation).toBe('arena-bottom');
  expect(liveLayout.meterOwnedByArenaFrame).toBe(true);
  expect(liveLayout.meterWidthDelta).toBeCloseTo(20, 1);
  expect(liveLayout.meterAttachmentDelta).toBeCloseTo(2, 1);
  expect(liveLayout.meterOutsideArenaPanel).toBe(true);
  expect(liveLayout.meterHeight).toBeGreaterThanOrEqual(39);
  expect(liveLayout.meterWidth).toBeGreaterThanOrEqual(579);
  expect(liveLayout.controlsBottomGap).toBeGreaterThanOrEqual(20);
  expect(liveLayout.controlsTopGap).toBeGreaterThanOrEqual(24);
  expect(liveLayout.controlKeyHeight).toBeGreaterThanOrEqual(18);
  expect(liveLayout.controlsColor).toBe('rgba(23, 32, 51, 0.58)');

  const desktopNos = await readNosCanvasRegions(page, 40, [
    { name: 'regular', x: 12, y: 33, sizeCells: 1 },
    { name: 'full', x: 4, y: 14, sizeCells: 2 },
    { name: 'cooling', x: 34, y: 44, sizeCells: 2 },
  ]);
  expect(desktopNos.cellSize).toBe(15);
  expectNosPickupIdentity(desktopNos.samples.regular);
  expectNosPickupIdentity(desktopNos.samples.full);
  // Rasterizers classify edge pixels differently at small canvas scales. The
  // authored 2x2 bottle still needs a clearly larger color footprint without
  // requiring an exact 2:1 classified-pixel ratio from every browser.
  expect(desktopNos.samples.full.total)
    .toBeGreaterThan(desktopNos.samples.regular.total * 1.5);
  expect(desktopNos.samples.cooling.counts.blue).toBe(0);
  expect(desktopNos.samples.cooling.counts.orange).toBe(0);
  expect(desktopNos.samples.wholeArena.oldLightningYellow).toBe(0);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-shell-desktop.png`,
      fullPage: true,
    });
  }

  const finalFrame = completedBoostSnapshot(11, 1201);
  finalFrame.GameEvent.event.Snapshot.game_state.start_ms =
    liveFrame.GameEvent.event.Snapshot.game_state.start_ms;
  await emitServerMessage(page, socketIndex, finalFrame);
  const scoreCard = page.getByTestId('game-over-card');
  await expect(scoreCard).toBeVisible();
  // Let completion effects flush: a Quickmatch result must never mount the
  // competitive rating placeholder while its score card is open.
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  expect(await scoreCard.getByTestId('rating-reveal').count()).toBe(0);
  await expect(page.getByTestId('game-roster-band').getByRole('button')).toHaveCount(2);
  await expect(page.getByTestId('game-roster-band')
    .getByRole('button', { name: 'Score card' })).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByTestId('game-scoreboard').getByRole('button', { name: 'Main menu' }))
    .toHaveCount(1);
  await expect(scoreCard).toContainText('Victory');
  await expect(scoreCard.locator('.game-over-scoreline')).toHaveText('1–0');
  await expect(scoreCard).toContainText('+110');
  await expect(scoreCard.getByRole('button', { name: 'Main menu' })).toBeVisible();
  await expect(scoreCard.getByRole('button', { name: 'Play again' })).toBeVisible();
  await expect(scoreCard.locator('.game-over-shortcut')).toContainText(/Space\s*to/i);
  await expect(scoreCard.locator('.game-over-play-chevron')).toHaveCount(1);
  await expect(scoreCard.locator('.game-start-chevrons.is-left')).toHaveCount(0);
  await expect(scoreCard.locator('.game-start-enable-sweep')).toHaveCount(1);
  await expect(scoreCard.locator('.game-start-enable-chevron')).toHaveCount(2);
  await expect(scoreCard.locator('.game-over-statline > div')).toHaveCount(5);
  await expect(scoreCard.locator('.game-over-statline')).toContainText('Time taken');
  await expect(scoreCard.locator('.game-over-statline')).toContainText('01:00');
  await expect(scoreCard.locator('.game-over-statline strong.is-ppm')).toHaveText('2.0');
  await expect(scoreCard.locator('.game-over-statline strong.is-apm')).toHaveText('36.0');
  await expect(scoreCard.locator('.game-over-standings-heading').getByText('Player', { exact: true }))
    .toBeVisible();
  await expect(scoreCard.locator('.game-over-standings-heading').getByText('Points', { exact: true }))
    .toBeVisible();
  await expect(scoreCard.getByText('Final field', { exact: true })).toHaveCount(0);

  const completedHudLayout = await page.evaluate(() => {
    const backdrop = document.querySelector('[data-testid="game-over-backdrop"]');
    const hudShell = document.querySelector('[data-testid="game-hud-shell"]');
    const roster = document.querySelector('[data-testid="game-roster-band"]');
    const rosterViewport = roster?.querySelector('[data-roster-viewport="true"]');
    const versus = roster?.querySelector('.game-roster-versus');
    const leftSlot = roster?.querySelector('.game-roster-action-slot.is-left');
    const rightSlot = roster?.querySelector('.game-roster-action-slot.is-right');
    const leftActions = leftSlot?.querySelector('.game-roster-actions.is-left');
    const rightActions = rightSlot?.querySelector('.game-roster-actions.is-right');
    const menu = leftActions?.querySelector('button');
    const scoreCardButton = rightActions?.querySelector('button');
    if (
      !backdrop || !hudShell || !roster || !rosterViewport || !versus || !leftSlot ||
      !rightSlot || !leftActions || !rightActions || !menu || !scoreCardButton
    ) {
      throw new Error('completed-game HUD is incomplete');
    }
    const backdropRect = backdrop.getBoundingClientRect();
    const rosterRect = roster.getBoundingClientRect();
    const viewportRect = rosterViewport.getBoundingClientRect();
    const versusRect = versus.getBoundingClientRect();
    const leftSlotRect = leftSlot.getBoundingClientRect();
    const rightSlotRect = rightSlot.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    const scoreCardRect = scoreCardButton.getBoundingClientRect();
    return {
      backdropTop: backdropRect.top,
      backdropBottom: backdropRect.bottom,
      viewportBottom: window.innerHeight,
      backdropZ: Number.parseInt(getComputedStyle(backdrop).zIndex, 10),
      hudZ: Number.parseInt(getComputedStyle(hudShell).zIndex, 10),
      rosterBottom: rosterRect.bottom,
      matchupCenterDelta: Math.abs(
        (viewportRect.left + viewportRect.width / 2) -
        (rosterRect.left + rosterRect.width / 2),
      ),
      versusCenterDelta: Math.abs(
        (versusRect.left + versusRect.width / 2) -
        (rosterRect.left + rosterRect.width / 2),
      ),
      actionSlotWidthDelta: Math.abs(leftSlotRect.width - rightSlotRect.width),
      menuPrecedesMatchup: menuRect.right <= viewportRect.left + 0.5,
      scoreCardFollowsMatchup: scoreCardRect.left >= viewportRect.right - 0.5,
      leftActionVisibility: getComputedStyle(leftActions).visibility,
      rightActionVisibility: getComputedStyle(rightActions).visibility,
      leftActionPointerEvents: getComputedStyle(leftActions).pointerEvents,
      rightActionPointerEvents: getComputedStyle(rightActions).pointerEvents,
    };
  });
  expect(completedHudLayout.backdropTop).toBe(0);
  expect(completedHudLayout.backdropBottom).toBe(completedHudLayout.viewportBottom);
  expect(completedHudLayout.backdropZ).toBeGreaterThan(completedHudLayout.hudZ);
  expect(completedHudLayout.rosterBottom).toBeGreaterThan(completedHudLayout.backdropTop);
  expect(completedHudLayout.matchupCenterDelta).toBeLessThanOrEqual(0.5);
  expect(completedHudLayout.versusCenterDelta).toBeLessThanOrEqual(0.5);
  expect(completedHudLayout.actionSlotWidthDelta).toBeLessThanOrEqual(0.5);
  expect(completedHudLayout.menuPrecedesMatchup).toBe(true);
  expect(completedHudLayout.scoreCardFollowsMatchup).toBe(true);
  expect(completedHudLayout.leftActionVisibility).toBe('visible');
  expect(completedHudLayout.rightActionVisibility).toBe('visible');
  expect(completedHudLayout.leftActionPointerEvents).toBe('auto');
  expect(completedHudLayout.rightActionPointerEvents).toBe('auto');
  await expect(page.getByTestId('boost-hud')).toHaveCount(0);

  const persistentScoreCardButton = page.getByTestId('game-roster-band')
    .getByRole('button', { name: 'Score card' });
  await scoreCard.getByRole('button', { name: 'Close score card' }).click();
  await expect(scoreCard).toHaveCount(0);
  await expect(persistentScoreCardButton).toHaveAttribute('aria-expanded', 'false');

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-complete-roster-desktop.png`,
      fullPage: true,
    });
  }

  expect(await persistentScoreCardButton.evaluate((button) => {
    const style = getComputedStyle(button);
    return [style.backgroundColor, style.borderTopColor, style.color];
  })).toEqual([
    'rgb(255, 255, 255)',
    'rgb(156, 163, 175)',
    'rgb(63, 63, 65)',
  ]);

  await persistentScoreCardButton.hover();
  await expect.poll(() => persistentScoreCardButton.evaluate((button) => {
    const style = getComputedStyle(button);
    return [style.backgroundColor, style.borderTopColor];
  })).toEqual(['rgb(239, 246, 255)', 'rgb(59, 130, 246)']);
  await page.mouse.move(0, 0);
  await persistentScoreCardButton.click();
  await expect(scoreCard).toBeVisible();
  await expect(persistentScoreCardButton).toHaveAttribute('aria-expanded', 'true');

  const resultDetails = await scoreCard.evaluate((card) => {
    const xp = card.querySelector('.game-over-statline strong.is-xp');
    const score = card.querySelector('.game-over-statline strong.is-score');
    const time = card.querySelector('.game-over-statline strong.is-time');
    const heading = card.querySelector('.game-over-standings-heading');
    const currentRow = card.querySelector('.game-over-player');
    if (!xp || !score || !time || !heading || !currentRow) {
      throw new Error('score card result details are incomplete');
    }
    return {
      xpColor: getComputedStyle(xp).color,
      scoreColor: getComputedStyle(score).color,
      timeColor: getComputedStyle(time).color,
      headingFontStyle: getComputedStyle(heading).fontStyle,
      currentRowBackground: getComputedStyle(currentRow).backgroundColor,
    };
  });
  expect(resultDetails).toEqual({
    xpColor: 'rgb(212, 154, 0)',
    scoreColor: 'rgb(22, 163, 74)',
    timeColor: 'rgb(63, 63, 65)',
    headingFontStyle: 'normal',
    currentRowBackground: 'rgba(0, 0, 0, 0)',
  });

  const ppmHelp = scoreCard.getByRole('button', { name: 'About points per minute' });
  const apmHelp = scoreCard.getByRole('button', { name: 'About accepted actions per minute' });
  const ppmTooltip = scoreCard.locator('[role="tooltip"]')
    .filter({ hasText: 'Points per minute:' });
  const apmTooltip = scoreCard.locator('[role="tooltip"]')
    .filter({ hasText: 'Accepted actions per minute:' });
  await ppmHelp.hover();
  await expect(ppmTooltip).toBeVisible();
  await expect(ppmTooltip).toContainText('final points divided by the elapsed match time');
  await page.mouse.move(0, 0);
  await apmHelp.focus();
  await expect(apmTooltip).toBeVisible();
  await expect(apmTooltip).toContainText('valid turns and successful Boost starts or manual stops');
  await expect(apmTooltip).toContainText('Retries, rejected inputs, and no-ops are excluded');

  const cardStyle = await scoreCard.evaluate((card) => {
    const style = getComputedStyle(card);
    const header = card.querySelector('.game-over-header');
    const titleSection = card.querySelector('.game-over-title-section');
    const title = titleSection?.querySelector('h2');
    const summary = titleSection?.querySelector('p');
    const scorePanel = card.querySelector('.game-over-score-panel');
    const jewel = card.querySelector('.game-over-jewel');
    const primary = card.querySelector('.game-shell-button.is-primary');
    const footer = card.querySelector('.game-over-actions');
    const menu = card.querySelector('.game-shell-button.is-menu');
    const replay = card.querySelector('.game-over-replay-actions');
    const motionChevron = card.querySelector('.game-start-enable-chevron.is-primary');
    const playContent = card.querySelector('.game-over-play-content');
    const close = card.querySelector('.game-over-close');
    const shortcutKeyText = card.querySelector('.game-over-shortcut kbd > span');
    const metricHelpGlyph = card.querySelector('.game-over-metric-help-mark');
    if (!header || !titleSection || !title || !summary || !scorePanel || !jewel || !primary ||
        !footer || !menu || !replay || !motionChevron || !playContent || !close ||
        !shortcutKeyText || !metricHelpGlyph) {
      throw new Error('score card actions are incomplete');
    }
    const headerRect = header.getBoundingClientRect();
    const titleRect = titleSection.getBoundingClientRect();
    const scoreRect = scorePanel.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    const replayRect = replay.getBoundingClientRect();
    const primaryStyle = getComputedStyle(primary);
    const menuStyle = getComputedStyle(menu);
    return {
      width: card.getBoundingClientRect().width,
      background: style.backgroundColor,
      borderTop: style.borderTopWidth,
      borderBottom: style.borderBottomWidth,
      shadow: style.boxShadow,
      headerHeight: headerRect.height,
      headerBackground: getComputedStyle(header).backgroundColor,
      titleBackground: getComputedStyle(titleSection).backgroundColor,
      titleColor: getComputedStyle(title).color,
      titleFontFamily: getComputedStyle(title).fontFamily,
      titleShadow: getComputedStyle(title).textShadow,
      titleScoreSeparated: titleRect.right <= scoreRect.left + 0.5,
      summaryBackground: getComputedStyle(summary).backgroundImage,
      summaryColor: getComputedStyle(summary).color,
      summaryFontSize: getComputedStyle(summary).fontSize,
      summaryFontStyle: getComputedStyle(summary).fontStyle,
      summaryFontWeight: getComputedStyle(summary).fontWeight,
      summaryPadding: getComputedStyle(summary).paddingTop,
      summaryPaddingLeft: getComputedStyle(summary).paddingLeft,
      summaryMarginTop: getComputedStyle(summary).marginTop,
      summaryMarginLeft: getComputedStyle(summary).marginLeft,
      scoreColor: getComputedStyle(scorePanel).color,
      scoreBackground: getComputedStyle(scorePanel).backgroundImage,
      scoreClip: getComputedStyle(scorePanel).clipPath,
      scoreBackdropFilter: getComputedStyle(scorePanel).backdropFilter,
      scoreBlendMode: getComputedStyle(scorePanel).mixBlendMode,
      scoreWidthRatio: scoreRect.width / headerRect.width,
      jewelArtwork: jewel.getAttribute('data-result-artwork'),
      jewelHidden: jewel.getAttribute('aria-hidden'),
      jewelLayerCount: jewel.children.length,
      jewelBackground: getComputedStyle(jewel).backgroundImage,
      jewelTransform: getComputedStyle(jewel).transform,
      jewelWidthRatio: Number.parseFloat(getComputedStyle(jewel).width) / headerRect.width,
      closeBackground: getComputedStyle(close).backgroundColor,
      closeShadow: getComputedStyle(close).boxShadow,
      statLabelFontStyles: Array.from(card.querySelectorAll('.game-over-statline > div > span'))
        .map((label) => getComputedStyle(label).fontStyle),
      statLabelFontSizes: Array.from(card.querySelectorAll('.game-over-statline > div > span'))
        .map((label) => getComputedStyle(label).fontSize),
      statTileAlignments: Array.from(card.querySelectorAll('.game-over-statline > div'))
        .map((tile) => [getComputedStyle(tile).alignItems, getComputedStyle(tile).textAlign]),
      primaryBackground: primaryStyle.backgroundColor,
      primaryFontSize: primaryStyle.fontSize,
      primaryShadow: primaryStyle.boxShadow,
      primaryOverflow: primaryStyle.overflow,
      menuBackground: menuStyle.backgroundColor,
      menuBorder: menuStyle.borderTopColor,
      menuFontStyle: menuStyle.fontStyle,
      menuFontSize: menuStyle.fontSize,
      shortcutTextTransform: getComputedStyle(shortcutKeyText).transform,
      metricHelpWidth: getComputedStyle(metricHelpGlyph).width,
      metricHelpHeight: getComputedStyle(metricHelpGlyph).height,
      metricHelpTransform: getComputedStyle(metricHelpGlyph).transform,
      motionAnimation: getComputedStyle(motionChevron).animationName,
      staticChevronIsLast: playContent.lastElementChild?.classList.contains('game-over-play-chevron'),
      footerRule: getComputedStyle(footer).borderTopWidth,
      menuLeft: menuRect.left,
      replayLeft: replayRect.left,
    };
  });
  expect(cardStyle.width).toBeLessThanOrEqual(565);
  expect(cardStyle.background).toBe('rgb(255, 255, 255)');
  expect(cardStyle.borderTop).toBe('1px');
  expect(cardStyle.borderBottom).toBe('1px');
  expect(cardStyle.shadow).not.toContain('inset');
  expect(cardStyle.headerHeight).toBe(124);
  expect(cardStyle.headerBackground).toBe('rgb(255, 255, 255)');
  expect(cardStyle.titleBackground).toBe('rgba(0, 0, 0, 0)');
  expect(cardStyle.titleColor).toBe('rgb(255, 255, 255)');
  expect(cardStyle.titleFontFamily).not.toContain('Archivo');
  expect(cardStyle.titleShadow).toBe(
    'rgba(20, 24, 31, 0.5) 2px 2px 0px, rgba(20, 24, 31, 0.5) 4px 4px 0px',
  );
  expect(cardStyle.titleScoreSeparated).toBe(true);
  expect(cardStyle.summaryBackground).toContain('linear-gradient');
  expect(cardStyle.summaryBackground).toContain('rgba(0, 0, 0, 0.7)');
  expect(cardStyle.summaryColor).toBe('rgb(255, 255, 255)');
  expect(cardStyle.summaryFontSize).toBe('13px');
  expect(cardStyle.summaryFontStyle).toBe('normal');
  expect(cardStyle.summaryFontWeight).toBe('400');
  expect(cardStyle.summaryPadding).toBe('5px');
  expect(cardStyle.summaryPaddingLeft).toBe('22px');
  expect(cardStyle.summaryMarginTop).toBe('8px');
  expect(cardStyle.summaryMarginLeft).toBe('-20px');
  expect(cardStyle.scoreColor).toBe('rgb(20, 24, 31)');
  expect(cardStyle.scoreBackground).toBe('none');
  expect(cardStyle.scoreClip).toBe('none');
  expect(cardStyle.scoreBackdropFilter).toBe('none');
  expect(cardStyle.scoreBlendMode).toBe('normal');
  expect(cardStyle.scoreWidthRatio).toBeCloseTo(160 / 520, 2);
  expect(cardStyle.jewelArtwork).toBe('azure-cut');
  expect(cardStyle.jewelHidden).toBe('true');
  expect(cardStyle.jewelLayerCount).toBe(11);
  expect(cardStyle.jewelBackground).toContain('linear-gradient');
  expect(cardStyle.jewelTransform).not.toBe('none');
  expect(cardStyle.jewelWidthRatio).toBeCloseTo(430 / 520, 2);
  expect(cardStyle.closeBackground).toBe('rgb(255, 255, 255)');
  expect(cardStyle.closeShadow).toBe('none');
  expect(cardStyle.statLabelFontStyles).toEqual(['normal', 'normal', 'normal', 'normal', 'normal']);
  expect(cardStyle.statLabelFontSizes).toEqual(['8.5px', '8.5px', '8.5px', '8.5px', '8.5px']);
  expect(cardStyle.statTileAlignments).toEqual([
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
  ]);
  expect(cardStyle.primaryBackground).toBe('rgb(59, 130, 246)');
  expect(cardStyle.primaryFontSize).toBe('11px');
  expect(cardStyle.primaryShadow).toBe('none');
  expect(cardStyle.primaryOverflow).toBe('hidden');
  expect(cardStyle.menuBackground).toBe('rgb(255, 255, 255)');
  expect(cardStyle.menuBorder).toBe('rgb(156, 163, 175)');
  expect(cardStyle.menuFontStyle).toBe('normal');
  expect(cardStyle.menuFontSize).toBe('11px');
  expect(cardStyle.shortcutTextTransform).toContain('matrix');
  expect(cardStyle.metricHelpWidth).toBe('12px');
  expect(cardStyle.metricHelpHeight).toBe('12px');
  expect(cardStyle.metricHelpTransform).toBe('none');
  expect(cardStyle.motionAnimation).toContain('game-start-enable-sweep-motion');
  expect(cardStyle.staticChevronIsLast).toBe(true);
  expect(cardStyle.footerRule).toBe('1px');
  expect(cardStyle.replayLeft).toBeGreaterThan(cardStyle.menuLeft);
  await page.mouse.move(0, 0);
  await page.evaluate(() => {
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  });
  await expect(apmTooltip).toBeHidden();

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await scoreCard.locator('.game-over-header').screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-over-azure-cut.png`,
    });
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-over-desktop.png`,
      fullPage: true,
    });
  }

  const menuButton = scoreCard.getByRole('button', { name: 'Main menu' });
  await menuButton.hover();
  await expect.poll(() => menuButton.evaluate((button) => {
    const style = getComputedStyle(button);
    return [style.backgroundColor, style.borderTopColor];
  })).toEqual(['rgb(239, 246, 255)', 'rgb(59, 130, 246)']);

  await page.mouse.move(0, 0);
  // Programmatic focus verifies the same visible keyboard treatment without
  // depending on the host OS's WebKit full-keyboard-access preference.
  await page.keyboard.press('Tab');
  await menuButton.focus();
  await expect(menuButton).toBeFocused();
  await expect.poll(() => menuButton.evaluate((button) => {
    const style = getComputedStyle(button);
    return [style.backgroundColor, style.borderTopColor, style.fontStyle];
  })).toEqual(['rgb(239, 246, 255)', 'rgb(59, 130, 246)', 'normal']);

  await page.emulateMedia({ reducedMotion: 'reduce' });
  await expect.poll(() => scoreCard.locator('.game-start-enable-chevron.is-primary')
    .evaluate((chevron) => getComputedStyle(chevron).animationName)).toBe('none');
  await page.emulateMedia({ reducedMotion: 'no-preference' });

  await page.emulateMedia({ forcedColors: 'active' });
  await expect(scoreCard.locator('.game-over-jewel')).toHaveCSS('display', 'none');
  const forcedColorHeader = await scoreCard.evaluate((card) => {
    const heading = card.querySelector('.game-over-title-section h2');
    const score = card.querySelector('.game-over-scoreline');
    if (!heading || !score) throw new Error('forced-color result text is missing');
    return {
      headingInk: getComputedStyle(heading).color,
      scoreInk: getComputedStyle(score).color,
      headingShadow: getComputedStyle(heading).textShadow,
    };
  });
  expect(forcedColorHeader.headingInk).toBe(forcedColorHeader.scoreInk);
  expect(forcedColorHeader.headingShadow).toBe('none');
  await page.emulateMedia({ forcedColors: 'none' });

  await scoreCard.getByRole('button', { name: 'Close score card' }).click();
  await expect(scoreCard).toHaveCount(0);
  const roster = page.getByTestId('game-roster-band');
  const rosterMenu = roster.getByRole('button', { name: 'Menu', exact: true });
  const rosterScoreCard = roster.getByRole('button', { name: 'Score card' });
  const scoreboardHome = page.getByTestId('game-scoreboard')
    .getByRole('button', { name: 'Main menu' });
  await expect(roster.getByRole('button')).toHaveCount(2);
  await expect(rosterMenu).toBeVisible();
  await expect(rosterScoreCard).toBeVisible();
  await expect(scoreboardHome).toBeVisible();
  expect(await scoreboardHome.evaluate((brand) => brand.tagName)).toBe('BUTTON');
  await scoreboardHome.focus();
  await expect(scoreboardHome).toBeFocused();
  await rosterScoreCard.click();
  await expect(page.getByTestId('game-over-card')).toBeVisible();
  await expect(page.getByTestId('game-over-card').getByRole('button', { name: 'Play again' }))
    .toBeFocused();
  await page.keyboard.press('Space');
  await expect.poll(() => socketMessages(page, socketIndex, 'QueueForMatch')).toHaveLength(1);
  await emitServerMessage(page, socketIndex, {
    LobbyUpdate: { ...lobbyUpdate.LobbyUpdate, state: 'queued' },
  });
  await expect(scoreCard.getByRole('button', { name: /Queued/ })).toBeDisabled();
  await expect(scoreCard.getByRole('button', { name: 'Close score card' })).toBeFocused();
});

test('2v2 roster stacks full-size inward staredown snakes with names in their bodies', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  const frame = boostSnapshot(10, 6);
  frame.GameEvent.event.Snapshot.game_state = fourSnakeBoostState(6);
  await establishActiveGame(page, frame);

  const roster = page.getByTestId('game-roster-band');
  await expect(roster.locator('.game-roster-snake')).toHaveCount(4);
  await expect(roster.locator('.game-roster-spectators')).toHaveCount(0);
  const layout = await roster.evaluate((element) => {
    const left = [...element.querySelectorAll('.game-roster-side.is-blue .game-roster-snake')];
    const right = [...element.querySelectorAll('.game-roster-side.is-red .game-roster-snake')];
    const names = (snakes) => snakes.map((snake) => (
      snake.querySelector('.game-roster-snake-canvas')?.dataset.playerName
    ));
    const skins = (snakes) => snakes.map((snake) => {
      const style = getComputedStyle(snake);
      return [
        style.getPropertyValue('--snake-fill').trim(),
        style.getPropertyValue('--snake-outline').trim(),
      ];
    });
    const leftRects = left.map((snake) => snake.getBoundingClientRect());
    const rightRects = right.map((snake) => snake.getBoundingClientRect());
    return {
      leftNames: names(left),
      rightNames: names(right),
      leftSkins: skins(left),
      rightSkins: skins(right),
      leftStacked: leftRects[1].top > leftRects[0].top,
      rightStacked: rightRects[1].top > rightRects[0].top,
      inwardHeads:
        left.every((snake) => snake.classList.contains('is-facing-right')) &&
        right.every((snake) => snake.classList.contains('is-facing-left')),
      minWidth: Math.min(...[...leftRects, ...rightRects].map((rect) => rect.width)),
      maxBottom: Math.max(...[...leftRects, ...rightRects].map((rect) => rect.bottom)),
      rosterBottom: element.getBoundingClientRect().bottom,
      overflow: element.scrollWidth - element.clientWidth,
    };
  });
  expect(layout.leftNames).toEqual(['You', 'three']);
  expect(layout.rightNames).toEqual(['two', 'four']);
  expect(layout.leftSkins).toEqual([
    ['#70bfe3', '#5299bb'],
    ['#3c8dde', '#286eae'],
  ]);
  expect(layout.rightSkins).toEqual([
    ['#ff6b6b', '#b84444'],
    ['#e34e5b', '#a92f3a'],
  ]);
  expect(layout.leftStacked).toBe(true);
  expect(layout.rightStacked).toBe(true);
  expect(layout.inwardHeads).toBe(true);
  expect(layout.minWidth).toBeGreaterThanOrEqual(88);
  expect(layout.maxBottom).toBeLessThanOrEqual(layout.rosterBottom + 0.5);
  expect(layout.overflow).toBeLessThanOrEqual(0);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-roster-2v2.png`,
      fullPage: true,
    });
  }
});

test('game report uses the four specified jewel designs for their exact outcomes', async ({ page }) => {
  const initial = completedBoostSnapshot(10, 1200);
  initial.GameEvent.event.Snapshot.game_state.status = { Started: { server_id: 1 } };
  let matchStart = initial.GameEvent.event.Snapshot.game_state.start_ms;
  let socketIndex = await establishActiveGame(page, initial);

  const variants = [
    {
      title: 'Victory',
      artwork: 'azure-cut',
      layers: 11,
      prepare(state) {
        state.status = { Complete: { winning_snake_id: 0 } };
      },
    },
    {
      title: 'Defeat',
      artwork: 'ruby-shatter',
      layers: 11,
      prepare(state) {
        state.status = { Complete: { winning_snake_id: 1 } };
      },
    },
    {
      title: 'Draw',
      artwork: 'topaz-cut',
      layers: 10,
      prepare(state) {
        state.status = { Complete: { winning_snake_id: null } };
      },
    },
    {
      title: 'Run complete',
      artwork: 'jade-fracture',
      layers: 8,
      prepare(state) {
        state.game_type = 'Solo';
        state.arena.snakes = [state.arena.snakes[0]];
        state.arena.snakes[0].team_id = null;
        state.arena.boost_pads = [];
        state.arena.team_zone_config = null;
        state.players = { 7: { user_id: 7, snake_id: 0 } };
        state.usernames = { 7: 'drain-tester' };
        state.spectators = [];
        state.scores = { 0: 10 };
        state.team_scores = null;
        state.player_xp = { 7: 110 };
        state.player_action_counts = { 7: 36 };
        state.status = { Complete: { winning_snake_id: 0 } };
      },
    },
  ];

  let streamSequence = 11;
  let tick = 1201;
  for (const [index, variant] of variants.entries()) {
    let needsManualOpen = index > 0;
    if (variant.artwork === 'jade-fracture') {
      const soloInitial = snapshot(10, 1200);
      variant.prepare(soloInitial.GameEvent.event.Snapshot.game_state);
      soloInitial.GameEvent.event.Snapshot.game_state.status = { Started: { server_id: 1 } };
      matchStart = soloInitial.GameEvent.event.Snapshot.game_state.start_ms;
      socketIndex = await establishActiveGame(page, soloInitial);
      await expect(page.getByTestId('boost-hud').locator('.game-boost-meter__value'))
        .toHaveText('∞');
      await expect(page.getByTestId('boost-button')).toHaveAccessibleName(/unlimited/i);
      await expect(page.getByTestId('game-roster-band').locator('.game-roster-field-label'))
        .toHaveCount(0);
      await expect(page.getByTestId('game-roster-band')
        .locator('.game-roster-player[aria-label="You"]')).toBeVisible();
      await expect(page.getByTestId('game-scoreboard').locator('.game-scoreboard-mode'))
        .toHaveText('Solo run');
      const soloModeCenterDelta = await page.getByTestId('game-scoreboard')
        .locator('.game-scoreboard-mode')
        .evaluate((mode) => {
          const label = mode.querySelector('strong');
          if (!label) throw new Error('solo mode label is missing');
          const modeRect = mode.getBoundingClientRect();
          const labelRect = label.getBoundingClientRect();
          return Math.abs(
            (modeRect.left + modeRect.width / 2) -
            (labelRect.left + labelRect.width / 2),
          );
        });
      expect(soloModeCenterDelta).toBeLessThanOrEqual(1);
      await expect(page.getByTestId('game-roster-band')).not.toContainText('Field');

      if (process.env.SNAKETRON_VISUAL_DIR) {
        await page.waitForTimeout(220);
        await page.screenshot({
          path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-shell-solo.png`,
          fullPage: true,
        });
      }
      streamSequence = 11;
      tick = 1201;
      needsManualOpen = false;
    }

    const completed = variant.artwork === 'jade-fracture'
      ? snapshot(streamSequence, tick)
      : completedBoostSnapshot(streamSequence, tick);
    completed.GameEvent.event.Snapshot.game_state.start_ms = matchStart;
    variant.prepare(completed.GameEvent.event.Snapshot.game_state);
    await emitServerMessage(page, socketIndex, completed);

    if (needsManualOpen) {
      await page.getByTestId('game-roster-band')
        .getByRole('button', { name: 'Score card' })
        .click();
    }

    const scoreCard = page.getByTestId('game-over-card');
    const header = scoreCard.locator('.game-over-header');
    const jewel = scoreCard.getByTestId('game-over-jewel');
    await expect(scoreCard).toBeVisible();
    await expect(scoreCard.getByRole('heading', { name: variant.title })).toBeVisible();
    await expect(header).toHaveAttribute('data-result-artwork', variant.artwork);
    await expect(jewel).toHaveAttribute('data-result-artwork', variant.artwork);
    await expect(jewel).toHaveAttribute('aria-hidden', 'true');
    await expect(jewel.locator('.game-over-jewel-layer')).toHaveCount(variant.layers);
    await expect(jewel).toHaveCSS('transform', /matrix\(/);
    await expect(scoreCard.locator('.game-over-score-panel')).toHaveCSS('background-image', 'none');
    await page.waitForTimeout(220);
    await expect(scoreCard.locator('.game-over-scoreline')).toBeVisible();
    await expect(scoreCard.getByRole('button', { name: 'Close score card' })).toBeVisible();
    if (variant.artwork === 'jade-fracture') {
      await expect(scoreCard.getByRole('heading', { name: 'Run complete' })).toHaveCSS(
        'text-shadow',
        'rgba(20, 24, 31, 0.5) 2px 2px 0px, rgba(20, 24, 31, 0.2) 4px 4px 0px',
      );
    }

    const geometry = await header.evaluate((node) => {
      const title = node.querySelector('.game-over-title-section');
      const score = node.querySelector('.game-over-score-panel');
      if (!title || !score) throw new Error('result header fields are missing');
      const titleRect = title.getBoundingClientRect();
      const scoreRect = score.getBoundingClientRect();
      return {
        separated: titleRect.right <= scoreRect.left + 0.5,
        overflow: node.scrollWidth - node.clientWidth,
      };
    });
    expect(geometry).toEqual({ separated: true, overflow: 0 });

    if (process.env.SNAKETRON_VISUAL_DIR) {
      await header.screenshot({
        path: `${process.env.SNAKETRON_VISUAL_DIR}/game-over-${variant.artwork}.png`,
      });
    }

    await scoreCard.getByRole('button', { name: 'Close score card' }).click();
    await expect(scoreCard).toHaveCount(0);
    streamSequence += 1;
    tick += 1;
  }
});

test('original-language game shell and score card remain usable on mobile', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const liveFrame = completedBoostSnapshot(10, 1200);
  liveFrame.GameEvent.event.Snapshot.game_state.status = { Started: { server_id: 1 } };
  const socketIndex = await establishActiveGame(page, liveFrame);

  await expect(page.getByTestId('game-scoreboard')).toBeVisible();
  await expect(page.getByTestId('game-roster-band')).toBeVisible();
  await expect(page.getByTestId('game-roster-band').getByRole('button')).toHaveCount(0);
  await expect(page.getByTestId('game-scoreboard').getByRole('button', { name: 'Main menu' }))
    .toHaveCount(0);
  await expect(page.getByTestId('boost-hud')).toBeVisible();
  await page.waitForTimeout(240);
  const mobileLayout = await page.evaluate(() => {
    const selectors = [
      '[data-testid="game-scoreboard"]',
      '[data-testid="game-arena-utility-anchor"]',
      '[data-testid="game-utility-rail"]',
      '[data-testid="game-roster-band"]',
      '.game-arena-panel',
      '[data-testid="boost-hud"]',
      '[data-testid="game-controls-hint"]',
    ];
    const anchor = document.querySelector('[data-testid="game-arena-utility-anchor"]');
    const utilityRail = document.querySelector('[data-testid="game-utility-rail"]');
    const roster = document.querySelector('[data-testid="game-roster-band"]');
    const rosterViewport = roster?.querySelector('[data-roster-viewport="true"]');
    const versus = roster?.querySelector('.game-roster-versus');
    const meter = document.querySelector('[data-testid="boost-hud"]');
    const panel = document.querySelector('.game-arena-panel');
    const arenaFrame = panel?.closest('.game-arena-frame');
    if (
      !anchor || !utilityRail || !roster || !rosterViewport || !versus || !meter || !panel ||
      !arenaFrame
    ) {
      throw new Error('mobile arena utility anchor is incomplete');
    }
    const anchorStyle = getComputedStyle(anchor);
    const rosterRect = roster.getBoundingClientRect();
    const viewportRect = rosterViewport.getBoundingClientRect();
    const versusRect = versus.getBoundingClientRect();
    const meterRect = meter.getBoundingClientRect();
    const panelRect = panel.getBoundingClientRect();
    const controlsRect = document.querySelector('[data-testid="game-controls-hint"]')
      .getBoundingClientRect();
    const chatRect = document.querySelector('.home-lobby-chat > button')
      .getBoundingClientRect();
    const controlsOverlapChat = !(
      controlsRect.right <= chatRect.left ||
      controlsRect.left >= chatRect.right ||
      controlsRect.bottom <= chatRect.top ||
      controlsRect.top >= chatRect.bottom
    );
    return {
      overflow: document.documentElement.scrollWidth - window.innerWidth,
      viewport: { width: window.innerWidth, height: window.innerHeight },
      controls: {
        left: controlsRect.left,
        right: controlsRect.right,
        top: controlsRect.top,
        bottom: controlsRect.bottom,
      },
      chat: {
        left: chatRect.left,
        right: chatRect.right,
        top: chatRect.top,
        bottom: chatRect.bottom,
      },
      controlsOverlapChat,
      utilityOwnedByArena: utilityRail.parentElement === anchor,
      utilityInsideScoreboardShell: Boolean(utilityRail.closest('[data-testid="game-hud-shell"]')),
      anchorMarginTop: Number.parseFloat(anchorStyle.marginTop),
      anchorMarginBottom: Number.parseFloat(anchorStyle.marginBottom),
      anchorPanelGap: panelRect.top - anchor.getBoundingClientRect().bottom,
      rosterPanelCenterDelta: (rosterRect.left + rosterRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      rosterViewportPanelCenterDelta: (viewportRect.left + viewportRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      versusPanelCenterDelta: (versusRect.left + versusRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      versusLabel: versus.querySelector('.game-roster-versus-label')?.textContent?.trim(),
      rosterSnakeCount: roster.querySelectorAll('.game-roster-snake').length,
      rosterScoreCount: roster.querySelectorAll('.game-roster-side-score').length,
      rosterActionSlotCount: roster.querySelectorAll('.game-roster-action-slot').length,
      boostLocation: meter.getAttribute('data-location'),
      meterOwnedByArenaFrame: meter.parentElement === arenaFrame,
      meterOutsidePanel: !panel.contains(meter),
      meterPanelWidthDelta: meterRect.width - panelRect.width,
      meterPanelLeftDelta: meterRect.left - panelRect.left,
      meterPanelRightDelta: meterRect.right - panelRect.right,
      meterAttachmentDelta: meterRect.top - panelRect.bottom,
      rects: selectors.map((selector) => {
        const node = document.querySelector(selector);
        if (!node) throw new Error(`missing ${selector}`);
        const rect = node.getBoundingClientRect();
        return { selector, left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom };
      }),
    };
  });
  expect(mobileLayout.overflow).toBeLessThanOrEqual(0);
  expect(mobileLayout.controls.left).toBeGreaterThanOrEqual(-1);
  expect(mobileLayout.controls.right).toBeLessThanOrEqual(mobileLayout.viewport.width + 1);
  expect(mobileLayout.controls.bottom).toBeLessThanOrEqual(mobileLayout.viewport.height + 1);
  expect(mobileLayout.chat.left).toBeGreaterThanOrEqual(-1);
  expect(mobileLayout.chat.right).toBeLessThanOrEqual(mobileLayout.viewport.width + 1);
  expect(mobileLayout.chat.bottom).toBeLessThanOrEqual(mobileLayout.viewport.height + 1);
  expect(mobileLayout.controlsOverlapChat).toBe(false);
  expect(mobileLayout.utilityOwnedByArena).toBe(true);
  expect(mobileLayout.utilityInsideScoreboardShell).toBe(false);
  expect(mobileLayout.anchorMarginTop).toBe(mobileLayout.anchorMarginBottom);
  expect(Math.abs(mobileLayout.anchorPanelGap - mobileLayout.anchorMarginBottom))
    .toBeLessThanOrEqual(1);
  expect(Math.abs(mobileLayout.rosterPanelCenterDelta)).toBeLessThanOrEqual(1);
  expect(Math.abs(mobileLayout.rosterViewportPanelCenterDelta)).toBeLessThanOrEqual(1);
  expect(Math.abs(mobileLayout.versusPanelCenterDelta)).toBeLessThanOrEqual(1);
  expect(mobileLayout.versusLabel).toBe('VS');
  expect(mobileLayout.rosterSnakeCount).toBe(2);
  expect(mobileLayout.rosterScoreCount).toBe(0);
  expect(mobileLayout.rosterActionSlotCount).toBe(0);
  expect(mobileLayout.boostLocation).toBe('arena-bottom');
  expect(mobileLayout.meterOwnedByArenaFrame).toBe(true);
  expect(mobileLayout.meterOutsidePanel).toBe(true);
  expect(mobileLayout.meterPanelWidthDelta).toBeCloseTo(-20, 1);
  expect(mobileLayout.meterPanelLeftDelta).toBeCloseTo(10, 1);
  expect(mobileLayout.meterPanelRightDelta).toBeCloseTo(-10, 1);
  expect(mobileLayout.meterAttachmentDelta).toBeCloseTo(-2, 1);
  for (const rect of mobileLayout.rects) {
    expect(rect.left, rect.selector).toBeGreaterThanOrEqual(-1);
    expect(rect.right, rect.selector).toBeLessThanOrEqual(391);
  }

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-shell-mobile.png`,
      fullPage: true,
    });
  }

  const finalFrame = completedBoostSnapshot(11, 1201);
  finalFrame.GameEvent.event.Snapshot.game_state.start_ms =
    liveFrame.GameEvent.event.Snapshot.game_state.start_ms;
  await emitServerMessage(page, socketIndex, finalFrame);
  const scoreCard = page.getByTestId('game-over-card');
  await expect(scoreCard).toBeVisible();
  await expect(page.getByTestId('game-roster-band').getByRole('button')).toHaveCount(2);
  await expect(page.getByTestId('game-roster-band')
    .getByRole('button', { name: 'Score card' })).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByTestId('game-scoreboard').getByRole('button', { name: 'Main menu' }))
    .toHaveCount(1);
  const cardRect = await scoreCard.boundingBox();
  expect(cardRect).not.toBeNull();
  expect(cardRect.x).toBeGreaterThanOrEqual(6);
  expect(cardRect.x + cardRect.width).toBeLessThanOrEqual(384);
  expect(cardRect.height).toBeLessThanOrEqual(828);
  await expect(scoreCard.getByRole('button', { name: 'Main menu' })).toBeVisible();
  await expect(scoreCard.getByRole('button', { name: 'Play again' })).toBeVisible();
  await expect(scoreCard.locator('.game-over-statline > div')).toHaveCount(5);
  await expect(scoreCard.locator('.game-over-statline strong.is-ppm')).toHaveText('2.0');
  await expect(scoreCard.locator('.game-over-statline strong.is-apm')).toHaveText('36.0');
  const mobileHeaderStyle = await scoreCard.evaluate((card) => {
    const header = card.querySelector('.game-over-header');
    const title = card.querySelector('.game-over-title-section');
    const heading = title?.querySelector('h2');
    const score = card.querySelector('.game-over-score-panel');
    const jewel = card.querySelector('.game-over-jewel');
    if (!header || !title || !heading || !score || !jewel) {
      throw new Error('mobile score card header is incomplete');
    }
    const titleRect = title.getBoundingClientRect();
    const headingRect = heading.getBoundingClientRect();
    const scoreRect = score.getBoundingClientRect();
    return {
      headerHeight: header.getBoundingClientRect().height,
      titleBackground: getComputedStyle(title).backgroundColor,
      titleReadable: headingRect.left >= titleRect.left && headingRect.right <= titleRect.right + 0.5,
      titleWidth: titleRect.width,
      scoreWidth: scoreRect.width,
      scoreColor: getComputedStyle(score).color,
      scoreBackground: getComputedStyle(score).backgroundImage,
      scoreClip: getComputedStyle(score).clipPath,
      scoreBackdropFilter: getComputedStyle(score).backdropFilter,
      jewelArtwork: jewel.getAttribute('data-result-artwork'),
      jewelLayerCount: jewel.children.length,
      horizontalOverflow: header.scrollWidth - header.clientWidth,
      statLabelFontStyles: Array.from(card.querySelectorAll('.game-over-statline > div > span'))
        .map((label) => getComputedStyle(label).fontStyle),
      statLabelFontSizes: Array.from(card.querySelectorAll('.game-over-statline > div > span'))
        .map((label) => getComputedStyle(label).fontSize),
      statTileAlignments: Array.from(card.querySelectorAll('.game-over-statline > div'))
        .map((tile) => [getComputedStyle(tile).alignItems, getComputedStyle(tile).textAlign]),
    };
  });
  expect(mobileHeaderStyle.headerHeight).toBeGreaterThanOrEqual(110);
  expect(mobileHeaderStyle.headerHeight).toBeLessThanOrEqual(113);
  expect(mobileHeaderStyle.titleBackground).toBe('rgba(0, 0, 0, 0)');
  expect(mobileHeaderStyle.titleReadable).toBe(true);
  expect(mobileHeaderStyle.titleWidth).toBeGreaterThan(mobileHeaderStyle.scoreWidth);
  expect(mobileHeaderStyle.scoreWidth).toBeGreaterThanOrEqual(108);
  expect(mobileHeaderStyle.scoreColor).toBe('rgb(20, 24, 31)');
  expect(mobileHeaderStyle.scoreBackground).toBe('none');
  expect(mobileHeaderStyle.scoreClip).toBe('none');
  expect(mobileHeaderStyle.scoreBackdropFilter).toBe('none');
  expect(mobileHeaderStyle.jewelArtwork).toBe('azure-cut');
  expect(mobileHeaderStyle.jewelLayerCount).toBe(11);
  expect(mobileHeaderStyle.horizontalOverflow).toBe(0);
  expect(mobileHeaderStyle.statLabelFontStyles)
    .toEqual(['normal', 'normal', 'normal', 'normal', 'normal']);
  expect(mobileHeaderStyle.statLabelFontSizes)
    .toEqual(['7.5px', '7.5px', '7.5px', '7.5px', '7.5px']);
  expect(mobileHeaderStyle.statTileAlignments).toEqual([
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
    ['center', 'center'],
  ]);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-over-mobile.png`,
      fullPage: true,
    });
  }

  await scoreCard.getByRole('button', { name: 'Close score card' }).click();
  const mobileRoster = page.getByTestId('game-roster-band');
  const mobileHome = page.getByTestId('game-scoreboard')
    .getByRole('button', { name: 'Main menu' });
  await expect(mobileRoster.getByRole('button')).toHaveCount(2);
  await expect(mobileRoster.getByRole('button', { name: 'Menu', exact: true })).toBeVisible();
  await expect(mobileRoster.getByRole('button', { name: 'Score card' })).toBeVisible();
  const mobileCompletedRosterLayout = await mobileRoster.evaluate((roster) => {
    const viewport = roster.querySelector('[data-roster-viewport="true"]');
    const versus = roster.querySelector('.game-roster-versus');
    const leftSlot = roster.querySelector('.game-roster-action-slot.is-left');
    const rightSlot = roster.querySelector('.game-roster-action-slot.is-right');
    const menu = leftSlot?.querySelector('button');
    const scoreCardButton = rightSlot?.querySelector('button');
    if (!viewport || !versus || !leftSlot || !rightSlot || !menu || !scoreCardButton) {
      throw new Error('mobile completed roster layout is incomplete');
    }
    const rosterRect = roster.getBoundingClientRect();
    const viewportRect = viewport.getBoundingClientRect();
    const versusRect = versus.getBoundingClientRect();
    const leftSlotRect = leftSlot.getBoundingClientRect();
    const rightSlotRect = rightSlot.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();
    const scoreCardRect = scoreCardButton.getBoundingClientRect();
    return {
      viewportCenterDelta: (rosterRect.left + rosterRect.width / 2) -
        (viewportRect.left + viewportRect.width / 2),
      versusCenterDelta: (rosterRect.left + rosterRect.width / 2) -
        (versusRect.left + versusRect.width / 2),
      actionSlotWidthDelta: leftSlotRect.width - rightSlotRect.width,
      leftSlotPrecedesViewport: leftSlotRect.right <= viewportRect.left + 0.5,
      rightSlotFollowsViewport: rightSlotRect.left >= viewportRect.right - 0.5,
      menuPrecedesViewport: menuRect.right <= viewportRect.left + 0.5,
      scoreCardFollowsViewport: scoreCardRect.left >= viewportRect.right - 0.5,
      actionSlotCount: roster.querySelectorAll('.game-roster-action-slot').length,
      overflow: roster.scrollWidth - roster.clientWidth,
    };
  });
  expect(Math.abs(mobileCompletedRosterLayout.viewportCenterDelta)).toBeLessThanOrEqual(1);
  expect(Math.abs(mobileCompletedRosterLayout.versusCenterDelta)).toBeLessThanOrEqual(1);
  expect(Math.abs(mobileCompletedRosterLayout.actionSlotWidthDelta)).toBeLessThanOrEqual(1);
  expect(mobileCompletedRosterLayout.leftSlotPrecedesViewport).toBe(true);
  expect(mobileCompletedRosterLayout.rightSlotFollowsViewport).toBe(true);
  expect(mobileCompletedRosterLayout.menuPrecedesViewport).toBe(true);
  expect(mobileCompletedRosterLayout.scoreCardFollowsViewport).toBe(true);
  expect(mobileCompletedRosterLayout.actionSlotCount).toBe(2);
  expect(mobileCompletedRosterLayout.overflow).toBeLessThanOrEqual(0);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-complete-roster-mobile.png`,
      fullPage: true,
    });
  }

  await expect(mobileHome).toBeVisible();
  await mobileHome.focus();
  await expect(mobileHome).toBeFocused();
  await mobileHome.press('Enter');
  await expect(page).toHaveURL(/\/$/);
});

test('landscape mobile keeps the centered roster and arena Boost footer aligned', async ({ page }) => {
  await page.setViewportSize({ width: 667, height: 375 });
  const socketIndex = await establishActiveGame(page);

  const readLayout = () => page.evaluate(() => {
    const canvas = document.querySelector('.game-arena-panel canvas');
    const panel = document.querySelector('.game-arena-panel');
    const utilityAnchor = document.querySelector('[data-testid="game-arena-utility-anchor"]');
    const utilityRail = document.querySelector('[data-testid="game-utility-rail"]');
    const meter = document.querySelector('[data-testid="boost-hud"]');
    const controls = document.querySelector('[data-testid="game-controls-hint"]');
    const chatButton = document.querySelector('.home-lobby-chat > button');
    const roster = document.querySelector('[data-testid="game-roster-band"]');
    const rosterViewport = roster?.querySelector('[data-roster-viewport="true"]');
    const versus = roster?.querySelector('.game-roster-versus');
    const arenaFrame = panel?.closest('.game-arena-frame');
    const boostReservoir = meter?.querySelector('.game-boost-meter__reservoir');
    const boostValue = meter?.querySelector('.game-boost-meter__value');
    if (
      !canvas || !panel || !utilityAnchor || !utilityRail || !meter || !controls || !chatButton ||
      !roster || !rosterViewport || !versus || !arenaFrame || !boostReservoir || !boostValue
    ) {
      throw new Error('landscape game shell is incomplete');
    }

    const canvasRect = canvas.getBoundingClientRect();
    const anchorRect = utilityAnchor.getBoundingClientRect();
    const anchorStyle = getComputedStyle(utilityAnchor);
    const panelRect = panel.getBoundingClientRect();
    const utilityRect = utilityRail.getBoundingClientRect();
    const meterRect = meter.getBoundingClientRect();
    const controlsRect = controls.getBoundingClientRect();
    const chatRect = chatButton.getBoundingClientRect();
    const rosterRect = roster.getBoundingClientRect();
    const rosterViewportRect = rosterViewport.getBoundingClientRect();
    const versusRect = versus.getBoundingClientRect();
    const cellSize = (canvasRect.width - 2) / 60;
    const controlsOverlapChat = !(
      controlsRect.right <= chatRect.left ||
      controlsRect.left >= chatRect.right ||
      controlsRect.bottom <= chatRect.top ||
      controlsRect.top >= chatRect.bottom
    );

    return {
      location: meter.getAttribute('data-location'),
      compact: meter.hasAttribute('data-compact'),
      cellSize,
      viewport: { width: window.innerWidth, height: window.innerHeight },
      document: {
        width: document.documentElement.scrollWidth,
        height: document.documentElement.scrollHeight,
      },
      meter: {
        left: meterRect.left,
        right: meterRect.right,
        top: meterRect.top,
        bottom: meterRect.bottom,
        width: meterRect.width,
      },
      utility: {
        left: utilityRect.left,
        right: utilityRect.right,
        top: utilityRect.top,
        bottom: utilityRect.bottom,
        width: utilityRect.width,
      },
      anchor: {
        marginTop: Number.parseFloat(anchorStyle.marginTop),
        marginBottom: Number.parseFloat(anchorStyle.marginBottom),
        panelGap: panelRect.top - anchorRect.bottom,
        ownsUtility: utilityRail.parentElement === utilityAnchor,
      },
      panel: {
        left: panelRect.left,
        right: panelRect.right,
        top: panelRect.top,
        bottom: panelRect.bottom,
        width: panelRect.width,
      },
      roster: {
        left: rosterRect.left,
        right: rosterRect.right,
        panelCenterDelta: (rosterRect.left + rosterRect.width / 2) -
          (panelRect.left + panelRect.width / 2),
      },
      rosterViewport: {
        panelCenterDelta: (rosterViewportRect.left + rosterViewportRect.width / 2) -
          (panelRect.left + panelRect.width / 2),
      },
      versusPanelCenterDelta: (versusRect.left + versusRect.width / 2) -
        (panelRect.left + panelRect.width / 2),
      versusLabel: versus.querySelector('.game-roster-versus-label')?.textContent?.trim(),
      rosterButtonCount: roster.querySelectorAll('button').length,
      rosterScoreCount: roster.querySelectorAll('.game-roster-side-score').length,
      rosterActionSlotCount: roster.querySelectorAll('.game-roster-action-slot').length,
      meterOwnedByArenaFrame: meter.parentElement === arenaFrame,
      meterOutsidePanel: !panel.contains(meter),
      meterAttachmentDelta: meterRect.top - panelRect.bottom,
      reservoirDisplay: getComputedStyle(boostReservoir).display,
      valueDisplay: getComputedStyle(boostValue).display,
      controls: { top: controlsRect.top, bottom: controlsRect.bottom },
      controlsOverlapChat,
    };
  });

  const expectLandscapeLayout = (layout) => {
    const epsilon = 0.75;
    expect(layout.location).toBe('arena-bottom');
    expect(layout.compact).toBe(false);
    expect(layout.cellSize).toBeCloseTo(5, 4);
    expect(Math.abs(layout.utility.width - layout.panel.width)).toBeLessThanOrEqual(epsilon);
    expect(Math.abs(layout.utility.left - layout.panel.left)).toBeLessThanOrEqual(epsilon);
    expect(Math.abs(layout.utility.right - layout.panel.right)).toBeLessThanOrEqual(epsilon);
    expect(layout.anchor.ownsUtility).toBe(true);
    expect(layout.anchor.marginTop).toBe(layout.anchor.marginBottom);
    expect(Math.abs(layout.anchor.panelGap - layout.anchor.marginBottom)).toBeLessThanOrEqual(epsilon);
    expect(layout.panel.width - layout.meter.width).toBeCloseTo(20, 1);
    expect(layout.meter.left - layout.panel.left).toBeCloseTo(10, 1);
    expect(layout.panel.right - layout.meter.right).toBeCloseTo(10, 1);
    expect(layout.meterAttachmentDelta).toBeCloseTo(-2, 1);
    expect(layout.meterOwnedByArenaFrame).toBe(true);
    expect(layout.meterOutsidePanel).toBe(true);
    expect(Math.abs(layout.roster.panelCenterDelta)).toBeLessThanOrEqual(epsilon);
    expect(Math.abs(layout.rosterViewport.panelCenterDelta)).toBeLessThanOrEqual(epsilon);
    expect(Math.abs(layout.versusPanelCenterDelta)).toBeLessThanOrEqual(epsilon);
    expect(layout.versusLabel).toBe('VS');
    expect(layout.utility.bottom).toBeLessThanOrEqual(layout.panel.top + epsilon);
    expect(layout.meter.left).toBeGreaterThanOrEqual(-epsilon);
    expect(layout.meter.right).toBeLessThanOrEqual(layout.viewport.width + epsilon);
    expect(layout.meter.top).toBeGreaterThanOrEqual(-epsilon);
    expect(layout.meter.bottom).toBeLessThanOrEqual(layout.viewport.height + epsilon);
    expect(layout.document.width).toBeLessThanOrEqual(layout.viewport.width);
    expect(layout.document.height).toBeLessThanOrEqual(layout.viewport.height);
    expect(layout.rosterButtonCount).toBe(0);
    expect(layout.rosterScoreCount).toBe(0);
    expect(layout.rosterActionSlotCount).toBe(0);
    expect(layout.reservoirDisplay).toBe('block');
    expect(layout.valueDisplay).toBe('grid');
    expect(layout.controls.top).toBeGreaterThanOrEqual(-epsilon);
    expect(layout.controls.bottom).toBeLessThanOrEqual(layout.viewport.height + epsilon);
    expect(layout.controlsOverlapChat).toBe(false);
  };

  await emitServerMessage(page, socketIndex, liveBoostSnapshotForLocalTeam(11, 6, 0));
  await expect(page.getByTestId('boost-hud')).toHaveAttribute('data-location', 'arena-bottom');
  await expect(page.getByTestId('boost-hud')).not.toHaveAttribute('data-compact', /.+/);
  await expect(page.getByTestId('boost-hud')).toContainText('33%');
  await expect(page.getByTestId('boost-hud')).not.toContainText('Boost');
  await page.waitForTimeout(240);
  const teamZeroLayout = await readLayout();
  expectLandscapeLayout(teamZeroLayout);

  const landscapeBottle = page.getByTestId('boost-nos-bottle');
  const landscapeMarkGeometry = await landscapeBottle.evaluate((element) => {
    const wordmark = element.querySelector('.game-boost-meter__nos-wordmark');
    if (!wordmark) {
      throw new Error('compact NOS wordmark is missing');
    }
    return {
      bottleWidth: element.getBoundingClientRect().width,
      wordmarkWidth: wordmark.getBoundingClientRect().width,
      imageRendering: getComputedStyle(element).imageRendering,
    };
  });
  expect(landscapeMarkGeometry.bottleWidth).toBeGreaterThanOrEqual(33);
  expect(landscapeMarkGeometry.wordmarkWidth).toBeGreaterThanOrEqual(8);
  expect(landscapeMarkGeometry.imageRendering).toBe('auto');

  const landscapeNos = await readNosCanvasRegions(page, 60, [
    { name: 'regular', x: 26, y: 12, sizeCells: 1 },
    { name: 'full', x: 14, y: 4, sizeCells: 2 },
    { name: 'cooling', x: 14, y: 34, sizeCells: 2 },
  ]);
  expect(landscapeNos.cellSize).toBe(5);
  expectNosPickupIdentity(landscapeNos.samples.regular);
  expectNosPickupIdentity(landscapeNos.samples.full);
  expect(landscapeNos.samples.full.total)
    .toBeGreaterThan(landscapeNos.samples.regular.total * 1.5);
  expect(landscapeNos.samples.cooling.counts.blue).toBe(0);
  expect(landscapeNos.samples.cooling.counts.orange).toBe(0);
  expect(landscapeNos.samples.wholeArena.oldLightningYellow).toBe(0);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.getByTestId('boost-hud').screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-landscape-partial.png`,
    });
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-shell-landscape-team0.png`,
      fullPage: true,
    });
  }

  const teamOneLiveFrame = liveBoostSnapshotForLocalTeam(12, 7, 1);
  await emitServerMessage(page, socketIndex, teamOneLiveFrame);
  await expect(page.getByTestId('boost-hud')).toHaveAttribute('data-location', 'arena-bottom');
  await expect(page.getByTestId('boost-hud')).toContainText('33%');
  const teamOneLayout = await readLayout();
  expectLandscapeLayout(teamOneLayout);
  expect(Math.abs(teamOneLayout.meter.left - teamZeroLayout.meter.left)).toBeLessThanOrEqual(0.5);
  expect(Math.abs(teamOneLayout.meter.right - teamZeroLayout.meter.right)).toBeLessThanOrEqual(0.5);

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await page.waitForTimeout(220);
    await page.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-game-shell-landscape-team1.png`,
      fullPage: true,
    });
  }

  const teamOneReadyFrame = liveBoostSnapshotForLocalTeam(13, 8, 1);
  teamOneReadyFrame.GameEvent.event.Snapshot.game_state
    .arena.snakes[1].boost.charge_ms = 3000;
  await emitServerMessage(page, socketIndex, teamOneReadyFrame);
  const compactReadyHud = page.getByTestId('boost-hud');
  await expect(compactReadyHud).toHaveAttribute('data-location', 'arena-bottom');
  await expect(compactReadyHud).toHaveAttribute('data-ready', 'true');
  await expect(compactReadyHud).toHaveClass(/is-ready/);
  await expect(compactReadyHud).toContainText('100%');
  expectLandscapeLayout(await readLayout());
  await expect.poll(() => compactReadyHud.evaluate((element) => {
    const button = element.querySelector('.game-boost-meter');
    const fill = element.querySelector('.game-boost-meter__fill');
    const track = element.querySelector('.game-boost-meter__track');
    const dock = element.querySelector('.game-boost-meter__canister-dock');
    if (!button || !fill || !track || !dock) {
      throw new Error('ready Boost track is incomplete');
    }
    return {
      background: getComputedStyle(button).backgroundColor,
      dock: getComputedStyle(dock).backgroundColor,
      fillRatio: fill.getBoundingClientRect().width / track.getBoundingClientRect().width,
      fillBackground: getComputedStyle(fill).backgroundImage,
    };
  })).toMatchObject({
    background: 'rgba(0, 0, 0, 0)',
    dock: 'rgba(255, 255, 255, 0.28)',
    fillRatio: 1,
  });

  if (process.env.SNAKETRON_VISUAL_DIR) {
    await compactReadyHud.screenshot({
      path: `${process.env.SNAKETRON_VISUAL_DIR}/boost-widget-landscape-ready.png`,
    });
  }

  const completedFrame = completedBoostSnapshot(14, 9);
  completedFrame.GameEvent.event.Snapshot.game_state.start_ms =
    teamOneReadyFrame.GameEvent.event.Snapshot.game_state.start_ms;
  await emitServerMessage(page, socketIndex, completedFrame);

  const shortScoreCard = page.getByTestId('game-over-card');
  await expect(shortScoreCard).toBeVisible();
  await expect(page.getByTestId('boost-hud')).toHaveCount(0);
  await expect(page.getByTestId('game-roster-band').getByRole('button')).toHaveCount(2);
  const shortResultLayout = await page.evaluate(() => {
    const backdrop = document.querySelector('[data-testid="game-over-backdrop"]');
    const card = document.querySelector('[data-testid="game-over-card"]');
    const roster = document.querySelector('[data-testid="game-roster-band"]');
    if (!backdrop || !card || !roster) {
      throw new Error('short-viewport score card is incomplete');
    }
    const backdropRect = backdrop.getBoundingClientRect();
    const cardRect = card.getBoundingClientRect();
    return {
      backdropTop: backdropRect.top,
      backdropBottom: backdropRect.bottom,
      cardTop: cardRect.top,
      cardBottom: cardRect.bottom,
      viewportBottom: window.innerHeight,
    };
  });
  expect(shortResultLayout.backdropTop).toBe(0);
  expect(shortResultLayout.backdropBottom).toBe(shortResultLayout.viewportBottom);
  expect(shortResultLayout.cardTop).toBeGreaterThanOrEqual(shortResultLayout.backdropTop);
  expect(shortResultLayout.cardBottom).toBeLessThanOrEqual(shortResultLayout.viewportBottom + 1);
});

test('NOS vector pickups retain identity and grid clearance at every canvas cell size', async ({ page }) => {
  await page.setViewportSize({ width: 500, height: 546 });
  const socketIndex = await establishActiveGame(page);
  const frame = liveBoostSnapshotForLocalTeam(11, 6, 0);
  const state = frame.GameEvent.event.Snapshot.game_state;
  state.arena.boost_pads[1].respawn_at_tick = state.tick + 10_000;
  await emitServerMessage(page, socketIndex, frame);
  await expect(page.getByTestId('boost-hud')).toBeVisible();

  const regularIntersections = [
    { x: 12, y: 33 }, { x: 13, y: 33 }, { x: 12, y: 34 }, { x: 13, y: 34 },
  ];
  const fullIntersections = [];
  const coolingIntersections = [];
  for (let x = 4; x <= 6; x += 1) {
    for (let y = 14; y <= 16; y += 1) fullIntersections.push({ x, y });
  }
  for (let x = 34; x <= 36; x += 1) {
    for (let y = 44; y <= 46; y += 1) coolingIntersections.push({ x, y });
  }

  for (let cellSize = 5; cellSize <= 15; cellSize += 1) {
    await page.setViewportSize({
      width: cellSize <= 7 ? 500 : 700,
      // At <=760px width the CSS HUD footprint is 116px. The widest compact
      // footer/chrome budget is another 136px; 2px of slack selects this exact
      // tier while remaining far below the next 60px cell-size step.
      height: 254 + cellSize * 60,
    });

    const readSamples = () => readNosCanvasRegions(page, 40, [
      { name: 'regular', x: 12, y: 33, sizeCells: 1 },
      { name: 'full', x: 4, y: 14, sizeCells: 2 },
      { name: 'cooling', x: 34, y: 44, sizeCells: 2 },
    ], false);
    // The rotated standard pickup does not retain a fully exact label-white
    // pixel until the largest tier; lower tiers preserve the separator through
    // antialiasing, while pure Rust geometry tests cover the authored band.
    const requireExactSeparatorPixel = cellSize === 15;
    await expect.poll(async () => (await readSamples()).cellSize).toBe(cellSize);
    await expect.poll(() => page.locator('.game-arena-panel canvas').evaluate((canvas) => (
      canvas.height
    ))).toBe(2 + cellSize * 60);
    await expect.poll(async () => {
      const sample = await readSamples();
      return sample.cellSize === cellSize
        && hasNosPickupIdentity(sample.samples.regular, requireExactSeparatorPixel)
        && hasNosPickupIdentity(sample.samples.full, requireExactSeparatorPixel);
    }).toBe(true);

    const rendered = await readSamples();
    expectNosPickupIdentity(rendered.samples.regular, requireExactSeparatorPixel);
    expectNosPickupIdentity(rendered.samples.full, requireExactSeparatorPixel);
    expect(rendered.samples.full.total)
      .toBeGreaterThan(rendered.samples.regular.total * 1.5);
    expect(rendered.samples.cooling.counts.blue).toBe(0);
    expect(rendered.samples.cooling.counts.orange).toBe(0);

    const availableGridInk = await readGridIntersectionInk(
      page,
      40,
      [...regularIntersections, ...fullIntersections],
    );
    const coolingGridInk = await readGridIntersectionInk(page, 40, coolingIntersections);
    expect(availableGridInk.cellSize).toBe(cellSize);
    expect(coolingGridInk.counts.every((count) => count > 0)).toBe(true);
    const ordinaryDotTones = new Set(coolingGridInk.samples.flat());
    expect(ordinaryDotTones.size).toBeGreaterThan(0);
    expect(availableGridInk.samples.flat().some((tone) => ordinaryDotTones.has(tone)))
      .toBe(false);

    if (cellSize === 15) {
      const label = await readNosCanvasRegions(page, 40, [
        {
          name: 'fullWordmark',
          x: 4.725,
          y: 14.725,
          sizeCells: 0.55,
          overscanPixels: 0,
        },
      ], false);
      expect(label.samples.fullWordmark.counts.orange).toBeGreaterThan(0);
      expect(label.samples.fullWordmark.counts.label).toBeGreaterThan(0);
    }

    if (process.env.SNAKETRON_VISUAL_DIR) {
      await page.locator('.game-arena-panel canvas').screenshot({
        path: `${process.env.SNAKETRON_VISUAL_DIR}/nos-canvas-cell-${cellSize}.png`,
      });
    }
  }
});

test('actual WASM round-trips every authoritative Boost field from a snapshot', async ({ page }) => {
  await establishActiveGame(page);
  const frame = boostSnapshot(11, 6);

  const result = await page.evaluate((snapshotFrame) => {
    const first = window.wasm.GameClient.newFromSnapshotFrame(42, JSON.stringify(snapshotFrame));
    const firstStateJson = first.getCommittedStateJson();
    const firstState = JSON.parse(firstStateJson);
    const firstHash = first.getCommittedHash();

    const restored = window.wasm.GameClient.newFromState(42, firstStateJson);
    const restoredState = JSON.parse(restored.getCommittedStateJson());
    const restoredHash = restored.getCommittedHash();

    // Drive one predicted quantum through the public WASM boundary, then use
    // that active state as another authoritative restore point. This covers
    // non-default speed, residual credit, funded charge, and active state.
    first.setLocalPlayerId(7);
    const activation = JSON.parse(first.processActivateBoost(0));
    const nextQuantumMs = firstState.start_ms + (firstState.tick + 1) * 50;
    first.rebuildPredictedState(BigInt(nextQuantumMs));
    const activeStateJson = first.getGameStateJson();
    const activeState = JSON.parse(activeStateJson);
    const activeRestore = window.wasm.GameClient.newFromState(42, activeStateJson);
    const activeRestoredState = JSON.parse(activeRestore.getCommittedStateJson());
    const activeHash = activeRestore.getCommittedHash();
    const activeRestoreAgain = window.wasm.GameClient.newFromState(
      42,
      activeRestore.getCommittedStateJson(),
    );
    const activeHashAgain = activeRestoreAgain.getCommittedHash();
    first.free();
    restored.free();
    activeRestore.free();
    activeRestoreAgain.free();

    return {
      firstHash,
      restoredHash,
      boostConfig: restoredState.properties.boost,
      pads: restoredState.arena.boost_pads,
      snake: {
        speed_milli: restoredState.arena.snakes[0].speed_milli,
        movement_credit: restoredState.arena.snakes[0].movement_credit,
        boost: restoredState.arena.snakes[0].boost,
      },
      activation: activation.command,
      activeSnake: {
        speed_milli: activeRestoredState.arena.snakes[0].speed_milli,
        movement_credit: activeRestoredState.arena.snakes[0].movement_credit,
        boost: activeRestoredState.arena.snakes[0].boost,
      },
      activeHash,
      activeHashAgain,
    };
  }, frame);

  expect(result.restoredHash).toBe(result.firstHash);
  expect(result.boostConfig).toEqual(frame.GameEvent.event.Snapshot.game_state.properties.boost);
  expect(result.pads).toEqual(frame.GameEvent.event.Snapshot.game_state.arena.boost_pads);
  expect(result.snake).toEqual({
    speed_milli: 1000,
    movement_credit: 0,
    boost: { charge_ms: 1000, active: false, intent: false },
  });
  expect(result.activation).toEqual({ ActivateBoost: { snake_id: 0 } });
  expect(result.activeSnake).toEqual({
    speed_milli: 1500,
    movement_credit: 75000,
    boost: { charge_ms: 950, active: true, intent: true },
  });
  expect(result.activeHashAgain).toBe(result.activeHash);
});

test('actual WASM admits only legacy-compatible completed TeamMatch snapshots', async ({ page }) => {
  await establishActiveGame(page);
  const currentFrame = boostSnapshot(11, 6);

  const result = await page.evaluate((sourceFrame) => {
    const legacyFrame = JSON.parse(JSON.stringify(sourceFrame));
    const legacyState = legacyFrame.GameEvent.event.Snapshot.game_state;
    legacyState.status = { Complete: { winning_snake_id: 0 } };
    legacyState.properties.tick_duration_ms = 100;
    legacyState.properties.time_limit_ms = 90_000;
    legacyState.properties.score_limit = null;
    legacyState.properties.boost = null;
    legacyState.arena.boost_pads = [];
    for (const snake of legacyState.arena.snakes) {
      snake.speed_milli = 1000;
      snake.movement_credit = 0;
      snake.boost = { charge_ms: 0, active: false, intent: false };
    }

    const completed = window.wasm.GameClient.newFromSnapshotFrame(
      42,
      JSON.stringify(legacyFrame),
    );
    const completedState = JSON.parse(completed.getCommittedStateJson());
    completed.free();

    const liveLegacyFrame = JSON.parse(JSON.stringify(legacyFrame));
    liveLegacyFrame.GameEvent.event.Snapshot.game_state.status = {
      Started: { server_id: 1 },
    };
    let liveLegacyRejected = false;
    try {
      window.wasm.GameClient.newFromSnapshotFrame(42, JSON.stringify(liveLegacyFrame)).free();
    } catch (_error) {
      liveLegacyRejected = true;
    }

    const malformedCurrentFrame = JSON.parse(JSON.stringify(sourceFrame));
    malformedCurrentFrame.GameEvent.event.Snapshot.game_state.status = {
      Complete: { winning_snake_id: null },
    };
    malformedCurrentFrame.GameEvent.event.Snapshot.game_state.arena.boost_pads.pop();
    let malformedCurrentRejected = false;
    try {
      window.wasm.GameClient.newFromSnapshotFrame(
        42,
        JSON.stringify(malformedCurrentFrame),
      ).free();
    } catch (_error) {
      malformedCurrentRejected = true;
    }

    return {
      completedStatus: completedState.status,
      completedBoost: completedState.properties.boost,
      completedPads: completedState.arena.boost_pads,
      liveLegacyRejected,
      malformedCurrentRejected,
    };
  }, currentFrame);

  expect(result.completedStatus).toEqual({ Complete: { winning_snake_id: 0 } });
  expect(result.completedBoost).toBeNull();
  expect(result.completedPads).toEqual([]);
  expect(result.liveLegacyRejected).toBe(true);
  expect(result.malformedCurrentRejected).toBe(true);
});

test('four-snake 2.0x WASM prediction and state publication stay within budget', async ({ page }) => {
  await establishActiveGame(page);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('Emulation.setCPUThrottlingRate', { rate: 4 });
  let result;
  try {
    result = await page.evaluate((state) => {
      const durations = [];
      const stateJson = JSON.stringify(state);
      for (let round = 0; round < 6; round += 1) {
        const client = window.wasm.GameClient.newFromState(42, stateJson);
        for (let ahead = 1; ahead <= 30; ahead += 1) {
          const targetMs = (state.tick + ahead) * state.properties.tick_duration_ms;
          const started = performance.now();
          client.rebuildPredictedState(BigInt(targetMs));
          // Include the current React publication path, not just engine time.
          JSON.parse(client.getGameStateJson());
          JSON.parse(client.getCommittedStateJson());
          durations.push(performance.now() - started);
        }
        client.free();
      }
      durations.sort((left, right) => left - right);
      const percentile = (value) => durations[
        Math.min(durations.length - 1, Math.floor(durations.length * value))
      ];
      return {
        samples: durations.length,
        p95Ms: percentile(0.95),
        p99Ms: percentile(0.99),
        maxMs: durations[durations.length - 1],
      };
    }, fourSnakeBoostState());
  } finally {
    await cdp.send('Emulation.setCPUThrottlingRate', { rate: 1 });
  }

  console.log('4x CPU-throttled Boost WASM publication profile', result);
  expect(result.samples).toBe(180);
  expect(result.p95Ms).toBeLessThan(8);
  expect(result.maxMs).toBeLessThan(50);
});

test('a rejected predicted Boost activation retracts immediately to authoritative charge', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('snaketron:boost-input-mode:v1', 'toggle');
  });
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));
  await page.getByTestId('boost-button').click();

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  const delivery = (await socketMessages(page, socketIndex, 'GameCommandV2'))[0].GameCommandV2;
  await expect.poll(() => page.evaluate(() => ({
    active: window.__gameArenaState?.arena?.snakes?.[0]?.boost?.active,
    speed: window.__gameArenaState?.arena?.snakes?.[0]?.speed_milli,
  }))).toEqual({ active: true, speed: 1500 });
  await expect(page.getByTestId('boost-button')).toBeEnabled();
  await expect(page.getByTestId('boost-button')).toHaveAccessibleName(/Stop Boost/);

  const authoritativeTick = await page.evaluate(() => window.__gameArenaState.tick);
  await emitServerMessage(page, socketIndex, {
    GameEvent: {
      game_id: 42,
      tick: authoritativeTick,
      sequence: 2,
      stream_seq: 12,
      user_id: 7,
      event: {
        CommandRejected: {
          command_id: delivery.command_id,
          command_id_client: delivery.command.command_id_client,
          reason: 'rejected activation for reconciliation test',
        },
      },
    },
  });

  await expect.poll(() => page.evaluate(() => {
    const snake = window.__gameArenaState?.arena?.snakes?.[0];
    return snake && {
      active: snake.boost.active,
      charge_ms: snake.boost.charge_ms,
      speed_milli: snake.speed_milli,
    };
  })).toEqual({ active: false, charge_ms: 1000, speed_milli: 1000 });
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  await expect(page.getByTestId('boost-hud')).toContainText('33%');
  await expect(page.getByTestId('boost-button')).toBeEnabled();
});

test('default Hold ignores text focus and repeat, then sends start and stop edges', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));
  await expect(page.getByTestId('boost-button')).toBeEnabled();

  await page.evaluate(() => {
    const input = document.createElement('input');
    input.id = 'boost-focus-probe';
    document.body.appendChild(input);
    input.focus();
  });
  await page.keyboard.press('Space');
  expect(await socketMessages(page, socketIndex, 'GameCommandV2')).toEqual([]);

  await page.evaluate(() => {
    document.getElementById('boost-focus-probe')?.blur();
    document.getElementById('boost-focus-probe')?.remove();
  });
  await page.keyboard.down('Space');
  await page.evaluate(() => {
    window.dispatchEvent(new KeyboardEvent('keydown', {
      code: 'Space',
      key: ' ',
      repeat: true,
      bubbles: true,
      cancelable: true,
    }));
  });
  await page.keyboard.up('Space');

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(2);
  const commands = (await socketMessages(page, socketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.command.command);
  expect(commands).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
});

test('an uninterrupted Space Hold resumes Boost after depletion and recharge', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  const logicalBoostCommands = async () => {
    const deliveries = await socketMessages(page, socketIndex, 'GameCommandV2');
    return [...new Map(deliveries.map((message) => {
      const envelope = message.GameCommandV2;
      return [envelope.command_id.sequence, envelope.command.command];
    })).values()];
  };
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));
  await expect(page.getByTestId('boost-button')).toBeEnabled();

  await page.keyboard.down('Space');
  await expect.poll(logicalBoostCommands)
    .toHaveLength(1);

  const active = boostSnapshot(12, 7);
  active.GameEvent.event.Snapshot.game_state.arena.snakes[0].boost = {
    charge_ms: 50,
    active: true,
    intent: true,
  };
  active.GameEvent.event.Snapshot.game_state.arena.snakes[0].speed_milli = 1500;
  await emitServerMessage(page, socketIndex, active);

  const depleted = boostSnapshot(13, 8);
  depleted.GameEvent.event.Snapshot.game_state.arena.snakes[0].boost = {
    charge_ms: 0,
    active: false,
    intent: true,
  };
  await emitServerMessage(page, socketIndex, depleted);
  await expect(page.getByTestId('boost-button')).toBeEnabled();
  expect(await logicalBoostCommands()).toHaveLength(1);

  const recharged = boostSnapshot(14, 9);
  recharged.GameEvent.event.Snapshot.game_state.arena.snakes[0].boost = {
    charge_ms: 750,
    active: true,
    intent: true,
  };
  recharged.GameEvent.event.Snapshot.game_state.arena.snakes[0].speed_milli = 1500;
  await emitServerMessage(page, socketIndex, recharged);

  // Intent is latched in the engine across depletion, so recharge resumes
  // without another activation edge from the browser.
  await expect.poll(logicalBoostCommands)
    .toHaveLength(1);
  expect(await logicalBoostCommands()).toEqual([
    { ActivateBoost: { snake_id: 0 } },
  ]);

  await page.keyboard.up('Space');
  await expect.poll(logicalBoostCommands)
    .toHaveLength(2);
  expect(await logicalBoostCommands()).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
});

test('persisted Toggle starts and stops Boost on successive Space presses', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('snaketron:boost-input-mode:v1', 'toggle');
  });
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));

  await expect(page.getByRole('checkbox', { name: 'Hold to boost' })).not.toBeChecked();
  await page.keyboard.press('Space');
  await page.keyboard.press('Space');

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(2);
  const commands = (await socketMessages(page, socketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.command.command);
  expect(commands).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
});

test('window blur releases default Hold Boost once', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await emitServerMessage(page, socketIndex, boostSnapshot(11, 6));
  await expect(page.getByTestId('boost-button')).toBeEnabled();

  await page.keyboard.down('Space');
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);
  await page.evaluate(() => window.dispatchEvent(new Event('blur')));
  await page.keyboard.up('Space');

  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(2);
  const commands = (await socketMessages(page, socketIndex, 'GameCommandV2'))
    .map((message) => message.GameCommandV2.command.command);
  expect(commands).toEqual([
    { ActivateBoost: { snake_id: 0 } },
    { DeactivateBoost: { snake_id: 0 } },
  ]);
});

// A shipped build cannot update itself — an itch.io bundle has no
// reload-to-upgrade path at all — so no protocol disagreement may ever strand
// the player behind a screen they have no way to act on. Every one of these
// mismatches has to degrade to a console warning and keep playing.
test('a mismatched server protocol and missing capabilities still authenticate', async ({ page }) => {
  await page.goto('/');
  await expect.poll(() => page.evaluate(() => (
    window.__wsInstance ? window.__mockSockets.indexOf(window.__wsInstance) : -1
  ))).toBeGreaterThanOrEqual(0);
  const socketIndex = await page.evaluate(() => window.__mockSockets.indexOf(window.__wsInstance));
  await expect.poll(() => socketMessages(page, socketIndex, 'Authenticate')).toHaveLength(1);

  await emitServerMessage(page, socketIndex, {
    Authenticated: {
      task_boot_id: 'mismatched-task',
      protocol_version: 999,
      capabilities: [],
      socket_generation: 1,
    },
  });

  // Reaching JoinLobby proves the handshake was accepted despite both mismatches.
  await expect.poll(() => socketMessages(page, socketIndex, 'JoinLobby')).toHaveLength(2);
  await expect(page.getByTestId('client-update-required')).toHaveCount(0);
  expect(await page.evaluate((index) => window.__mockSockets[index].closeCalls, socketIndex))
    .toEqual([]);
});

test('a legacy client-update denial no longer strands the player', async ({ page }) => {
  const socketIndex = await establishAuthenticatedLobby(page);
  const socketCount = await page.evaluate(() => window.__mockSockets.length);

  await emitServerMessage(page, socketIndex, {
    AccessDenied: { reason: 'Client update required' },
  });
  await page.waitForTimeout(250);

  await expect(page.getByTestId('client-update-required')).toHaveCount(0);
  expect(await page.evaluate((index) => ({
    connected: window.__wsContext?.isConnected,
    activeSocketIndex: window.__wsInstance
      ? window.__mockSockets.indexOf(window.__wsInstance)
      : -1,
    closeCalls: window.__mockSockets[index].closeCalls,
    socketCount: window.__mockSockets.length,
  }), socketIndex)).toEqual({
    connected: true,
    activeSocketIndex: socketIndex,
    closeCalls: [],
    socketCount,
  });
});

test('terminal cutoff rejects a command crossed by buffered completion', async ({ page }) => {
  const socketIndex = await establishActiveGame(page);
  await page.keyboard.press('ArrowUp');
  await expect.poll(() => socketMessages(page, socketIndex, 'GameCommandV2'))
    .toHaveLength(1);

  const terminalSnapshot = snapshot(11, 6);
  terminalSnapshot.GameEvent.event.Snapshot.game_state.status = {
    Complete: { winning_snake_id: null },
  };
  await emitServerMessage(page, socketIndex, terminalSnapshot);
  await emitServerMessage(page, socketIndex, {
    CommandOutcomesComplete: {
      game_id: 42,
      terminal_rejection_reason: 'game completed',
    },
  });

  await expect(page.getByText(
    'Game completion did not reconcile every pending command. Your command outbox was retained safely.',
  )).toHaveCount(0);
  await page.waitForTimeout(1_500);
  expect(await socketMessages(page, socketIndex, 'GameCommandV2')).toHaveLength(1);

  await page.keyboard.press('ArrowLeft');
  await page.waitForTimeout(100);
  expect(await socketMessages(page, socketIndex, 'GameCommandV2')).toHaveLength(1);
});

test('an unacknowledged matchmaking admission replays only while restored state is waiting', async ({ page }) => {
  const oldSocketIndex = await establishAuthenticatedLobby(page);
  await page.evaluate(() => {
    window.__wsContext.sendMessage({
      QueueForMatch: {
        game_type: { FreeForAll: { max_players: 2 } },
        queue_mode: 'Quickmatch',
      },
    });
  });
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'QueueForMatch'))
    .toHaveLength(1);
  await emitServerMessage(page, oldSocketIndex, {
    AccessDenied: {
      reason: 'an unrelated request was rejected',
    },
  });

  const socketCountBeforeCrash = await page.evaluate(() => window.__mockSockets.length);
  await page.evaluate((index) => {
    window.__mockSockets[index].serverClose(1012, 'gateway died before queue admission');
  }, oldSocketIndex);
  await expect.poll(() => page.evaluate(() => window.__mockSockets.length))
    .toBeGreaterThan(socketCountBeforeCrash);
  const replacementSocketIndex = socketCountBeforeCrash;
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'Authenticate')).toHaveLength(1);
  await emitServerMessage(page, replacementSocketIndex, {
    Authenticated: {
      task_boot_id: 'replacement-lobby-task',
      protocol_version: 7,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 2,
    },
  });
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'JoinLobby'))
    .toHaveLength(1);
  expect(await socketMessages(page, replacementSocketIndex, 'QueueForMatch')).toEqual([]);

  await emitServerMessage(page, replacementSocketIndex, {
    LobbyUpdate: {
      ...lobbyUpdate.LobbyUpdate,
      lobby_code: 'OTHER-LOBBY',
      state: 'queued',
    },
  });
  expect(await socketMessages(page, replacementSocketIndex, 'QueueForMatch')).toEqual([]);

  await emitServerMessage(page, replacementSocketIndex, lobbyUpdate);
  await expect.poll(() => socketMessages(page, replacementSocketIndex, 'QueueForMatch'))
    .toHaveLength(1);

  await emitServerMessage(page, replacementSocketIndex, {
    LobbyUpdate: {
      ...lobbyUpdate.LobbyUpdate,
      state: 'queued',
    },
  });
  const socketCountBeforeAcknowledgedCrash = await page.evaluate(() => window.__mockSockets.length);
  await page.evaluate((index) => {
    window.__mockSockets[index].serverClose(1012, 'gateway died after queue admission');
  }, replacementSocketIndex);
  await expect.poll(() => page.evaluate(() => window.__mockSockets.length))
    .toBeGreaterThan(socketCountBeforeAcknowledgedCrash);
  const acknowledgedReplacementIndex = socketCountBeforeAcknowledgedCrash;
  await expect.poll(() => socketMessages(page, acknowledgedReplacementIndex, 'Authenticate'))
    .toHaveLength(1);
  await emitServerMessage(page, acknowledgedReplacementIndex, {
    Authenticated: {
      task_boot_id: 'acknowledged-replacement-task',
      protocol_version: 7,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 3,
    },
  });
  await expect.poll(() => socketMessages(page, acknowledgedReplacementIndex, 'JoinLobby'))
    .not.toHaveLength(0);
  await emitServerMessage(page, acknowledgedReplacementIndex, {
    LobbyUpdate: {
      ...lobbyUpdate.LobbyUpdate,
      state: 'queued',
    },
  });
  await page.waitForTimeout(250);
  expect(await socketMessages(page, acknowledgedReplacementIndex, 'QueueForMatch')).toEqual([]);
});

for (const [messageType, payload] of [
  ['QueueForMatch', {
    game_type: { FreeForAll: { max_players: 2 } },
    queue_mode: 'Quickmatch',
  }],
  ['QueueForMatchMulti', {
    game_types: [
      { FreeForAll: { max_players: 2 } },
      { TeamMatch: { per_team: 1 } },
    ],
    queue_mode: 'Quickmatch',
  }],
]) {
  test(`${messageType} retries the returned matchmaking admission failure`, async ({ page }) => {
    const socketIndex = await establishAuthenticatedLobby(page);
    await page.evaluate(({ messageType, payload }) => {
      window.__wsContext.sendMessage({ [messageType]: payload });
    }, { messageType, payload });
    await expect.poll(() => socketMessages(page, socketIndex, messageType)).toHaveLength(1);

    await emitServerMessage(page, socketIndex, {
      AccessDenied: { reason: RETRYABLE_MATCHMAKING_ADMISSION_REASON },
    });
    await expect.poll(() => socketMessages(page, socketIndex, messageType)).toHaveLength(2);
    const messages = await socketMessages(page, socketIndex, messageType);
    expect(messages[1]).toEqual(messages[0]);

    await emitServerMessage(page, socketIndex, {
      AccessDenied: { reason: RETRYABLE_MATCHMAKING_ADMISSION_REASON },
    });
    await emitServerMessage(page, socketIndex, {
      LobbyUpdate: {
        ...lobbyUpdate.LobbyUpdate,
        state: 'queued',
      },
    });
    await page.waitForTimeout(600);
    expect(await socketMessages(page, socketIndex, messageType)).toHaveLength(2);
  });
}

test('matchmaking admission retries are bounded and surface the final denial', async ({ page }) => {
  const socketIndex = await establishAuthenticatedLobby(page);
  await page.evaluate(() => {
    window.__matchmakingAdmissionDenials = [];
    window.__wsContext.onMessage('AccessDenied', (message) => {
      window.__matchmakingAdmissionDenials.push(message.data.reason);
    });
    window.__wsContext.sendMessage({
      QueueForMatch: {
        game_type: { FreeForAll: { max_players: 2 } },
        queue_mode: 'Quickmatch',
      },
    });
  });
  await expect.poll(() => socketMessages(page, socketIndex, 'QueueForMatch')).toHaveLength(1);

  for (let expectedSendCount = 2; expectedSendCount <= 4; expectedSendCount += 1) {
    await emitServerMessage(page, socketIndex, {
      AccessDenied: { reason: RETRYABLE_MATCHMAKING_ADMISSION_REASON },
    });
    await expect.poll(() => socketMessages(page, socketIndex, 'QueueForMatch'), {
      timeout: 2_000,
    }).toHaveLength(expectedSendCount);
    expect(await page.evaluate(() => window.__matchmakingAdmissionDenials)).toEqual([]);
  }

  await emitServerMessage(page, socketIndex, {
    AccessDenied: { reason: RETRYABLE_MATCHMAKING_ADMISSION_REASON },
  });
  await expect.poll(() => page.evaluate(() => window.__matchmakingAdmissionDenials))
    .toEqual([RETRYABLE_MATCHMAKING_ADMISSION_REASON]);
  await page.waitForTimeout(1_100);
  expect(await socketMessages(page, socketIndex, 'QueueForMatch')).toHaveLength(4);
});

test('unrelated access denials do not retry matchmaking admission', async ({ page }) => {
  const socketIndex = await establishAuthenticatedLobby(page);
  await page.evaluate(() => {
    window.__wsContext.sendMessage({
      QueueForMatch: {
        game_type: { FreeForAll: { max_players: 2 } },
        queue_mode: 'Quickmatch',
      },
    });
  });
  await expect.poll(() => socketMessages(page, socketIndex, 'QueueForMatch')).toHaveLength(1);
  await emitServerMessage(page, socketIndex, {
    AccessDenied: { reason: 'Join a lobby before queueing for matchmaking' },
  });
  await page.waitForTimeout(350);
  expect(await socketMessages(page, socketIndex, 'QueueForMatch')).toHaveLength(1);
});

test('planned lobby handoff replays only after the candidate restores authoritative waiting state', async ({ page }) => {
  const oldSocketIndex = await establishAuthenticatedLobby(page);
  await page.evaluate(() => {
    window.__wsContext.sendMessage({
      QueueForMatch: {
        game_type: { FreeForAll: { max_players: 2 } },
        queue_mode: 'Quickmatch',
      },
    });
  });
  await expect.poll(() => socketMessages(page, oldSocketIndex, 'QueueForMatch'))
    .toHaveLength(1);

  const candidateSocketIndex = await beginDrain(page, oldSocketIndex);
  await emitServerMessage(page, candidateSocketIndex, {
    Authenticated: {
      task_boot_id: 'planned-lobby-replacement',
      protocol_version: 7,
      capabilities: REQUIRED_CAPABILITIES,
      socket_generation: 2,
    },
  });
  await expect.poll(() => socketMessages(page, candidateSocketIndex, 'JoinLobby'))
    .toHaveLength(1);
  expect(await socketMessages(page, candidateSocketIndex, 'QueueForMatch')).toEqual([]);

  await emitServerMessage(page, candidateSocketIndex, lobbyUpdate);
  expect(await socketMessages(page, candidateSocketIndex, 'QueueForMatch')).toEqual([]);
  await confirmContinuityProbe(page, oldSocketIndex);

  await expect.poll(() => page.evaluate((index) => (
    window.__mockSockets.indexOf(window.__wsInstance) === index
  ), candidateSocketIndex)).toBe(true);
  await expect.poll(() => socketMessages(page, candidateSocketIndex, 'QueueForMatch'))
    .toHaveLength(1);
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
