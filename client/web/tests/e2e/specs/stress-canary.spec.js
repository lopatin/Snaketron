const { test, expect } = require('@playwright/test');

const REQUIRED_CAPABILITY = 'stress-matchmaking-pool-v1';
const DEFAULT_MAX_DISRUPTION_MS = 10_000;
const POLL_INTERVAL_MS = 250;
const GAME_COMPLETION_TIMEOUT_MS = 3 * 60_000;

function requiredEnvironment(name) {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positiveIntegerEnvironment(name, fallback) {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

const target = requiredEnvironment('SNAKETRON_STRESS_TARGET').replace(/\/+$/, '');
const websocketUrl = requiredEnvironment('SNAKETRON_STRESS_WS_URL');
const region = requiredEnvironment('SNAKETRON_STRESS_REGION');
const stressKey = requiredEnvironment('SNAKETRON_STRESS_TEST_KEY');
const canaryDurationMs = positiveIntegerEnvironment(
  'SNAKETRON_STRESS_CANARY_DURATION_MS',
  10 * 60_000,
);
const maxDisruptionMs = positiveIntegerEnvironment(
  'SNAKETRON_STRESS_MAX_DISRUPTION_MS',
  DEFAULT_MAX_DISRUPTION_MS,
);

async function createStressGuest(request, suffix) {
  const response = await request.post(`${target}/api/auth/guest`, {
    headers: {
      'x-snaketron-stress-test-key': stressKey,
    },
    data: {
      nickname: `StressWeb${suffix}`,
    },
  });
  expect(response.status()).toBe(200);
  const payload = await response.json();
  expect(payload?.token).toEqual(expect.any(String));
  expect(payload?.user?.matchmakingPool).toBe('stress');
  return payload;
}

async function prepareBrowser(page, guest) {
  const pageErrors = [];
  page.on('pageerror', error => pageErrors.push(String(error)));

  await page.addInitScript(({ token, regionId, wsUrl, origin }) => {
    localStorage.setItem('token', token);
    localStorage.removeItem('snaketron:lastLobby');
    localStorage.setItem('snaketron_selected_region', JSON.stringify({
      regionId,
      wsUrl,
      origin,
      timestamp: Date.now(),
    }));
  }, {
    token: guest.token,
    regionId: region,
    wsUrl: websocketUrl,
    origin: target,
  });

  await page.goto('/');
  await expect.poll(() => page.evaluate(capability => ({
    connected: Boolean(window.__wsContext?.isConnected),
    authenticated: Boolean(window.__wsContext?.isSessionAuthenticated),
    capable: Boolean(window.__wsContext?.serverCapabilities?.has(capability)),
  }), REQUIRED_CAPABILITY)).toEqual({
    connected: true,
    authenticated: true,
    capable: true,
  });

  await page.evaluate(async () => {
    await window.__wsContext.createLobby();
    window.__wsContext.updateLobbyPreferences({
      selectedModes: ['duel'],
      competitive: false,
    });
  });

  return pageErrors;
}

async function queueDuel(page) {
  await page.evaluate(() => {
    window.__wsContext.sendMessage({
      QueueForMatch: {
        game_type: { TeamMatch: { per_team: 1 } },
        queue_mode: 'Quickmatch',
      },
    });
  });
}

async function sampleClientStates(pages, disruption) {
  const states = await Promise.all(pages.map(page => page.evaluate(() => {
    const gameMatch = window.location.pathname.match(/^\/play\/(\d+)$/);
    return {
      connected: Boolean(window.__wsContext?.isConnected),
      authenticated: Boolean(window.__wsContext?.isSessionAuthenticated),
      stale: document.body.innerText.includes('CONNECTION LOST'),
      failed: Boolean(document.querySelector('[data-testid="game-load-failure"]')),
      complete: Array.from(document.querySelectorAll('button'))
        .some(button => button.textContent?.trim() === 'Play Again'),
      gameId: gameMatch ? gameMatch[1] : null,
    };
  })));

  const sampledAt = Date.now();
  states.forEach((state, index) => {
    expect(state.failed).toBe(false);

    const usable = state.connected && state.authenticated;
    if (!usable && disruption.disconnectedAt[index] === null) {
      disruption.disconnectedAt[index] = sampledAt;
    } else if (usable && disruption.disconnectedAt[index] !== null) {
      disruption.maxDisconnectedMs = Math.max(
        disruption.maxDisconnectedMs,
        sampledAt - disruption.disconnectedAt[index],
      );
      disruption.disconnectedAt[index] = null;
    }
    if (state.stale && disruption.staleAt[index] === null) {
      disruption.staleAt[index] = sampledAt;
    } else if (!state.stale && disruption.staleAt[index] !== null) {
      disruption.maxStaleMs = Math.max(
        disruption.maxStaleMs,
        sampledAt - disruption.staleAt[index],
      );
      disruption.staleAt[index] = null;
    }

    for (const startedAt of [disruption.disconnectedAt[index], disruption.staleAt[index]]) {
      if (startedAt !== null) {
        expect(sampledAt - startedAt).toBeLessThanOrEqual(maxDisruptionMs);
      }
    }
  });

  return states;
}

async function waitForSharedPlayableGame(pages, disruption, previousGameId = null) {
  const deadline = Date.now() + 15_000;
  let sharedGameId = null;
  while (Date.now() < deadline) {
    const states = await sampleClientStates(pages, disruption);
    const gameIds = states.map(state => state.gameId);
    if (gameIds.every(gameId => gameId !== null && gameId !== previousGameId)
      && gameIds.every(gameId => gameId === gameIds[0])) {
      [sharedGameId] = gameIds;
      break;
    }
    await pages[0].waitForTimeout(POLL_INTERVAL_MS);
  }
  expect(sharedGameId).toMatch(/^\d+$/);

  await Promise.all(pages.map(async page => {
    await expect(page.getByTestId('game-load-failure')).toHaveCount(0);
    await expect(page.getByTestId('game-snapshot-loading')).toHaveCount(0);
  }));
  return sharedGameId;
}

function numericMetric(noteRecords, metricName) {
  let maximum = 0;
  const prefix = `ws_metric name=${metricName} `;
  for (const { note } of noteRecords) {
    if (!note.startsWith(prefix)) continue;
    const match = note.match(/(?:value|duration_ms)=(\d+)/);
    if (match) maximum = Math.max(maximum, Number.parseInt(match[1], 10));
  }
  return maximum;
}

function resyncHealth(noteRecords) {
  let startedAt = null;
  let maximumMs = 0;
  for (const { tsMs, note } of noteRecords) {
    if (note.startsWith('resync requested') && startedAt === null) {
      startedAt = tsMs;
    } else if (note === 'engine rebuilt from snapshot (resync)' && startedAt !== null) {
      maximumMs = Math.max(maximumMs, tsMs - startedAt);
      startedAt = null;
    }
  }
  return { maximumMs, recovered: startedAt === null };
}

async function traceNoteRecords(page) {
  return page.evaluate(() => (window.snaketronDebug?.getTraceRecords?.() || [])
    .flatMap(record => record.Note?.note ? [{
      tsMs: record.Note.ts_ms,
      note: record.Note.note,
    }] : []));
}

async function monitorUntilGameCompletes(pages, disruption) {
  const deadline = Date.now() + GAME_COMPLETION_TIMEOUT_MS;
  let directionIndex = 0;
  const directions = ['ArrowUp', 'ArrowRight', 'ArrowDown', 'ArrowLeft'];

  while (Date.now() < deadline) {
    const states = await sampleClientStates(pages, disruption);
    if (states.every(state => state.complete)) return;
    await Promise.all(pages.map(page => page.keyboard.press(directions[directionIndex])));
    directionIndex = (directionIndex + 1) % directions.length;
    await pages[0].waitForTimeout(POLL_INTERVAL_MS);
  }

  throw new Error('browser canary game did not complete authoritatively');
}

test('real web clients complete isolated games through natural scale-out', async ({ browser, request }) => {
  const unique = `${Date.now().toString(36)}${process.pid.toString(36)}`.slice(-8);
  const guests = await Promise.all([
    createStressGuest(request, `${unique}A`),
    createStressGuest(request, `${unique}B`),
  ]);
  const contexts = await Promise.all([browser.newContext(), browser.newContext()]);
  const pages = await Promise.all(contexts.map(context => context.newPage()));
  const pageErrorLists = await Promise.all(pages.map((page, index) => (
    prepareBrowser(page, guests[index])
  )));
  const disruption = {
    disconnectedAt: [null, null],
    staleAt: [null, null],
    maxDisconnectedMs: 0,
    maxStaleMs: 0,
    maxReportedReconnectMs: 0,
    maxReportedUsableGapMs: 0,
    maxReportedStaleMs: 0,
    maxReportedResyncMs: 0,
  };
  const finishedAt = Date.now() + canaryDurationMs;
  let completedGames = 0;
  let previousGameId = null;

  try {
    while (completedGames === 0 || Date.now() < finishedAt) {
      await Promise.all(pages.map(queueDuel));
      previousGameId = await waitForSharedPlayableGame(pages, disruption, previousGameId);
      await monitorUntilGameCompletes(pages, disruption);
      completedGames += 1;

      for (const page of pages) {
        const noteRecords = await traceNoteRecords(page);
        disruption.maxReportedReconnectMs = Math.max(
          disruption.maxReportedReconnectMs,
          numericMetric(noteRecords, 'reconnect_duration_ms'),
        );
        disruption.maxReportedUsableGapMs = Math.max(
          disruption.maxReportedUsableGapMs,
          numericMetric(noteRecords, 'usable_session_gap_ms'),
        );
        disruption.maxReportedStaleMs = Math.max(
          disruption.maxReportedStaleMs,
          numericMetric(noteRecords, 'stale_overlay_duration_ms'),
        );
        const resync = resyncHealth(noteRecords);
        expect(resync.recovered).toBe(true);
        disruption.maxReportedResyncMs = Math.max(
          disruption.maxReportedResyncMs,
          resync.maximumMs,
        );
      }
    }

    await sampleClientStates(pages, disruption);
    expect(completedGames).toBeGreaterThan(0);
    expect(disruption.disconnectedAt).toEqual([null, null]);
    expect(disruption.staleAt).toEqual([null, null]);
    expect(disruption.maxDisconnectedMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(disruption.maxStaleMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(disruption.maxReportedReconnectMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(disruption.maxReportedUsableGapMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(disruption.maxReportedStaleMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(disruption.maxReportedResyncMs).toBeLessThanOrEqual(maxDisruptionMs);
    expect(pageErrorLists.flat()).toEqual([]);
  } finally {
    await Promise.all(contexts.map(context => context.close()));
  }
});
