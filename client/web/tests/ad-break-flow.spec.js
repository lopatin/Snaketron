const { test, expect } = require('@playwright/test');
const { WebSocketServer } = require('ws');

test.use({ headless: true });

const appUrl = process.env.SNAKETRON_TEST_BASE_URL || 'http://127.0.0.1:3000';
const capabilities = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
  'terminal-command-cutoff-v1',
  'ad-break-v1',
];

const lobbyUpdate = (state, adBreak = undefined) => ({
  LobbyUpdate: {
    lobby_code: 'ADTEST1',
    members: [{ user_id: 42, username: 'AdPlayer', ts: Date.now() }],
    host_user_id: 42,
    state,
    preferences: { selected_modes: ['duel'], competitive: false },
    ...(adBreak ? { ad_break: adBreak } : {}),
  },
});

test('a missing provider resolves the server-owned break and shows a neutral wait screen', async ({ page }) => {
  const received = [];
  const webSocketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve, reject) => {
    webSocketServer.once('listening', resolve);
    webSocketServer.once('error', reject);
  });
  const address = webSocketServer.address();
  if (!address || typeof address === 'string') throw new Error('WebSocket server did not bind');

  webSocketServer.on('connection', socket => {
    socket.send(JSON.stringify({
      AdConfiguration: {
        enabled: true,
        provider: 'none',
        banners: { bottom: true, sides: true },
        video: { pre_match: true },
      },
    }));
    socket.on('message', bytes => {
      const message = JSON.parse(bytes.toString());
      received.push(message);
      if (message?.Ping) {
        socket.send(JSON.stringify({
          Pong: { client_time: message.Ping.client_time, server_time: Date.now() },
        }));
      }
      if (message?.Authenticate) {
        socket.send(JSON.stringify({
          Authenticated: {
            task_boot_id: 'ad-break-test',
            protocol_version: 9,
            capabilities,
            socket_generation: 1,
          },
        }));
      }
      if (message === 'CreateLobby') {
        socket.send(JSON.stringify({ LobbyCreated: { lobby_code: 'ADTEST1' } }));
      }
      if (message?.QueueForMatch) {
        socket.send(JSON.stringify(lobbyUpdate('ad_break', {
          id: 'break-test-1',
          expires_at_ms: Date.now() + 30_000,
          participant_count: 1,
          resolved_count: 0,
          resolved_user_ids: [],
          ad_user_ids: [42],
        })));
      }
      if (message?.AdBreakResolved?.break_id === 'break-test-1') {
        socket.send(JSON.stringify(lobbyUpdate('ad_break', {
          id: 'break-test-1',
          expires_at_ms: Date.now() + 30_000,
          participant_count: 1,
          resolved_count: 1,
          resolved_user_ids: [42],
          ad_user_ids: [42],
        })));
        setTimeout(() => socket.send(JSON.stringify(lobbyUpdate('queued'))), 700);
      }
    });
  });

  await page.addInitScript(() => {
    localStorage.clear();
    sessionStorage.clear();
  });
  await page.route('http://localhost:8080/api/**', async route => {
    const pathname = new URL(route.request().url()).pathname;
    const headers = { 'access-control-allow-origin': '*' };
    if (pathname === '/api/regions') {
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify([{
          id: 'ad-test-region',
          name: 'Ad Test Region',
          origin: 'http://ad-test-region.test',
          ws_url: `ws://127.0.0.1:${address.port}`,
        }]),
      });
    }
    if (pathname === '/api/regions/user-counts') {
      return route.fulfill({ contentType: 'application/json', headers, body: '{}' });
    }
    if (pathname === '/api/auth/guest') {
      return route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify({
          token: 'ad-break-guest-token',
          user: { id: 42, username: 'AdPlayer', isGuest: true },
        }),
      });
    }
    return route.abort();
  });
  await page.route('http://ad-test-region.test/api/health', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    headers: { 'access-control-allow-origin': '*' },
    body: JSON.stringify({ status: 'ok' }),
  }));

  try {
    await page.goto(appUrl);
    await page.getByPlaceholder('Nickname').fill('AdPlayer');
    await page.locator('button[type="submit"]').click();

    await expect.poll(() => received.some(message => message?.AdBreakResolved)).toBe(true);
    const resolution = received.find(message => message?.AdBreakResolved)?.AdBreakResolved;
    expect(resolution).toEqual({ break_id: 'break-test-1', resolution: 'unavailable' });

    const overlay = page.getByTestId('pre-match-ad-break');
    await expect(overlay).toBeVisible();
    await expect(overlay.getByText('You’re ready')).toBeVisible();
    await expect(overlay).not.toContainText(/disable|uninstall|ad.?block/i);
    await expect(overlay).toBeHidden({ timeout: 3_000 });
  } finally {
    for (const client of webSocketServer.clients) client.terminate();
    await new Promise(resolve => webSocketServer.close(resolve));
  }
});
