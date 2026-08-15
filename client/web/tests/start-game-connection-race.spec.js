const { test, expect } = require('@playwright/test');
const { WebSocketServer } = require('ws');

test.use({ headless: true });

const REQUIRED_CAPABILITIES = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
  'terminal-command-cutoff-v1',
];

const appUrl = process.env.SNAKETRON_TEST_BASE_URL || 'http://localhost:3000';

test('an early Start Game click waits for the authenticated regional socket', async ({ page }) => {
  const receivedMessages = [];
  let authenticationAcknowledged = false;
  let commandSentBeforeAuthentication = false;
  const webSocketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve, reject) => {
    webSocketServer.once('listening', resolve);
    webSocketServer.once('error', reject);
  });

  const address = webSocketServer.address();
  if (!address || typeof address === 'string') {
    throw new Error('Failed to start the test WebSocket server');
  }

  webSocketServer.on('connection', socket => {
    socket.on('message', data => {
      const message = JSON.parse(data.toString());
      receivedMessages.push(message);

      if (message?.Ping) {
        socket.send(JSON.stringify({
          Pong: {
            client_time: message.Ping.client_time,
            server_time: Date.now(),
          },
        }));
      }

      if (message?.Authenticate) {
        setTimeout(() => {
          authenticationAcknowledged = true;
          socket.send(JSON.stringify({
            Authenticated: {
              task_boot_id: 'start-race-test',
              protocol_version: 10,
              capabilities: REQUIRED_CAPABILITIES,
              socket_generation: 1,
            },
          }));
        }, 200);
      }

      if (message === 'CreateLobby') {
        commandSentBeforeAuthentication = !authenticationAcknowledged;
        socket.send(JSON.stringify({
          LobbyCreated: { lobby_code: 'RACE123' },
        }));
      }
    });
  });

  let releaseRegions;
  const regionGate = new Promise(resolve => {
    releaseRegions = resolve;
  });

  await page.addInitScript(() => window.localStorage.clear());
  await page.route('http://localhost:8080/api/**', async route => {
    const { pathname } = new URL(route.request().url());
    const corsHeaders = { 'access-control-allow-origin': '*' };

    if (pathname === '/api/config') {
      await route.fulfill({
        contentType: 'application/json',
        headers: corsHeaders,
        body: JSON.stringify({
          version: 1,
          announcement: { enabled: false, message: '' },
        }),
      });
      return;
    }

    if (pathname === '/api/regions') {
      await regionGate;
      await route.fulfill({
        contentType: 'application/json',
        headers: corsHeaders,
        body: JSON.stringify([{
          id: 'race-region',
          name: 'Race Region',
          origin: 'http://race-region.test',
          ws_url: `ws://127.0.0.1:${address.port}`,
        }]),
      });
      return;
    }

    if (pathname === '/api/regions/user-counts') {
      await route.fulfill({
        contentType: 'application/json',
        headers: corsHeaders,
        body: JSON.stringify({ 'race-region': 0 }),
      });
      return;
    }

    if (pathname === '/api/auth/guest') {
      await route.fulfill({
        contentType: 'application/json',
        headers: corsHeaders,
        body: JSON.stringify({
          token: 'guest-race-token',
          user: { id: 42, username: 'RacePlayer', isGuest: true },
        }),
      });
      return;
    }

    await route.abort();
  });

  await page.route('http://race-region.test/api/health', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    headers: { 'access-control-allow-origin': '*' },
    body: JSON.stringify({ status: 'ok' }),
  }));

  try {
    await page.goto(appUrl);
    await page.getByPlaceholder('Nickname').fill('RacePlayer');

    const startButton = page.locator('button[type="submit"]');
    await expect(startButton).toBeEnabled();
    await startButton.click();
    await expect(startButton).toHaveText('Starting...');

    // Keep region discovery blocked beyond the old hard-coded 500 ms delay.
    // The submitted intent must remain pending rather than becoming a no-op.
    await page.waitForTimeout(650);
    expect(receivedMessages).toEqual([]);

    releaseRegions();

    await expect.poll(() => receivedMessages.some(message => message?.QueueForMatch)).toBe(true);
    expect(commandSentBeforeAuthentication).toBe(false);
    await expect(page.getByText('Finding match...', { exact: true })).toBeVisible();
    await expect(startButton).toHaveText('Finding Match...');

    const startSequence = receivedMessages.filter(message =>
      message?.Authenticate || message === 'CreateLobby' || message?.QueueForMatch
    );
    expect(startSequence).toEqual([
      {
        Authenticate: {
          token: 'guest-race-token',
          protocol_version: 10,
          distribution: 'web',
        },
      },
      'CreateLobby',
      {
        QueueForMatch: {
          game_type: { TeamMatch: { per_team: 1 } },
          queue_mode: 'Quickmatch',
        },
      },
    ]);
  } finally {
    releaseRegions();
    for (const client of webSocketServer.clients) {
      client.terminate();
    }
    await new Promise(resolve => webSocketServer.close(resolve));
  }
});
