const { test, expect } = require('@playwright/test');
const { WebSocketServer } = require('ws');

const tickerResponse = (items) => ({
  items,
  generatedAt: '2026-08-14T12:00:00Z',
  refreshAfterSeconds: 60,
});

const REQUIRED_CAPABILITIES = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
  'terminal-command-cutoff-v1',
];

test('a one-item news feed loops without a gap and pauses on hover', async ({
  page,
}) => {
  await page.route('**/api/news', (route) =>
    route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(
        tickerResponse([
          {
            id: 'performance:test',
            kind: 'system',
            text: 'Season 0 is live!',
            occurredAt: '2026-08-14T12:00:00Z',
            cta: null,
          },
        ]),
      ),
    }),
  );

  await page.goto('/');
  const ticker = page.getByTestId('news-ticker');
  await expect(ticker).toBeVisible();

  await expect
    .poll(() =>
      ticker.evaluate((element) => {
        const viewport = element.querySelector('.news-ticker-viewport');
        const groups = Array.from(
          element.querySelectorAll('.news-ticker-group'),
        );
        if (!viewport || groups.length < 2) {
          return false;
        }
        const groupWidth = groups[0].getBoundingClientRect().width;
        return (
          groupWidth > 0 &&
          (groups.length - 1) * groupWidth >=
            viewport.getBoundingClientRect().width
        );
      }),
    )
    .toBe(true);

  const track = ticker.locator('.news-ticker-track');
  await expect(ticker.locator('.news-ticker-toggle')).toHaveCount(0);
  await expect(track).toHaveCSS('animation-play-state', 'running');

  await ticker.hover();
  await expect(track).toHaveCSS(
    'animation-play-state',
    'paused',
  );

  await page.mouse.move(0, 0);
  await expect(track).toHaveCSS(
    'animation-play-state',
    'running',
  );
});

test('reduced-motion mode presents every headline as a static list', async ({
  page,
}) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.route('**/api/news', (route) =>
    route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(
        tickerResponse(
          [
            'Season 0 is live!',
            'Troncat89 dropped 820 points in Solo 2 minutes ago!',
            'SnakeByte won ranked Duel in 1:12 — 8 minutes ago!',
          ].map((text, index) => ({
              id: `item:${index}`,
              kind: index === 0 ? 'system' : 'performance',
              text,
              occurredAt: '2026-08-14T12:00:00Z',
              cta: null,
            }),
          ),
        ),
      ),
    }),
  );

  await page.goto('/');
  const list = page.getByTestId('news-ticker').locator('.news-ticker-accessible');
  await expect(list).toBeVisible();
  await expect(list.locator('li')).toHaveCount(3);
  await expect(
    list.getByText('Troncat89 dropped 820 points in Solo 2 minutes ago!'),
  ).toBeVisible();
  await expect(list.locator('li').first()).toHaveCSS('font-style', 'normal');
  await expect(list.locator('li').first()).toHaveCSS('font-weight', '400');
  await expect(list.locator('li').first()).toHaveCSS('text-transform', 'none');
  await expect(page.locator('.news-ticker-toggle')).toHaveCount(0);
});

test('data-backed fallback claims and CTAs come entirely from the server', async ({
  page,
}) => {
  await page.route('**/api/news', (route) =>
    route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(
        tickerResponse([
          {
            id: 'system:season',
            kind: 'system',
            text: 'Season 0 is on!',
            occurredAt: '2026-08-14T12:00:00Z',
            cta: {
              label: 'Open the leaderboard.',
              action: 'viewLeaderboards',
            },
          },
          {
            id: 'ranking:solo-leader',
            kind: 'ranking',
            text: "Lopatron33 has held Solo's top score for at least 35 days — 1,240 points!",
            occurredAt: '2026-07-10T12:00:00Z',
            cta: {
              label: 'Take a run.',
              action: 'playSolo',
            },
          },
          {
            id: 'result:ranked-2v2',
            kind: 'performance',
            text: 'Troncat89 & SnakeByte won ranked 2v2 by 12 points 3 minutes ago!',
            occurredAt: '2026-08-12T12:00:00Z',
            cta: {
              label: 'Queue up.',
              action: 'playRankedTwoVsTwo',
            },
          },
          {
            id: 'result:ranked-solo',
            kind: 'performance',
            text: 'Viper7 dropped 940 points in ranked Solo 2 minutes ago!',
            occurredAt: '2026-08-12T12:01:00Z',
            cta: {
              label: 'Take a ranked run.',
              action: 'playRankedSolo',
            },
          },
        ]),
      ),
    }),
  );

  await page.goto('/');
  const ticker = page.getByTestId('news-ticker');
  const list = ticker.locator('.news-ticker-accessible');

  await expect(list.locator('li')).toHaveCount(4);
  await expect(list.getByText('Season 0 is on!')).toHaveCount(1);
  await expect(
    list.getByText(
      "Lopatron33 has held Solo's top score for at least 35 days — 1,240 points!",
    ),
  ).toHaveCount(1);
  await expect(list.getByRole('link')).toHaveCount(1);
  await expect(list.getByRole('button')).toHaveCount(3);
  await expect(ticker.locator('.news-ticker-toggle')).toHaveCount(0);
  const leadersLink = list.getByRole('link', {
    name: 'Open the leaderboard.',
  });
  await expect(leadersLink).toHaveAttribute(
    'href',
    '/leaderboards',
  );
  await expect(leadersLink).not.toContainText('Season 0 is on!');

  const movingControls = ticker.locator(
    '.news-ticker-viewport .news-ticker-link',
  );
  await expect.poll(() => movingControls.count()).toBeGreaterThan(0);
  expect(
    await movingControls.evaluateAll((controls) =>
      controls.every((control) => control.getAttribute('tabindex') === '-1'),
    ),
  ).toBe(true);

  const movingCopy = ticker.locator('.news-ticker-viewport .news-ticker-copy').first();
  await expect(movingCopy).toHaveCSS('font-style', 'normal');
  await expect(movingCopy).toHaveCSS('font-weight', '400');
  await expect(movingCopy).toHaveCSS('text-transform', 'none');

  // No nickname has been entered, so every CTA below applies its selection and
  // then stops at the one thing the form still needs, exactly as the disabled
  // Start button would. Matchmaking is covered by the auto-start test below.
  const soloPrompt = list.getByRole('button', { name: 'Take a run.' });
  await soloPrompt.focus();
  await expect(list).toBeVisible();
  await soloPrompt.click();
  await expect(
    page.getByRole('button', { name: 'SOLO', exact: true }),
  ).toHaveAttribute('aria-pressed', 'true');
  await expect(
    page.getByRole('button', { name: 'DUEL', exact: true }),
  ).toHaveAttribute('aria-pressed', 'false');
  // A casual-queue CTA still opens the competitive queue for its mode.
  await expect(
    page.getByRole('checkbox', { name: 'Competitive' }),
  ).toBeChecked();
  await expect(page.getByPlaceholder('Nickname')).toBeFocused();

  const rankedTwoVsTwoPrompt = list.getByRole('button', {
    name: 'Queue up.',
  });
  await rankedTwoVsTwoPrompt.focus();
  await rankedTwoVsTwoPrompt.click();
  await expect(
    page.getByRole('button', { name: '2V2', exact: true }),
  ).toHaveAttribute('aria-pressed', 'true');
  await expect(
    page.getByRole('button', { name: 'SOLO', exact: true }),
  ).toHaveAttribute('aria-pressed', 'false');
  await expect(page.getByRole('checkbox', { name: 'Competitive' })).toBeChecked();

  const rankedSoloPrompt = list.getByRole('button', {
    name: 'Take a ranked run.',
  });
  await rankedSoloPrompt.focus();
  await rankedSoloPrompt.click();
  await expect(
    page.getByRole('button', { name: 'SOLO', exact: true }),
  ).toHaveAttribute('aria-pressed', 'true');
  await expect(
    page.getByRole('button', { name: '2V2', exact: true }),
  ).toHaveAttribute('aria-pressed', 'false');
  await expect(page.getByRole('checkbox', { name: 'Competitive' })).toBeChecked();

  await page.emulateMedia({ reducedMotion: 'reduce' });
  await expect(
    leadersLink.locator('.news-ticker-cta'),
  ).toHaveCSS('text-decoration-line', 'underline');
});

/**
 * The ticker's play CTAs are one-click entries into ranked matchmaking, so the
 * proof has to be the queue command on the wire — the mode chips alone cannot
 * distinguish "selected FFA" from "queued for competitive FFA".
 */
test('a play CTA queues the competitive version of its mode without a second click', async ({
page,
}) => {
const receivedMessages = [];
  const webSocketServer = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve, reject) => {
    webSocketServer.once('listening', resolve);
    webSocketServer.once('error', reject);
  });

  const address = webSocketServer.address();
  if (!address || typeof address === 'string') {
    throw new Error('Failed to start the test WebSocket server');
  }

  webSocketServer.on('connection', (socket) => {
    socket.on('message', (data) => {
      const message = JSON.parse(data.toString());
      receivedMessages.push(message);

      if (message?.Ping) {
        socket.send(
          JSON.stringify({
            Pong: {
              client_time: message.Ping.client_time,
              server_time: Date.now(),
            },
          }),
        );
      }

      if (message?.Authenticate) {
        socket.send(
          JSON.stringify({
            Authenticated: {
              task_boot_id: 'ticker-cta-test',
              protocol_version: 10,
              capabilities: REQUIRED_CAPABILITIES,
              socket_generation: 1,
            },
          }),
        );
      }

      if (message === 'CreateLobby') {
        socket.send(
          JSON.stringify({ LobbyCreated: { lobby_code: 'TICKER1' } }),
        );
      }
    });
  });

  await page.addInitScript(() => window.localStorage.clear());
  await page.route('**/api/**', async (route) => {
    const { pathname } = new URL(route.request().url());
    const headers = { 'access-control-allow-origin': '*' };
    const json = (body) =>
      route.fulfill({
        contentType: 'application/json',
        headers,
        body: JSON.stringify(body),
      });

    switch (pathname) {
      case '/api/news':
        return json(
          tickerResponse([
            {
              id: 'system:season',
              kind: 'system',
              text: 'Season 0: own the arena!',
              occurredAt: '2026-08-14T12:00:00Z',
              // The casual FFA action, i.e. the case this test exists for.
              cta: { label: 'Play FFA.', action: 'playFfa' },
            },
          ]),
        );
      case '/api/config':
        return json({
          version: 1,
          announcement: { enabled: false, message: '' },
        });
      case '/api/regions':
        return json([
          {
            id: 'ticker-region',
            name: 'Ticker Region',
            origin: 'http://ticker-region.test',
            ws_url: `ws://127.0.0.1:${address.port}`,
          },
        ]);
      case '/api/regions/user-counts':
        return json({ 'ticker-region': 0 });
      case '/api/auth/guest':
        return json({
          token: 'guest-ticker-token',
          user: { id: 77, username: 'TickerPlayer', isGuest: true },
        });
      case '/api/health':
        return json({ status: 'ok' });
      default:
        return route.abort();
    }
  });

  try {
    await page.goto('/');
    await page.getByPlaceholder('Nickname').fill('TickerPlayer');

    const prompt = page
      .getByTestId('news-ticker')
      .locator('.news-ticker-accessible')
      .getByRole('button', { name: 'Play FFA.' });
    await prompt.focus();
    await prompt.click();

    await expect
      .poll(() =>
        receivedMessages.find((message) => message?.QueueForMatch),
      )
      .toEqual({
        QueueForMatch: {
          game_type: { FreeForAll: { max_players: 4 } },
          queue_mode: 'Competitive',
        },
      });

    await expect(
      page.getByRole('button', { name: 'FFA', exact: true }),
    ).toHaveAttribute('aria-pressed', 'true');
    await expect(
      page.getByRole('checkbox', { name: 'Competitive' }),
    ).toBeChecked();
    await expect(page.locator('button[type="submit"]')).toHaveText(
      'Finding Match...',
    );

    // The CTAs go quiet while queued, but stay in the DOM so the marquee
    // does not reflow under the cursor that just clicked one.
    await expect(prompt).toBeDisabled();
    await expect(
      page.locator('.news-ticker-viewport .news-ticker-link'),
    ).not.toHaveCount(0);
  } finally {
    for (const client of webSocketServer.clients) {
      client.terminate();
    }
    await new Promise((resolve) => webSocketServer.close(resolve));
  }
});
