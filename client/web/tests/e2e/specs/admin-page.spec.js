const { test, expect } = require('@playwright/test');

test.use({ headless: true });

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('token', 'admin-page-test-token');
    localStorage.setItem('__admin-test-authorized', 'true');

    window.__adminRecord = {
      schemaVersion: 2,
      version: 1,
      config: {
        announcement: { enabled: false, message: '' },
        ads: {
          enabled: false,
          minimumGamesPlayed: 1,
          minimumIntervalMinutes: 10,
          distributions: {
            web: { enabled: false },
            crazygames: { enabled: false },
            itch: { enabled: false },
          },
        },
        history: { snapshotRetentionDays: 30, summaryRetentionDays: 365 },
      },
      updatedAtMs: 1_725_000_000_000,
      updatedBy: { userId: 1, username: 'OpsAdmin' },
    };
    window.__adminPutBody = null;

    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = typeof input === 'string'
        ? input
        : (input instanceof URL ? input.href : input.url);
      if (url.endsWith('/client_bg.wasm')) {
        return nativeFetch(input, init);
      }

      const { pathname } = new URL(url);
      let payload;
      if (pathname === '/api/auth/me') {
        payload = {
          id: 1,
          username: 'OpsAdmin',
          mmr: 1500,
          isGuest: false,
          isAdmin: localStorage.getItem('__admin-test-authorized') === 'true',
        };
      } else if (pathname === '/api/config') {
        payload = {
          version: window.__adminRecord.version,
          announcement: window.__adminRecord.config.announcement,
        };
      } else if (pathname === '/api/admin/config' && (init?.method || 'GET') === 'PUT') {
        window.__adminPutBody = JSON.parse(init.body);
        window.__adminRecord = {
          ...window.__adminRecord,
          version: 2,
          config: window.__adminPutBody.config,
          updatedAtMs: 1_725_000_100_000,
        };
        payload = window.__adminRecord;
      } else if (pathname === '/api/admin/config') {
        payload = window.__adminRecord;
      } else if (pathname === '/api/admin/history') {
        payload = {
          entries: [{
            schemaVersion: 1,
            gameId: 501,
            startedAtMs: 1_725_000_000_000,
            endedAtMs: 1_725_000_092_000,
            durationMs: 92_000,
            mode: 'duel',
            modeLabel: 'Duel',
            queueMode: 'competitive',
            isPrivate: false,
            isStressTest: false,
            completedByInactivity: false,
            players: [
              {
                userId: 7,
                username: 'BlueSnake',
                teamId: null,
                score: 3250,
                teamScore: null,
                xpGained: 70,
                mmrDelta: 14,
                outcome: 'win',
              },
              {
                userId: 8,
                username: 'CoralSnake',
                teamId: null,
                score: 2890,
                teamScore: null,
                xpGained: 40,
                mmrDelta: -14,
                outcome: 'loss',
              },
            ],
            winnerUserIds: [7],
            snapshotAvailableUntilMs: 1_727_592_000_000,
          }],
          nextCursor: null,
        };
      } else if (pathname === '/api/admin/config/audit') {
        payload = { entries: [window.__adminRecord], nextCursor: null };
      } else if (pathname === '/api/regions') {
        payload = [];
      } else if (pathname === '/api/regions/user-counts') {
        payload = {};
      } else {
        throw new Error(`Unexpected fetch in admin page test: ${url}`);
      }

      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };
  });
});

test('admin can inspect history, publish runtime controls, and review audit data', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'OpsAdmin' }).click();
  await expect(page.getByRole('menuitem', { name: 'Admin' })).toBeVisible();
  await page.getByRole('menuitem', { name: 'Admin' }).click();

  await expect(page).toHaveURL('/admin');
  await expect(page.getByText('Control room', { exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible();

  await page.getByRole('button', { name: /Configuration/ }).click();
  await expect(page.getByRole('heading', { name: 'Configuration' })).toBeVisible();
  await page.getByLabel('Publish banner').check();
  await page.getByLabel('Message').fill('Scheduled maintenance begins after this round.');
  await page.getByLabel('Enable pre-match video ads').check();
  await page.getByLabel('Enable ads for Website').check();
  await page.getByLabel('Enable ads for CrazyGames').check();
  await page.getByLabel('Minimum games played').fill('3');
  await page.getByLabel('Minimum interval').fill('15');
  await page.getByRole('button', { name: 'Publish configuration' }).click();
  await expect(page.getByText('Configuration v2 is live.')).toBeVisible();
  await expect(
    page.getByLabel('Service announcement').getByText('Scheduled maintenance begins after this round.'),
  ).toBeVisible();
  await expect.poll(() => page.evaluate(() => window.__adminPutBody.expectedVersion)).toBe(1);
  await expect.poll(() => page.evaluate(() => window.__adminPutBody.config.ads)).toEqual({
    enabled: true,
    minimumGamesPlayed: 3,
    minimumIntervalMinutes: 15,
    distributions: {
      web: { enabled: true },
      crazygames: { enabled: true },
      itch: { enabled: false },
    },
  });

  await page.getByRole('button', { name: /Match history/ }).click();
  await expect(page.getByRole('heading', { name: 'Match history' })).toBeVisible();
  await expect(page.getByText('BlueSnake', { exact: true })).toBeVisible();
  await expect(page.getByText('CoralSnake', { exact: true })).toBeVisible();
  await expect(page.getByText('3,250', { exact: true })).toBeVisible();
  await expect(page.getByText('-14', { exact: true })).toBeVisible();

  await page.getByRole('button', { name: /Audit/ }).click();
  await expect(page.getByRole('heading', { name: 'Audit' })).toBeVisible();
  await expect(page.getByText('Version 2', { exact: true })).toBeVisible();
  await expect(page.getByText('OpsAdmin (#1)', { exact: true })).toBeVisible();
  await expect(page.getByText(
    'Enabled · Website, CrazyGames · 3+ games · 15m interval',
    { exact: true },
  )).toBeVisible();
});

test('a non-admin cannot open the admin route or see its account action', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('__admin-test-authorized', 'false'));
  await page.goto('/admin');
  await expect(page).toHaveURL('/');
  await expect(page.getByRole('heading', { name: 'Overview' })).toHaveCount(0);

  await page.getByRole('button', { name: 'OpsAdmin' }).click();
  await expect(page.getByRole('menuitem', { name: 'Admin' })).toHaveCount(0);
});
