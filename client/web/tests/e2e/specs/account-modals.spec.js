const { test, expect } = require('@playwright/test');

test.use({ headless: true });

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('token', 'account-modal-test-token');

    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input, init) => {
      const url = typeof input === 'string'
        ? input
        : (input instanceof URL ? input.href : input.url);

      if (url.endsWith('/client_bg.wasm')) {
        return nativeFetch(input, init);
      }

      let payload;
      if (url.endsWith('/api/auth/me')) {
        payload = { id: 7, username: 'ModalTester', mmr: 1234, isGuest: false, isAdmin: false };
      } else if (url.endsWith('/api/config')) {
        payload = {
          version: 1,
          announcement: { enabled: false, message: '' },
          ads: { postMatchEnabled: false, minimumIntervalMinutes: 10 },
        };
      } else if (url.includes('/api/history?')) {
        payload = {
          entries: [{
            schemaVersion: 1,
            gameId: 99,
            startedAtMs: 1_725_000_000_000,
            endedAtMs: 1_725_000_125_000,
            durationMs: 125_000,
            mode: 'duel',
            modeLabel: 'Duel',
            queueMode: 'competitive',
            isPrivate: false,
            isStressTest: false,
            completedByInactivity: false,
            players: [{
              userId: 7,
              username: 'ModalTester',
              teamId: null,
              score: 4321,
              teamScore: null,
              xpGained: 80,
              mmrDelta: 12,
              outcome: 'win',
            }],
            winnerUserIds: [7],
            snapshotAvailableUntilMs: 1_727_592_000_000,
          }],
          nextCursor: null,
        };
      } else if (url.endsWith('/api/regions')) {
        payload = [];
      } else if (url.endsWith('/api/regions/user-counts')) {
        payload = {};
      } else {
        throw new Error(`Unexpected fetch in account modal test: ${url}`);
      }

      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    };
  });
});

test('Profile and History open over the current page without navigation', async ({ page }) => {
  await page.goto('/');
  const originalUrl = page.url();
  const accountTrigger = page.getByRole('button', { name: 'ModalTester' });

  await expect(accountTrigger).toBeVisible();
  await accountTrigger.click();
  await page.getByRole('menuitem', { name: 'Profile' }).click();

  const profileDialog = page.getByRole('dialog', { name: 'Profile' });
  await expect(profileDialog).toBeVisible();
  await expect(page.getByRole('dialog')).toHaveCount(1);
  await expect(profileDialog.getByText('ModalTester', { exact: true })).toBeVisible();
  await expect(profileDialog.getByText('#7', { exact: true })).toBeVisible();
  await expect(profileDialog.getByText('1,234', { exact: true })).toBeVisible();
  expect(page.url()).toBe(originalUrl);

  await profileDialog.getByRole('button', { name: 'Done' }).click();
  await expect(profileDialog).toHaveCount(0);
  await expect(accountTrigger).toBeFocused();

  await accountTrigger.click();
  await page.getByRole('menuitem', { name: 'History' }).click();

  const historyDialog = page.getByRole('dialog', { name: 'History' });
  await expect(historyDialog).toBeVisible();
  await expect(page.getByRole('dialog')).toHaveCount(1);
  await expect(historyDialog.getByText('Duel', { exact: true })).toBeVisible();
  await expect(historyDialog.getByText('Victory', { exact: true })).toBeVisible();
  await expect(historyDialog.getByText('4,321', { exact: true })).toBeVisible();
  await expect(historyDialog.getByText('+80', { exact: true })).toBeVisible();
  await expect(historyDialog.getByText('+12', { exact: true })).toBeVisible();
  await expect(historyDialog.getByText('2:05', { exact: true })).toBeVisible();
  expect(page.url()).toBe(originalUrl);

  await page.setViewportSize({ width: 360, height: 720 });
  const mobileDialogBounds = await historyDialog.boundingBox();
  expect(mobileDialogBounds).not.toBeNull();
  expect(mobileDialogBounds.x).toBeGreaterThanOrEqual(0);
  expect(mobileDialogBounds.x + mobileDialogBounds.width).toBeLessThanOrEqual(360);
  expect(mobileDialogBounds.y + mobileDialogBounds.height).toBeLessThanOrEqual(720);

  await page.keyboard.press('Escape');
  await expect(historyDialog).toHaveCount(0);
  await expect(accountTrigger).toBeFocused();
  expect(page.url()).toBe(originalUrl);
});

for (const legacyPath of ['/profile', '/history']) {
  test(`${legacyPath} redirects to Play instead of rendering an account page`, async ({ page }) => {
    await page.goto(legacyPath);
    await expect(page).toHaveURL('/');
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'ModalTester' })).toBeVisible();
  });
}
