const { test, expect } = require('@playwright/test');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');

test('scenario replay lab switches checked fixtures and viewer speed', async ({ page }) => {
  await page.goto('/qa/scenario-player?scenario=demolition-cutoff&timeScale=0.5');

  const canvas = page.getByTestId('scenario-canvas');
  await expect(canvas).toHaveAttribute('data-scenario', 'demolition-cutoff');
  await expect(canvas.locator('canvas')).toHaveAttribute('data-ready', 'true');
  await expect(page.getByTestId('scenario-playback-rail')).toBeVisible();
  await expect(page.getByRole('button', { name: '0.5×' })).toHaveAttribute('aria-pressed', 'true');

  await page.getByRole('button', { name: 'Fourteen-point bank' }).click();
  await expect(page).toHaveURL(/scenario=team-bank/);
  await expect(canvas).toHaveAttribute('data-scenario', 'team-bank');
  await expect(canvas.locator('canvas')).toHaveAttribute('data-ready', 'true');

  await page.getByRole('button', { name: '2×' }).click();
  await expect(page).toHaveURL(/timeScale=2/);
  await expect(page.getByRole('button', { name: '2×' })).toHaveAttribute('aria-pressed', 'true');
});

test('capture mode is full-bleed, provider-free, and deterministically stepped', async ({ page }) => {
  const requests = [];
  page.on('request', (request) => requests.push(request.url()));

  await page.goto('/qa/scenario-player?capture=1&scenario=combo-frenzy');
  await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__));
  await page.evaluate(() => window.__SNAKETRON_CAPTURE__.ready());

  await expect(page.getByTestId('scenario-capture')).toBeVisible();
  await expect(page.locator('.home-arena-backdrop')).toHaveCount(0);
  await expect(page.locator('.scenario-qa__masthead')).toHaveCount(0);
  await expect(page.getByTestId('scenario-playback-rail')).toHaveCount(0);

  const result = await page.evaluate(async () => {
    const capture = window.__SNAKETRON_CAPTURE__;
    const before = capture.renderedTick();
    await capture.stepMs(250);
    return {
      before,
      after: capture.renderedTick(),
      durationMs: capture.durationMs(),
      cueTrack: capture.cueTrack(),
      ready: document.documentElement.dataset.scenarioCaptureReady,
    };
  });

  expect(result.after).toBeGreaterThan(result.before);
  expect(result.durationMs).toBeGreaterThan(250);
  expect(result.cueTrack.end_tick).toBeGreaterThan(result.cueTrack.start_tick);
  expect(result.ready).toBe('true');
  expect(requests.filter((url) => /\/api\/|wss?:/.test(url))).toEqual([]);
});

test('capture mode scrubs the combo flourish from virtual time', async ({ page }) => {
  const captureOnset = async () => {
    await page.goto('/qa/scenario-player?capture=1&scenario=combo-frenzy');
    await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__));
    await page.evaluate(() => window.__SNAKETRON_CAPTURE__.ready());
    await page.evaluate(async () => {
      await window.__SNAKETRON_CAPTURE__.stepMs(100);
      await window.__SNAKETRON_CAPTURE__.stepMs(100);
    });

    const burst = page.getByTestId('combo-callout-burst');
    await expect(burst).toHaveCSS('animation-play-state', 'paused');
    await expect(burst).toHaveCSS('animation-delay', '0s');
    return page.getByTestId('scenario-capture').screenshot();
  };

  const first = await captureOnset();
  const second = await captureOnset();
  expect(second.equals(first)).toBe(true);

  await page.evaluate(() => window.__SNAKETRON_CAPTURE__.stepMs(50));
  await expect(page.getByTestId('combo-callout-burst')).toHaveCSS(
    'animation-delay',
    '-0.05s',
  );
});

test('capture mode accepts a pre-boot serialized scenario source', async ({ page }) => {
  const sourceJson = readFileSync(join(__dirname, '../scenarios/team-bank.json'), 'utf8');
  await page.addInitScript((script) => {
    window.__SNAKETRON_CAPTURE_SOURCE__ = { kind: 'script', script };
  }, sourceJson);

  await page.goto('/qa/scenario-player?capture=1&scenario=file-source');
  await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__));
  await page.evaluate(() => window.__SNAKETRON_CAPTURE__.ready());

  await expect(page.getByTestId('scenario-canvas')).toHaveAttribute('data-scenario', 'team-bank');
  expect(await page.evaluate(() => window.__SNAKETRON_CAPTURE__.starSnakeId())).toBe(0);
});

test('viewer capture clock preserves the canonical PotG timing contract', async ({ page }) => {
  const clipJson = readFileSync(join(
    __dirname,
    '../../../docs/qa/play-of-the-game-calibration/clips/01-game-8000148.json',
  ), 'utf8');
  await page.addInitScript((clip) => {
    window.__SNAKETRON_CAPTURE_SOURCE__ = { kind: 'highlight', clip };
  }, clipJson);

  await page.goto('/qa/scenario-player?capture=1&scenario=potg-viewer-clock');
  await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__));
  const timing = await page.evaluate(async () => {
    const capture = window.__SNAKETRON_CAPTURE__;
    await capture.ready();
    const cueTrack = capture.cueTrack();
    const focusSourceMs = (658 - cueTrack.start_tick) * cueTrack.tick_duration_ms;
    await capture.stepViewerMs(8_000);
    const focusTick = capture.renderedTick();
    await capture.stepViewerMs(4_500);
    return {
      sourceDurationMs: capture.durationMs(),
      viewerDurationMs: capture.viewerDurationMs(),
      focusViewerMs: capture.viewerMsForSourceMs(focusSourceMs),
      focusTick,
      finalTick: capture.renderedTick(),
      expectedFinalTick: cueTrack.end_tick,
    };
  });

  expect(timing).toEqual({
    sourceDurationMs: 9_000,
    viewerDurationMs: 12_500,
    focusViewerMs: 8_000,
    focusTick: 658,
    finalTick: 718,
    expectedFinalTick: 718,
  });
});

test('capture readiness rejects fatally when the crash sprite cannot decode', async ({ page }) => {
  await page.route('**/*crash-explosion.png', (route) => route.abort('failed'));
  await page.goto('/qa/scenario-player?capture=1&scenario=team-bank');
  await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__));

  const message = await page.evaluate(async () => {
    try {
      await window.__SNAKETRON_CAPTURE__.ready();
      return null;
    } catch (error) {
      return error instanceof Error ? error.message : String(error);
    }
  });

  expect(message).toMatch(/decode|image|sprite/i);
  await expect(page.getByTestId('scenario-canvas')).toHaveAttribute('data-playback', 'error');
  await expect(page.getByRole('alert')).toContainText('Replay unavailable');
  expect(await page.evaluate(() => (
    document.documentElement.dataset.scenarioCaptureReady
  ))).toBeUndefined();
});

test('reduced motion poses the poster until the viewer explicitly plays', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto('/qa/scenario-player?scenario=team-bank');

  const canvas = page.getByTestId('scenario-canvas');
  await expect(canvas.locator('canvas')).toHaveAttribute('data-ready', 'true');
  await expect(canvas).toHaveAttribute('data-motion', 'reduced');
  await expect(canvas).toHaveAttribute('data-playback', 'paused');

  await page.getByRole('button', { name: 'Play replay animation' }).click();
  await expect(canvas).toHaveAttribute('data-motion', 'explicit');
});

test('the home play surface embeds a replay without waiting for app transport', async ({ page }) => {
  await page.goto('/');

  const spotlight = page.getByTestId('scenario-marketing');
  await expect(spotlight).toBeVisible();
  await expect(spotlight.getByTestId('scenario-canvas-surface')).toHaveAttribute(
    'data-ready',
    'true',
  );
  await expect(spotlight).toContainText('Real engine');
});
