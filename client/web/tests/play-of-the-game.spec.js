const { test, expect } = require('@playwright/test');

const apiOrSocketRequests = (requests) => requests.filter((url) => (
  /\/api\//.test(url) || /^wss?:/.test(url)
));

test('valid PotG is network-free, legible at 560px, and starts inside the quickmatch budget', async ({ page }) => {
  await page.setViewportSize({ width: 600, height: 900 });
  const requests = [];
  page.on('request', (request) => requests.push(request.url()));
  const openedAt = Date.now();

  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  const band = page.getByTestId('play-of-the-game');
  const canvas = band.getByTestId('scenario-canvas-surface');
  await expect(canvas).toHaveAttribute('data-ready', 'true');
  await band.scrollIntoViewIfNeeded();
  await expect(band).toHaveAttribute('data-playback', 'playing', { timeout: 4_000 });
  expect(Date.now() - openedAt).toBeLessThan(4_000);

  const box = await band.boundingBox();
  expect(box).not.toBeNull();
  expect(box.width).toBeGreaterThanOrEqual(556);
  expect(box.width).toBeLessThanOrEqual(560);
  await expect(band.locator('.potg-star__name')).toHaveText('BANKER');
  await expect(band.locator('.potg-star__reason')).toHaveText('Goal run — 15 points');
  expect(await band.locator('.potg-star__name').evaluateAll((nodes) => (
    nodes.every((node) => node.scrollWidth <= node.clientWidth)
  ))).toBe(true);
  expect(apiOrSocketRequests(requests)).toEqual([]);
});

test('the star wears a rank badge only when this client knows their rank', async ({ page }) => {
  // No rating in scope: the clip alone never carries the star's standing, and
  // a guessed badge would be a false claim about a real player's rank.
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');
  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await expect(band.locator('.potg-star__rank')).toHaveCount(0);

  // The star is the local player and their post-match rating has landed.
  await page.goto('/qa/play-of-the-game?state=ranked&chrome=0');
  const ranked = page.getByTestId('play-of-the-game');
  await expect(ranked.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await expect(ranked.locator('.potg-star__rank')).toBeVisible();
});

test('the achievement is a popover on the star plate, not a second plate', async ({ page }) => {
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');
  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await expect(band).toHaveAttribute('data-intro', 'done', { timeout: 6_000 });

  // Only the star's plate occupies the arena; the reason is hidden until asked for.
  const reason = band.locator('.potg-star__reason');
  await expect(reason).toHaveCSS('opacity', '0');

  await band.getByTestId('potg-star').hover();
  await expect(reason).toHaveCSS('opacity', '1');
  await expect(reason).toHaveText('Goal run — 15 points');

  // Keyboard users get the same detail.
  await page.mouse.move(0, 0);
  await expect(reason).toHaveCSS('opacity', '0');
  await band.getByTestId('potg-star').focus();
  await expect(reason).toHaveCSS('opacity', '1');
});

test('ready PotG lower third remains readable at the 375px mobile target', async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await band.scrollIntoViewIfNeeded();
  await expect(band.locator('.potg-lower-third')).toBeVisible();
  await expect(band.locator('.potg-star__name')).toHaveText('BANKER');
  await expect(band.locator('.potg-star__reason')).toHaveText('Goal run — 15 points');

  const metrics = await band.evaluate((element) => {
    const star = element.querySelector('.potg-star__name');
    const reason = element.querySelector('.potg-star__reason');
    const rect = element.getBoundingClientRect();
    return {
      width: rect.width,
      starFits: star.scrollWidth <= star.clientWidth,
      reasonFits: reason.scrollWidth <= reason.clientWidth,
      starFont: Number.parseFloat(getComputedStyle(star).fontSize),
      reasonFont: Number.parseFloat(getComputedStyle(reason).fontSize),
    };
  });
  expect(metrics.width).toBeGreaterThanOrEqual(355);
  expect(metrics.width).toBeLessThanOrEqual(360);
  expect(metrics.starFits).toBe(true);
  expect(metrics.reasonFits).toBe(true);
  expect(metrics.starFont).toBeGreaterThanOrEqual(10);
  expect(metrics.reasonFont).toBeGreaterThanOrEqual(6.5);
});

test('one-shot autoplay remains paused until IntersectionObserver reports the band visible', async ({ page }) => {
  await page.addInitScript(() => {
    const observers = [];
    window.IntersectionObserver = class IntersectionObserverStub {
      constructor(callback) {
        this.callback = callback;
        this.target = null;
        observers.push(this);
      }

      observe(target) {
        this.target = target;
        this.callback([{ target, isIntersecting: false, intersectionRatio: 0 }], this);
      }

      unobserve() {}

      disconnect() {}

      takeRecords() { return []; }
    };
    window.__setPotgIntersection = (ratio) => {
      for (const observer of observers) {
        observer.callback([{
          target: observer.target,
          isIntersecting: ratio > 0,
          intersectionRatio: ratio,
        }], observer);
      }
    };
  });

  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');
  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await expect(band).toHaveAttribute('data-playback', 'paused');
  await page.waitForTimeout(600);
  await expect(band).toHaveAttribute('data-playback', 'paused');

  await page.evaluate(() => window.__setPotgIntersection(1));
  await expect(band).toHaveAttribute('data-playback', 'playing');
});

test('the terminal frame freezes and the replay control restarts it', async ({ page }) => {
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await band.scrollIntoViewIfNeeded();
  await expect(band).toHaveAttribute('data-playback', 'playing', { timeout: 5_000 });
  await expect(band).toHaveAttribute('data-playback', 'complete', { timeout: 15_000 });

  // The control is an icon button matched to the results card's own close
  // button, so it is identified by its accessible name, not by visible text.
  const replay = page.getByTestId('potg-replay');
  await expect(replay).toBeVisible();
  await expect(replay).toBeEnabled();
  await expect(replay).toHaveAccessibleName(/Replay play of the game/i);
  await replay.click();
  await expect(band).toHaveAttribute('data-playback', 'playing');
});

test('reduced-motion starts on the focus poster and animates only after explicit play', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  const band = page.getByTestId('play-of-the-game');
  const scenario = band.getByTestId('scenario-canvas');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await band.scrollIntoViewIfNeeded();
  await expect(scenario).toHaveAttribute('data-motion', 'reduced');
  await expect(band).toHaveAttribute('data-playback', 'paused');

  await scenario.getByRole('button', { name: 'Play replay animation' }).click();
  await expect(scenario).toHaveAttribute('data-motion', 'explicit');
  await expect(band).toHaveAttribute('data-playback', 'playing');
});

test('a sprite decode failure reaches PotG onError and collapses to the poster', async ({ page }) => {
  await page.route('**/*crash-explosion.png', (route) => route.abort('failed'));
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  await expect(page.getByTestId('potg-render-fallback')).toBeVisible();
  await expect(page.getByTestId('scenario-canvas')).toHaveCount(0);
});

test('modal playback sustains at least 30fps under Chrome 4x CPU throttle', async ({ page, browserName }) => {
  test.skip(browserName !== 'chromium', 'Chrome CPU throttling requires a CDP session');
  const session = await page.context().newCDPSession(page);
  await session.send('Emulation.setCPUThrottlingRate', { rate: 4 });
  await page.goto('/qa/play-of-the-game?state=ready&chrome=0');

  const band = page.getByTestId('play-of-the-game');
  await expect(band.getByTestId('scenario-canvas-surface')).toHaveAttribute('data-ready', 'true');
  await band.scrollIntoViewIfNeeded();
  await expect(band).toHaveAttribute('data-playback', 'playing');

  const fps = await page.evaluate(() => new Promise((resolve) => {
    const startedAt = performance.now();
    let frames = 0;
    const sample = (now) => {
      frames += 1;
      if (now - startedAt >= 2_000) {
        resolve((frames - 1) * 1_000 / (now - startedAt));
        return;
      }
      requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
  }));
  expect(fps).toBeGreaterThanOrEqual(30);
});

test('the panel stays closed while pending, and both fills arrive by animating', async ({ page }) => {
  // Nothing is reserved while the server is still cutting: the panel has to
  // be closed for its arrival to be an opening rather than a content swap.
  await page.goto('/qa/play-of-the-game?state=pending&chrome=0');
  const card = page.getByTestId('game-over-card');
  await expect(card).toBeVisible();
  await expect(page.getByTestId('play-of-the-game')).toHaveCount(0);
  await expect(page.getByTestId('potg-sponsor')).toHaveCount(0);
  await expect(page.locator('.potg-band, .potg-sponsor')).toHaveCount(0);

  await page.goto('/qa/play-of-the-game?state=unavailable&chrome=0');
  await expect(page.getByTestId('potg-sponsor')).toHaveAttribute(
    'data-unavailable-reason',
    'absent',
  );
  await expect(page.getByTestId('potg-sponsor')).toContainText('Sponsored');

  // The sponsor slot fills the same space, so it opens the same way — inside
  // the slot, which is what pushes the rest of the card down as it grows.
  await expect(page.getByTestId('potg-slot')).toHaveCount(1);
  await expect(page.getByTestId('potg-slot').locator('.potg-sponsor')).toHaveCount(1);

  // ...but it keeps its own quiet border rather than the replay's recess.
  const inset = await page.getByTestId('potg-sponsor').evaluate((el) => (
    getComputedStyle(el, '::after').boxShadow
  ));
  expect(inset === 'none' || inset === '').toBe(true);

  await page.goto('/qa/play-of-the-game?state=incompatible&chrome=0');
  await expect(page.getByTestId('potg-sponsor')).toHaveAttribute(
    'data-unavailable-reason',
    'incompatible',
  );
});

for (const [label, state] of [['replay', 'ready'], ['sponsor slot', 'unavailable']]) {
  test(`the ${label} opens by pushing the card, not by appearing at full height`, async ({ page }) => {
    await page.goto(`/qa/play-of-the-game?state=${state}&chrome=0`);
    await page.getByTestId('game-over-card').waitFor();

    const frames = await page.evaluate(() => new Promise((resolve) => {
      const samples = [];
      const startedAt = performance.now();
      const tick = () => {
        const slot = document.querySelector('.potg-slot');
        const statline = document.querySelector('.game-over-statline');
        if (slot && statline) {
          samples.push({
            slot: slot.getBoundingClientRect().height,
            statTop: statline.getBoundingClientRect().top,
          });
        }
        if (performance.now() - startedAt < 900) requestAnimationFrame(tick);
        else resolve(samples);
      };
      requestAnimationFrame(tick);
    }));

    expect(frames.length).toBeGreaterThan(10);
    const heights = frames.map((frame) => frame.slot);
    const settled = heights[heights.length - 1];

    // It opens from nothing, and every frame is at least as tall as the last:
    // the card is displaced continuously rather than in one step.
    expect(heights[0]).toBeLessThan(settled * 0.9);
    expect(heights.every((h, i) => i === 0 || h >= heights[i - 1] - 0.5)).toBe(true);

    // What sits below it moves with it, which is the whole point.
    const statTops = frames.map((frame) => frame.statTop);
    expect(statTops[statTops.length - 1]).toBeGreaterThan(statTops[0] + 20);
  });
}

for (const malformedState of ['malformed-anchor', 'bad-end-hash']) {
  test(`${malformedState} collapses to the branded poster with replay hidden`, async ({ page }) => {
    await page.goto(`/qa/play-of-the-game?state=${malformedState}&chrome=0`);

    const poster = page.getByTestId('potg-render-fallback');
    await expect(poster).toBeVisible();
    await expect(poster).toContainText('BANKER');
    await expect(poster).toContainText('Replay renderer unavailable');
    await expect(page.getByTestId('potg-replay')).toHaveCount(0);
    await expect(page.getByTestId('scenario-canvas')).toHaveCount(0);
  });
}
