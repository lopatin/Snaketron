// Capture the base-skin surfaces: the picker, its home/away switch, one base's
// dialog, and a live 2v2 arena with a different base on each end.
//
//   node tests/capture-base-skins.mjs [--out DIR] [--url http://localhost:3000]
//
// Needs a dev server and something answering the skins API — see
// `docs/screenshots/base-skins/README.md` for the whole recipe, including the
// wasm rebuild that is easy to forget and looks like a broken feature. Modelled on capture-rating-reveal.mjs:
// reduced motion is stubbed and the device scale factor is pinned so a rerun
// produces comparable pixels.
//
// Every shot waits on `skinAssetsPending()` going false rather than on a timer.
// A base skin is a picture fetched at first paint, so a capture that does not
// wait photographs the fallback tint and looks exactly like a broken feature.

import { chromium } from '@playwright/test';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const args = process.argv.slice(2);
const readArg = (name, fallback) => {
  const index = args.indexOf(name);
  return index === -1 ? fallback : args[index + 1];
};

const BASE_URL = readArg('--url', 'http://localhost:3000');
// Resolved against this file, not the shell's cwd: the default output lives in
// the repository, and `node tests/capture-base-skins.mjs` should put it there
// whether it was run from client/web or from the root.
const OUT_DIR = path.resolve(
  readArg('--out', new URL('../../../docs/screenshots/base-skins', import.meta.url).pathname),
);

const settleAssets = (page) =>
  page.waitForFunction(
    () => {
      const wasm = window.__wasm ?? window.wasm;
      return Boolean(wasm) && !wasm.skinAssetsPending();
    },
    null,
    { timeout: 20_000 },
  );

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  // No `channel: 'chrome'`: the bundled Chromium is what every other
  // capture script uses and what `npx playwright install` provides.
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
    deviceScaleFactor: 2,
    reducedMotion: 'reduce',
  });
  const page = await context.newPage();

  const shoot = async (name, target) => {
    const file = path.join(OUT_DIR, `${name}.png`);
    await (target ?? page).screenshot({ path: file });
    console.log(file);
  };

  await page.goto(`${BASE_URL}/skins`, { waitUntil: 'networkidle' });
  await page.waitForSelector('[data-testid="base-skin-list"] li');
  await settleAssets(page);
  // One more frame after the assets land: the previews repaint on that promise,
  // and screenshotting in the same tick catches the paint before it.
  await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));

  await shoot('skins-page-home');

  await page.click('[data-testid="base-facing-away"]');
  await page.click('[data-testid="snake-facing-away"]');
  await settleAssets(page);
  await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));
  await shoot('skins-page-away');

  // Back to home, then open one base's dialog.
  await page.click('[data-testid="base-facing-home"]');
  await page.click('[data-testid="skin-open-invaders@1"]');
  await page.waitForSelector('[data-testid="skin-modal"]');
  await settleAssets(page);
  await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r))));
  await shoot('base-modal', page.locator('[data-testid="skin-modal"]'));

  await page.click('[data-testid="skin-modal"] .shop-close');

  // A live arena, which is the surface the whole feature exists for. Built
  // from a committed team scenario so the geometry is the real one, with each
  // team's base injected the way match preparation would have.
  // Read from disk rather than fetched: scenarios are webpack modules, not
  // static assets, so there is no URL to ask for.
  const scenario = JSON.parse(
    await readFile(new URL('../scenarios/team-bank.json', import.meta.url), 'utf8'),
  );
  for (const [rotation, name] of [
    [0, 'arena-rotation-0'],
    [90, 'arena-rotation-90'],
  ]) {
    await page.evaluate(
      async ([script, rotationDegrees]) => {
        const wasm = window.__wasm ?? window.wasm;
        const player = new wasm.ScenarioPlayer(JSON.stringify(script));
        player.seek(0);
        const state = JSON.parse(player.currentStateJson());
        state.team_bases = { 0: 'invaders@1', 1: 'dragon@1' };
        const client = wasm.GameClient.newFromState(1, JSON.stringify(state));

        let canvas = document.getElementById('capture-arena');
        if (!canvas) {
          canvas = document.createElement('canvas');
          canvas.id = 'capture-arena';
          Object.assign(canvas.style, {
            position: 'fixed',
            inset: '0',
            zIndex: '9999',
            background: '#fff',
          });
          document.body.append(canvas);
        }
        const cell = 18;
        const wide = rotationDegrees === 90 || rotationDegrees === 270;
        canvas.width = (wide ? 40 : 60) * cell;
        canvas.height = (wide ? 60 : 40) * cell;
        const noop = () => {};
        const paint = () =>
          client.render(canvas, cell, rotationDegrees, 1, 0, true, null, noop, noop);
        paint();
        // The pictures are requested by that first paint; wait for them and
        // paint once more, or the shot is of the fallback tint.
        const deadline = Date.now() + 15000;
        while (wasm.skinAssetsPending() && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        paint();
      },
      [scenario, rotation],
    );
    await shoot(name, page.locator('#capture-arena'));
  }

  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
