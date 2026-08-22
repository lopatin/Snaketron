// Capture the skin contact sheet for review and for a PR.
//
// Usage, from client/web with the dev server running:
//   node tests/capture-skin-sheet.mjs http://localhost:3000 classic@1 ../../docs/screenshots/skins/classic
//
// Reduced motion is forced and every animation tile renders at a pinned clock,
// so two runs of the same skin produce identical images. That is what makes the
// output worth diffing rather than just glancing at.
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, skinRef, outDir] = process.argv.slice(2);
if (!baseUrl || !skinRef || !outDir) {
  throw new Error(
    'usage: node capture-skin-sheet.mjs <base-url> <skin-ref> <out-dir>',
  );
}
mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1280, height: 1400 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
const page = await context.newPage();

page.on('console', (message) => {
  if (message.type() === 'error') {
    console.error(`[page] ${message.text()}`);
  }
});

await page.goto(`${baseUrl}/qa/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="skins-qa"]');

const selector = `[data-testid="skin-select-${skinRef}"]`;
if ((await page.locator(selector).count()) === 0) {
  throw new Error(
    `no skin named ${skinRef} in the catalogue — check the id against skinCatalog()`,
  );
}
await page.click(selector);
// One frame for the tiles to repaint against the newly selected skin.
await page.waitForTimeout(150);

// A textured skin fetches its pixels on its first paint, so a sheet captured
// straight after selecting one would show every tile wearing the flat coat
// underneath. The tiles repaint themselves once this goes false; wait for that
// first, or the screenshot is of the fallback.
await page.waitForFunction(() => window.wasm?.skinAssetsPending() === false, {
  timeout: 10_000,
});
await page.waitForTimeout(150);

const assetStatus = await page.evaluate(() => {
  const raw = window.wasm?.skinAssetsStatus?.();
  return raw ? JSON.parse(raw) : null;
});
if (!assetStatus) {
  throw new Error('renderer does not expose skin asset evidence');
}
if (assetStatus.failed > 0) {
  throw new Error(
    `${assetStatus.failed} of ${assetStatus.requested} requested skin images failed to decode`,
  );
}
if (assetStatus.requested > 0 && assetStatus.drawnImages === 0) {
  throw new Error(
    'skin images decoded but no image pixels reached a canvas; refusing to capture the fallback',
  );
}

const slug = skinRef.replace(/[^a-z0-9]+/gi, '-');
await page.screenshot({
  path: join(outDir, `${slug}-sheet.png`),
  fullPage: true,
});

for (const section of ['roles', 'poses', 'states', 'anim', 'swatches']) {
  const heading = page.locator(`h2#skins-qa-${section}`);
  if ((await heading.count()) === 0) {
    continue;
  }
  const region = heading.locator('xpath=..');
  await region.screenshot({ path: join(outDir, `${slug}-${section}.png`) });
}

console.log(`captured ${skinRef} to ${outDir}`);
await browser.close();
