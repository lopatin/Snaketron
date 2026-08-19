// Capture the Skins page for review and for a PR.
//
// Usage, from client/web with the dev server running:
//   node tests/capture-skins-page.mjs http://localhost:3000 ../../docs/screenshots/first-class-skins
//
// Reduced motion is forced so every preview paints its pinned frame: two runs
// produce identical images, which is what makes the output worth diffing rather
// than just glancing at.
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, outDir] = process.argv.slice(2);
if (!baseUrl || !outDir) {
  throw new Error('usage: node capture-skins-page.mjs <base-url> <out-dir>');
}
mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1400, height: 1100 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
const page = await context.newPage();

page.on('console', (message) => {
  if (message.type() === 'error') {
    console.error(`[page] ${message.text()}`);
  }
});

await page.goto(`${baseUrl}/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="snake-skin-list"] li');
await page.waitForSelector('[data-testid="base-skin-list"] li');

// Textured skins fetch their pixels on first paint, so a page captured before
// they settle shows the flat fallback coat — which is exactly the quiet wrong
// answer a screenshot is supposed to avoid producing.
await page.waitForFunction(() => !window.wasm?.skinAssetsPending?.(), null, {
  timeout: 10_000,
});
await page.waitForTimeout(250);

await page.screenshot({
  path: join(outDir, 'skins-page.png'),
  fullPage: false,
});
console.log('wrote skins-page.png');

await page.screenshot({
  path: join(outDir, 'skins-page-full.png'),
  fullPage: true,
});
console.log('wrote skins-page-full.png');

// The Builder, opened on a new skin. Its previews animate, so reduced motion
// (set on the context) is what makes this frame reproducible.
await page.goto(`${baseUrl}/skins/builder`, { waitUntil: 'networkidle' });
await page.waitForSelector('.builder-preview canvas');
await page.waitForTimeout(400);
await page.screenshot({
  path: join(outDir, 'skin-builder.png'),
  fullPage: false,
});
console.log('wrote skin-builder.png');

await browser.close();
