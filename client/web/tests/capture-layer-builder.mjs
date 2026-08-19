// Photograph the layers panel, screen by screen.
//
// Usage, from client/web with the dev server and a mock API running:
//   node tests/capture-layer-builder.mjs http://localhost:3100 \
//     ../../docs/screenshots/layer-documents
//
// Reduced motion is forced so two runs of the same shot produce the same
// image — except where the shot is *about* motion, which is called out below.
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, outDir] = process.argv.slice(2);
if (!baseUrl || !outDir) {
  throw new Error('usage: node capture-layer-builder.mjs <base-url> <out-dir>');
}
mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1440, height: 1000 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
// The app reads its bearer token from localStorage before it calls
// /api/auth/me, so a stored token plus a mock that answers is a real session.
await context.addInitScript(() => {
  window.localStorage.setItem('token', 'capture-session');
});

const page = await context.newPage();
page.on('console', (message) => {
  if (message.type() === 'error') {
    console.error(`[page] ${message.text()}`);
  }
});

const settle = async (ms = 700) => {
  await page
    .waitForFunction(() => !window.wasm?.skinAssetsPending?.(), null, { timeout: 10_000 })
    .catch(() => {});
  await page.waitForTimeout(ms);
};

const shot = async (name, options = {}) => {
  await settle();
  await page.screenshot({ path: join(outDir, `${name}.png`), ...options });
  console.log(`wrote ${name}.png`);
};

const clip = async (name, selector, pad = 16) => {
  const target = page.locator(selector).first();
  await target.scrollIntoViewIfNeeded();
  await settle();
  const box = await target.boundingBox();
  if (!box) {
    throw new Error(`nothing to photograph at ${selector}`);
  }
  await page.screenshot({
    path: join(outDir, `${name}.png`),
    clip: {
      x: Math.max(0, box.x - pad),
      y: Math.max(0, box.y - pad),
      width: box.width + pad * 2,
      height: box.height + pad * 2,
    },
  });
  console.log(`wrote ${name}.png`);
};

const pickTemplate = async (label) => {
  await page.getByRole('button', { name: new RegExp(label, 'i') }).first().click();
  await settle();
};

const selectLayer = async (name) => {
  await page.locator('.builder-layer-name', { hasText: name }).first().click();
  await settle(400);
};

await page.goto(`${baseUrl}/skins/builder`);
await settle(1200);
await shot('01-templates');

await pickTemplate('Shine');
await shot('02-panel');
await clip('03-stack', '.builder-stack');

// The shine's stops: the whole thesis in one control. A position that reads
// the clock is what makes a gleam travel, and there is no Animations section
// anywhere on the page.
await selectLayer('Shine');
await clip('04-inspector-gradient', '.builder-inspector');

// An expression field in each of its two states, on the same value.
await selectLayer('Body');
await clip('05-inspector-slider', '.builder-inspector');
await page.locator('.builder-fx').first().click();
await settle(400);
await page.locator('.builder-expression-input').first().fill('0.4 + 0.6 * sin(tau * time)');
await settle(900);
await clip('06-inspector-expression', '.builder-inspector');

// What a bad expression says, and where.
await page.locator('.builder-expression-input').first().fill('s / len');
await settle(900);
await clip('07-site-error', '.builder-problems');
await page.locator('.builder-expression-input').first().fill('0.4 + 0.6 * sin(tau * time)');
await settle(700);

// The cost meter, beside the save it gates.
await clip('08-cost', '.builder-actions');

// A word worn along the body.
await pickTemplate('Lit').catch(() => {});
await page.goto(`${baseUrl}/skins/builder`);
await settle(1200);
await pickTemplate('Lit');
await page.getByRole('button', { name: /Painted stretch/i }).click();
await settle(500);
const sourceSelect = page
  .locator('.builder-inspector select')
  .filter({ has: page.locator('option[value="text"]') })
  .first();
await sourceSelect.selectOption('text');
await settle(1200);
await clip('09-text-layer', '.builder-preview-strip');
await clip('10-text-inspector', '.builder-inspector');

// The document panel: what belongs to the skin rather than to a layer.
await page.locator('.builder-document summary').click();
await settle(500);
await clip('11-document', '.builder-document');

await browser.close();
console.log('done');
