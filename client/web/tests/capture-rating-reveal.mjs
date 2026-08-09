// One-shot capture of the rating-reveal QA scenarios (docs/screenshots).
// Usage, from client/web with the dev server running:
//   node tests/capture-rating-reveal.mjs http://localhost:3100 ../../docs/screenshots/rating-reveal
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, outDir] = process.argv.slice(2);
if (!baseUrl || !outDir) {
  throw new Error('usage: node capture-rating-reveal.mjs <base-url> <out-dir>');
}
mkdirSync(outDir, { recursive: true });

const SCENARIOS = [
  'promotion',
  'gain',
  'demotion',
  'draw',
  'placement',
  'quickmatch',
  'pending',
  'none',
];

const browser = await chromium.launch();

const captureAll = async (viewport, suffix) => {
  const context = await browser.newContext({
    viewport,
    reducedMotion: 'reduce',
    deviceScaleFactor: 2,
  });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/qa/rating-reveal`, { waitUntil: 'networkidle' });
  for (const id of SCENARIOS) {
    await page.click(`[data-testid="qa-scenario-${id}"]`);
    await page.waitForTimeout(400);
    await page
      .locator('[data-testid="game-over-card"]')
      .screenshot({ path: join(outDir, `${id}${suffix}.png`) });
    console.log(`captured ${id}${suffix}`);
  }
  await context.close();
};

await captureAll({ width: 1000, height: 800 }, '');
// One mobile shot of the headline scenario to show the band's compact layout.
const mobileContext = await browser.newContext({
  viewport: { width: 375, height: 812 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
const mobilePage = await mobileContext.newPage();
await mobilePage.goto(`${baseUrl}/qa/rating-reveal`, { waitUntil: 'networkidle' });
await mobilePage.click('[data-testid="qa-scenario-promotion"]');
await mobilePage.waitForTimeout(400);
await mobilePage
  .locator('[data-testid="game-over-card"]')
  .screenshot({ path: join(outDir, 'promotion-mobile.png') });
console.log('captured promotion-mobile');
await mobileContext.close();

await browser.close();
