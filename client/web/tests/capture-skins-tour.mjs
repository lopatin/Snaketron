// Photograph the whole skins experience, screen by screen and state by state.
//
// Usage, from client/web with the dev server and a mock API running:
//   node tests/capture-skins-tour.mjs http://localhost:3100 http://localhost:8477 \
//     ../../docs/screenshots/first-class-skins
//
// Reduced motion is forced and textured skins are waited for, so two runs of
// the same shot produce the same image. Signing in is a stored token plus a
// mock that answers /api/auth/me — the app's own auth path, not a stub inside
// the component.
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, apiUrl, outDir] = process.argv.slice(2);
if (!baseUrl || !apiUrl || !outDir) {
  throw new Error('usage: node capture-skins-tour.mjs <base-url> <api-url> <out-dir>');
}
mkdirSync(outDir, { recursive: true });

const setState = async (patch) => {
  const response = await fetch(`${apiUrl}/__test/state`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
  });
  if (!response.ok) {
    throw new Error(`could not set mock state: ${response.status}`);
  }
};

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

const settle = async () => {
  await page.waitForFunction(() => !window.wasm?.skinAssetsPending?.(), null, {
    timeout: 10_000,
  }).catch(() => {});
  await page.waitForTimeout(300);
};

const shot = async (name, options = {}) => {
  await settle();
  await page.screenshot({ path: join(outDir, `${name}.png`), ...options });
  console.log(`wrote ${name}.png`);
};

// A close-up of one element, with a little room around it. The element is
// scrolled into view first: a clip rectangle outside the viewport is not a
// crop, it is an error.
const clip = async (name, selector, pad = 16) => {
  const target = page.locator(selector).first();
  await target.scrollIntoViewIfNeeded();
  await settle();
  const box = await target.boundingBox();
  if (!box) {
    throw new Error(`no element for ${selector}`);
  }
  const viewport = page.viewportSize();
  await page.screenshot({
    path: join(outDir, `${name}.png`),
    clip: {
      x: Math.max(0, box.x - pad),
      y: Math.max(0, box.y - pad),
      width: Math.min(box.width + pad * 2, viewport.width - Math.max(0, box.x - pad)),
      height: Math.min(box.height + pad * 2, viewport.height - Math.max(0, box.y - pad)),
    },
  });
  console.log(`wrote ${name}.png`);
};

// ---------------------------------------------------------------------------
// 1. Browsing, signed out. The shop window a visitor lands on.
// ---------------------------------------------------------------------------
await setState({ signedIn: false });
await page.goto(`${baseUrl}/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="snake-skin-list"] li');
await shot('01-browse-signed-out');

// ---------------------------------------------------------------------------
// 2. Browsing, signed in: equip becomes possible and one skin is already worn.
// ---------------------------------------------------------------------------
await setState({ signedIn: true, selectedSkin: 'pitlane@1', owned: [1002] });
await page.goto(`${baseUrl}/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="snake-skin-list"] li');
await shot('02-browse-signed-in');

// The equipped row, close up: the state marker rather than a button.
await clip('03-equipped-row', '.skins-row.is-equipped');

// A priced player-made skin beside a free one.
await clip('04-authored-rows', '[data-testid="snake-skin-list"] li:nth-child(-n+2)', 8);

// ---------------------------------------------------------------------------
// 3. Equipping. The row that was offering an action now shows the state.
// ---------------------------------------------------------------------------
await page.locator('[data-testid="skin-equip-snake-aurora@1"]').click();
await page.waitForSelector('[data-testid="skin-row-snake-aurora@1"].is-equipped');
await shot('05-after-equipping');

// ---------------------------------------------------------------------------
// 4. The base rail: two halves of the arena, and its own equip slot.
// ---------------------------------------------------------------------------
await page.locator('[data-testid="skin-equip-base-ember@1"]').click();
await page.waitForTimeout(400);
await clip('06-base-rail', '.skins-column-bases', 12);

// ---------------------------------------------------------------------------
// 5. Buying. A priced skin, bought with Snakebux, ends up worn.
// ---------------------------------------------------------------------------
await setState({ signedIn: true, selectedSkin: 'classic@1', balanceBux: 900 });
await page.goto(`${baseUrl}/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="skin-buy-skin:1001"]');
await clip('07-buy-action', '[data-testid="skin-row-snake-skin:1001"]', 10);
await page.locator('[data-testid="skin-buy-skin:1001"]').click();
await page.waitForTimeout(600);
await shot('08-after-buying');

// ---------------------------------------------------------------------------
// 6. The Builder, on a new skin. The builder is an admin surface now, so the
//    session is promoted before entering it.
// ---------------------------------------------------------------------------
await setState({ signedIn: true, isAdmin: true });
await page.goto(`${baseUrl}/skins/builder`, { waitUntil: 'networkidle' });
await page.waitForSelector('.builder-preview canvas');
await shot('09-builder-new');

// The preview strip on its own: the same skin as you, as an opponent, turning.
await clip('10-builder-previews', '.builder-preview-strip', 10);

// Colour controls, generated from the schema rather than hand-written.
await clip('11-builder-colours', '.builder-form > .builder-group:nth-of-type(1)', 10);

// ---------------------------------------------------------------------------
// 7. Editing: a changed colour repaints all three previews.
// ---------------------------------------------------------------------------
await page.evaluate(() => {
  const input = document.querySelector('[data-testid="builder-palette.friendly.0.fill"]');
  const setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype,
    'value',
  ).set;
  setter.call(input, '#a855f7');
  input.dispatchEvent(new Event('input', { bubbles: true }));
});
await page.waitForTimeout(500);
await clip('12-builder-edited', '.builder-preview-strip', 10);

// ---------------------------------------------------------------------------
// 8. The validator refusing an edit, in the editor, before any save.
// ---------------------------------------------------------------------------
await page.evaluate(() => {
  const input = document.querySelector('[data-testid="builder-palette.friendly.0.fill"]');
  const setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype,
    'value',
  ).set;
  // Enemy red on a friendly role: the hue windows exist to stop exactly this.
  setter.call(input, '#ff2b2b');
  input.dispatchEvent(new Event('input', { bubbles: true }));
});
await page.waitForSelector('.builder-problems');
await shot('13-builder-invalid');

// ---------------------------------------------------------------------------
// 9. Animation: an optional section switched on, with its tracks.
// ---------------------------------------------------------------------------
await page.goto(`${baseUrl}/skins/builder`, { waitUntil: 'networkidle' });
await page.waitForSelector('.builder-preview canvas');
await page.locator('[data-testid="builder-toggle-animation"]').check();
await page.waitForTimeout(200);
await page.locator('[data-testid="builder-add-animation.tracks"]').click();
await page.waitForTimeout(300);
await clip('14-builder-animation', '.builder-group-optional:has([data-testid="builder-toggle-animation"])', 10);

// ---------------------------------------------------------------------------
// 10. Saving, then asking for review.
// ---------------------------------------------------------------------------
await page.goto(`${baseUrl}/skins/builder`, { waitUntil: 'networkidle' });
await page.waitForSelector('.builder-preview canvas');
await page.getByRole('button', { name: 'Save skin' }).click();
await page.waitForSelector('.builder-status');
// The whole header, not just the buttons: the confirmation and the fact that
// "send for review" has appeared are the point of the shot.
await page.evaluate(() => window.scrollTo(0, 0));
await clip('15-builder-saved', '.builder-main', 0);

// ---------------------------------------------------------------------------
// 11. The review queue an admin decides from.
// ---------------------------------------------------------------------------
await setState({ signedIn: true, isAdmin: true });
await page.goto(`${baseUrl}/admin`, { waitUntil: 'networkidle' });
await page.getByRole('link', { name: 'Skins' }).or(page.getByRole('button', { name: 'Skins' })).first().click();
await page.waitForSelector('.admin-skin-queue li', { timeout: 8000 });
await shot('16-admin-review-queue');
await clip('17-admin-queue-row', '.admin-skin-queue', 12);

// ---------------------------------------------------------------------------
// 12. Narrow viewport: the two columns stack.
// ---------------------------------------------------------------------------
await setState({ signedIn: true, selectedSkin: 'aurora@1' });
await page.setViewportSize({ width: 430, height: 1100 });
await page.goto(`${baseUrl}/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="snake-skin-list"] li');
await shot('18-browse-narrow');

await browser.close();
