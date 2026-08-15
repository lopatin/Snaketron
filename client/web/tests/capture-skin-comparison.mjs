// Capture one image showing every shipped skin against the same poses.
//
// The per-skin sheets from `capture-skin-sheet.mjs` answer "does this skin look
// right?". They cannot answer "are these actually different skins?", because
// comparing them means opening six files and remembering the last one. This
// composes the same three tiles from each skin into a single labelled grid, so
// a reviewer sees the catalogue at once.
//
// Usage, from client/web with a dev server running:
//   node tests/capture-skin-comparison.mjs http://localhost:3100 ../../docs/screenshots/skins/all-skins.png
//
// Reduced motion is forced and the tiles are fixed-clock samples, so two runs
// produce identical bytes. That is what makes the output worth committing.
import { chromium } from '@playwright/test';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

const [baseUrl, outFile] = process.argv.slice(2);
if (!baseUrl || !outFile) {
  throw new Error('usage: node capture-skin-comparison.mjs <base-url> <out-file>');
}

/** Mirrors `server/src/skin_catalog.rs`. */
const SKINS = [
  'classic@1',
  'ember@1',
  'aurora@1',
  'tidewave@1',
  'voltage@1',
  'lantern@1',
  'gambit@1',
  'harlequin@1',
  'pitlane@1',
];

/** Three tiles that between them show palette, corner handling and the Boost band. */
const TILES = [
  'fixture-pose-longer_than_head_gradient',
  'fixture-pose-zigzag',
  'fixture-state-boost',
];

const LABEL_WIDTH = 132;
const PAD = 12;

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1600, height: 1400 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
const page = await context.newPage();
await page.goto(`${baseUrl}/qa/skins`, { waitUntil: 'networkidle' });
await page.waitForSelector('[data-testid="skins-qa"]');

// Each step is self-contained: collect the tiles as data URLs, then compose
// once at the end. Nothing is stashed on `window` between steps, because the
// dev server's hot reload will happily wipe it mid-run and the failure looks
// like a selector bug rather than a reload.
const rows = [];
for (const skin of SKINS) {
  const selector = `[data-testid="skin-select-${skin}"]`;
  if ((await page.locator(selector).count()) === 0) {
    throw new Error(`no skin named ${skin} in the catalogue`);
  }
  await page.click(selector);
  // One frame for every tile to repaint against the newly selected skin.
  await page.waitForTimeout(150);

  const cells = await page.evaluate((tiles) => {
    return tiles.map((tile) => {
      const canvas = document.querySelector(`[data-testid="${tile}"] canvas`);
      if (!canvas) {
        throw new Error(`no tile ${tile} on the page`);
      }
      return {
        png: canvas.toDataURL('image/png'),
        width: canvas.width,
        height: canvas.height,
      };
    });
  }, TILES);
  rows.push({ skin, cells });
}

const composed = await page.evaluate(
  async ([rows, labelWidth, pad]) => {
    const columns = rows[0].cells.map((cell) => cell.width);
    const rowHeight =
      Math.max(...rows.flatMap((row) => row.cells.map((cell) => cell.height))) +
      pad;

    const composite = document.createElement('canvas');
    composite.id = 'skin-comparison';
    composite.width =
      labelWidth + columns.reduce((wide, w) => wide + w + pad, 0) + pad;
    composite.height = rowHeight * rows.length + pad;
    Object.assign(composite.style, {
      position: 'fixed',
      top: '0',
      left: '0',
      zIndex: '9999',
    });
    document.body.appendChild(composite);

    const context = composite.getContext('2d');
    context.fillStyle = '#ffffff';
    context.fillRect(0, 0, composite.width, composite.height);

    for (const [index, row] of rows.entries()) {
      const y = pad + index * rowHeight;
      context.fillStyle = '#111111';
      context.font =
        '600 15px ui-sans-serif, system-ui, -apple-system, sans-serif';
      context.textBaseline = 'middle';
      context.fillText(row.skin, pad, y + rowHeight / 2 - pad / 2);

      let x = labelWidth;
      for (const [column, cell] of row.cells.entries()) {
        const image = new Image();
        await new Promise((resolve, reject) => {
          image.onload = resolve;
          image.onerror = () => reject(new Error('tile failed to decode'));
          image.src = cell.png;
        });
        context.drawImage(image, x, y);
        x += columns[column] + pad;
      }
    }
    return { width: composite.width, height: composite.height };
  },
  [rows, LABEL_WIDTH, PAD],
);

mkdirSync(dirname(outFile), { recursive: true });
await page.locator('#skin-comparison').screenshot({ path: outFile });
await browser.close();
console.log(
  `composed ${rows.length} skins x ${TILES.length} tiles ` +
    `(${composed.width}x${composed.height}) -> ${outFile}`,
);
