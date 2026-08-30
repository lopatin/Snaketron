// Render one v2 skin document through the real WASM renderer and photograph it.
//
// Usage, from client/web with the dev server running:
//   node tests/capture-draft-skin.mjs http://localhost:3000 \
//     ../../skin-schema/skins/breaker.skin.json ../../docs/screenshots/skins/breaker
//
// The document is registered as a *draft* — the same door the Builder's live
// preview uses — so nothing has to be added to the shipped catalogue to look
// at it. Reduced motion is forced and every tile paints at a pinned clock, so
// two runs produce the same pixels.
import { chromium } from '@playwright/test';
import { mkdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const [baseUrl, skinPath, outDir] = process.argv.slice(2);
if (!baseUrl || !skinPath || !outDir) {
  throw new Error('usage: capture-draft-skin.mjs <base-url> <skin.json> <out-dir>');
}
mkdirSync(outDir, { recursive: true });

const documentJson = readFileSync(skinPath, 'utf8');
const periodMs = Number(JSON.parse(documentJson).period_ms || 2000);
const HANDLE = 'draft:review';

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1500, height: 1100 },
  reducedMotion: 'reduce',
  deviceScaleFactor: 2,
});
const page = await context.newPage();
const consoleErrors = [];
page.on('console', (m) => {
  if (m.type() === 'error') consoleErrors.push(m.text());
});

await page.goto(`${baseUrl.replace(/\/$/, '')}/qa/skins`, { waitUntil: 'networkidle' });
await page.waitForFunction(() => window.wasm?.registerDraftSkin !== undefined);

const painted = await page.evaluate(
  ({ handle, documentJson, periodMs }) => {
    window.wasm.registerDraftSkin(handle, documentJson);
    const fixtures = JSON.parse(window.wasm.skinFixtures());

    const CELL = 15;
    const poseByName = Object.fromEntries(fixtures.poses.map((p) => [p.name, p]));
    // `skinFixtures()` publishes the conformance corpus only. The preview-only
    // bodies are renderable by name but not listed, so the framing the Builder
    // and the geometry guide use is supplied here rather than guessed at.
    Object.assign(poseByName, {
      prototype_straight_16: { name: 'prototype_straight_16', cellsWide: 18, cellsHigh: 3 },
      straight_16: { name: 'straight_16', cellsWide: 16, cellsHigh: 1 },
    });

    const root = document.createElement('main');
    root.id = 'breaker-evidence';
    root.style.cssText =
      'background:#12161c;color:#e8eef4;font:13px/1.4 system-ui,sans-serif;padding:20px;';
    document.body.replaceChildren(root);

    const tiles = [];
    const section = (id, title, note) => {
      const s = document.createElement('section');
      s.id = id;
      s.style.cssText = 'padding:16px;background:#12161c;';
      const h = document.createElement('h2');
      h.textContent = title;
      h.style.cssText = 'margin:0 0 2px;font:600 16px system-ui;color:#fff';
      const p = document.createElement('p');
      p.textContent = note;
      p.style.cssText = 'margin:0 0 12px;color:#93a3b5;font-size:12px';
      const grid = document.createElement('div');
      grid.style.cssText =
        'display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end;';
      s.append(h, p, grid);
      root.append(s);
      return grid;
    };

    const add = (grid, opts) => {
      const {
        pose,
        role = 'own',
        label,
        cellSize = CELL,
        boost = false,
        dead = false,
        animMs = 0,
        scale = 1,
      } = opts;
      const fixture = poseByName[pose];
      if (!fixture) throw new Error(`unknown pose ${pose}`);
      const w = (fixture.cellsWide + 2) * cellSize;
      const h = (fixture.cellsHigh + 2) * cellSize;
      const figure = document.createElement('figure');
      figure.style.cssText = 'margin:0;display:flex;flex-direction:column;gap:4px';
      const canvas = document.createElement('canvas');
      canvas.width = w;
      canvas.height = h;
      canvas.style.cssText = `width:${w * scale}px;height:${h * scale}px;image-rendering:pixelated;background:#fff;border-radius:4px`;
      const cap = document.createElement('figcaption');
      cap.textContent = label;
      cap.style.cssText = 'color:#93a3b5;font-size:11px';
      figure.append(canvas, cap);
      grid.append(figure);
      tiles.push({ canvas, pose, role, cellSize, boost, dead, animMs });
    };

    // 1. The prototype-geometry pose, the same body the reference strip uses.
    const g0 = section(
      'shot-reference',
      'Reference pose',
      'The repository prototype-geometry body (16 cells, 15px/cell), enlarged 3x — directly comparable with the reference strip.',
    );
    add(g0, { pose: 'prototype_straight_16', label: 'own · resting', scale: 3 });
    add(g0, {
      pose: 'prototype_straight_16',
      role: 'enemy',
      label: 'enemy · resting',
      scale: 3,
    });

    // 2. Every role.
    const g1 = section(
      'shot-roles',
      'Roles',
      'The same 21-cell body in every palette slot the renderer resolves.',
    );
    for (const role of fixtures.roles) add(g1, { pose: 'longer_than_head_gradient', role, label: role });

    // 3. Bodies and turns.
    const g2 = section(
      'shot-poses',
      'Bodies and turns',
      'Spawn length, growth, corners, U-turns and a tiling-length body.',
    );
    for (const [pose, label] of [
      ['single_cell', 'one cell'],
      ['two_cell', 'two cells'],
      ['starting_length', 'spawn (4 cells)'],
      ['straight_horizontal', 'straight, 6 cells'],
      ['straight_vertical', 'straight, vertical'],
      ['single_corner', 'one corner'],
      ['reversed_travel', 'reversed travel'],
      ['zigzag', 'zigzag'],
      ['one_cell_runs', 'one-cell runs'],
      ['self_crossing', 'self crossing'],
      ['wide_u_turn', 'wide U-turn'],
      ['tile_wrapping_length', 'long body (33 cells)'],
    ])
      add(g2, { pose, label });

    // 4. Scale and state.
    const g3 = section(
      'shot-scale-state',
      'Scale and state',
      'The arena picks integer cell sizes from 5 to 15 px. Boost adds the system band; death is the corpse pass.',
    );
    for (const size of fixtures.cellSizes)
      add(g3, {
        pose: 'starting_length',
        cellSize: size,
        label: `${size}px/cell`,
        scale: 15 / size,
      });
    for (const size of fixtures.cellSizes)
      add(g3, {
        pose: 'longer_than_head_gradient',
        cellSize: size,
        label: `long · ${size}px/cell`,
        scale: 15 / size,
      });
    add(g3, { pose: 'longer_than_head_gradient', boost: true, label: 'boosting' });
    add(g3, {
      pose: 'longer_than_head_gradient',
      role: 'enemy',
      boost: true,
      label: 'enemy boosting',
    });
    add(g3, { pose: 'zigzag', dead: true, label: 'dead, turning' });

    // 5. One full animation cycle.
    const g4 = section(
      'shot-film-strip',
      'One cycle',
      `Eight fixed samples across the ${periodMs}ms period. Sample 0 is the resting/reduced-motion frame.`,
    );
    for (let i = 0; i < 8; i += 1)
      add(g4, {
        pose: 'longer_than_head_gradient',
        animMs: (i / 8) * periodMs,
        label: `t = ${Math.round((i / 8) * periodMs)}ms`,
      });

    window.__paint = () => {
      for (const t of tiles) {
        window.wasm.renderSkinFixture(
          t.canvas,
          handle,
          t.pose,
          t.role,
          t.cellSize,
          t.boost,
          t.dead,
          t.animMs,
          false,
        );
      }
      return tiles.length;
    };
    return window.__paint();
  },
  { handle: HANDLE, documentJson, periodMs },
);

// Paint once more after a frame, so anything that had to settle has.
await page.evaluate(() => window.__paint());
await page.waitForTimeout(250);
await page.evaluate(() => window.__paint());

const shots = [
  ['shot-reference', 'reference-pose.png'],
  ['shot-roles', 'roles.png'],
  ['shot-poses', 'poses.png'],
  ['shot-scale-state', 'scale-and-state.png'],
  ['shot-film-strip', 'film-strip.png'],
];
for (const [id, file] of shots) {
  await page.locator(`#${id}`).screenshot({ path: join(outDir, file) });
}
await page.locator('#breaker-evidence').screenshot({
  path: join(outDir, 'contact-sheet.png'),
});

await browser.close();
if (consoleErrors.length > 0) {
  console.error(`page console errors:\n  ${consoleErrors.join('\n  ')}`);
  process.exit(1);
}
console.log(`painted ${painted} tiles -> ${outDir}`);
