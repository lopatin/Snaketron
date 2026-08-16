// Gate 2: the browser pixel comparator, and the six-skin appearance oracle.
//
// `specs/skins-prd.md` section 6.2 described this suite and it was never built
// (see that document's section 15.5). `specs/skin-shading-prd.md` sections 12
// and 17 make it a prerequisite rather than a nice-to-have, because the golden
// op trace stops being a proxy for appearance the moment a lowering changes
// grammar — and the shading engine changes grammar by design.
//
// Everything here renders in ONE browser. Both sides of every comparison go
// through the same Skia, the same antialiasing, and the same subpixel rules, so
// a non-zero difference is a real difference rather than an artifact of the
// checker.

import { test, expect } from '@playwright/test';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  SHIPPED_SKINS,
  baselineSpecs,
  sheetName,
} from './baseline-specs.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const BASELINE_DIR = join(HERE, 'baselines');
const BLESS = !!process.env.SKIN_BASELINE_BLESS;

/**
 * The harness runs with no backend, so the app's region probe and auth check
 * both log fetch failures on every page load. Reporting those as skin
 * regressions would train whoever reads this suite to ignore its output, so the
 * filter is narrow on purpose: uncaught exceptions always count, and console
 * errors count only when they are about the renderer.
 */
const IRRELEVANT_TO_SKINS = /Failed to fetch|ERR_CONNECTION_REFUSED|region/i;

async function openHarness(page) {
  const failures = [];
  page.on('console', (message) => {
    if (message.type() === 'error' && !IRRELEVANT_TO_SKINS.test(message.text())) {
      failures.push(message.text());
    }
  });
  page.on('pageerror', (error) => failures.push(String(error)));

  await page.goto('/qa/skin-parity');
  await page.waitForSelector('[data-testid="skin-parity-ready"]');
  await page.waitForFunction(() => window.__skinParity?.ready === true);
  await warmTextures(page);
  return failures;
}

/**
 * Paint every skin once, then wait for the pixels.
 *
 * A textured skin requests its atlas on its *first* paint and cannot show it
 * until it decodes. Every entry point here paints synchronously, so without
 * this the suite would baseline the flat coat underneath a coat skin — and then
 * pass forever against that baseline, which is the failure mode a pixel oracle
 * exists to rule out.
 */
async function warmTextures(page) {
  await page.evaluate((skins) => {
    for (const skin of skins) {
      window.__skinParity.render('a', {
        skin,
        pose: 'straight_horizontal',
        cellSize: 15,
      });
    }
  }, SHIPPED_SKINS);
  await page.waitForFunction(() => window.wasm?.skinAssetsPending() === false, {
    timeout: 10_000,
  });
}

/** The pose names the harness reports, straight from the Rust fixture corpus. */
async function poseNames(page) {
  return page.evaluate(() =>
    window.__skinParity.fixtures.poses.map((pose) => pose.name),
  );
}

test.describe('skin pixel comparator', () => {
  // The whole point of a checker is that it can fail. These two cases are the
  // test for the test: without them a comparator that always returned "zero
  // difference" would make every other assertion here pass vacuously.
  // Over the FULL matrix, not just the default state. An earlier version of
  // this check only compared calm snakes and passed while the harness was
  // reporting a boosting snake as differing from itself by 61 levels across
  // 240 pixels — Chrome had moved one canvas to software rasterization behind
  // our backs. A self-check that does not cover the states the real assertions
  // cover is decoration.
  test('reports zero difference for a skin against itself', async ({ page }) => {
    const failures = await openHarness(page);
    const specs = SHIPPED_SKINS.flatMap((skin) =>
      baselineSpecs(skin, ['single_cell', 'single_corner', 'zigzag']),
    );

    const drift = await page.evaluate(
      (specs) =>
        specs
          .map((spec) => ({ spec, report: window.__skinParity.compare(spec, spec) }))
          .filter(({ report }) => report.maxDelta !== 0 || report.pixels === 0)
          .map(({ spec, report }) => ({
            skin: spec.skin,
            pose: spec.pose,
            boost: spec.boost,
            maxDelta: report.maxDelta,
            over1: report.over1,
            pixels: report.pixels,
          })),
      specs,
    );

    expect(
      drift,
      'the comparator reported a difference between a render and itself, so ' +
        'every other number it produces is unreliable',
    ).toEqual([]);
    expect(failures).toEqual([]);
  });

  test('exceeds the threshold for two visibly different skins', async ({
    page,
  }) => {
    await openHarness(page);
    const report = await page.evaluate(() =>
      window.__skinParity.compare(
        { skin: 'classic@1', pose: 'longer_than_head_gradient', cellSize: 15 },
        { skin: 'ember@1', pose: 'longer_than_head_gradient', cellSize: 15 },
      ),
    );
    expect(
      report.passes,
      `classic and ember compared equal (max delta ${report.maxDelta}), so the ` +
        'comparator is not measuring what it claims to',
    ).toBe(false);
    expect(report.maxDelta).toBeGreaterThan(4);
  });
});

test.describe('shipped skins match their committed appearance', () => {
  // The oracle for the five non-classic skins. Classic has a golden op trace
  // recorded before the skin system existed; the others have only this.
  for (const skin of SHIPPED_SKINS) {
    test(`${skin} is unchanged`, async ({ page }) => {
      const failures = await openHarness(page);
      const specs = baselineSpecs(skin, await poseNames(page));
      const path = join(BASELINE_DIR, sheetName(skin));

      if (BLESS) {
        const sheet = await page.evaluate(
          (specs) => window.__skinParity.captureSheet(specs),
          specs,
        );
        mkdirSync(BASELINE_DIR, { recursive: true });
        writeFileSync(
          path,
          Buffer.from(sheet.png.replace(/^data:image\/png;base64,/, ''), 'base64'),
        );
        console.log(`blessed ${skin}: ${sheet.tiles.length} tiles -> ${path}`);
        return;
      }

      expect(
        existsSync(path),
        `no baseline for ${skin}. Record one with ` +
          'SKIN_BASELINE_BLESS=1 npm run test:skins',
      ).toBe(true);

      const png = `data:image/png;base64,${readFileSync(path).toString('base64')}`;
      const reports = await page.evaluate(
        ([specs, png]) => window.__skinParity.compareToSheet(specs, png),
        [specs, png],
      );

      const worst = reports[0];
      const broken = reports.filter((report) => !report.passes);
      expect(
        broken.map((report) => ({
          pose: report.spec.pose,
          boost: report.spec.boost,
          maxDelta: report.maxDelta,
          over1: report.over1,
          within1: report.within1,
        })),
        `${skin} no longer paints what its baseline recorded. Worst tile: ` +
          `${worst.spec.pose} (boost ${worst.spec.boost}), max delta ` +
          `${worst.maxDelta} over ${worst.over1}/${worst.pixels} pixels. ` +
          'If the change is intended, re-record with ' +
          'SKIN_BASELINE_BLESS=1 npm run test:skins and review the image diff.',
      ).toEqual([]);
      expect(failures).toEqual([]);
    });
  }
});
