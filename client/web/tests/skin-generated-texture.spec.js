const { test, expect } = require('@playwright/test');
const { statSync } = require('node:fs');
const { join } = require('node:path');

const texturePath = join(__dirname, '../public/images/skins/jaguar.v1.png');
const variantRef = `sha256:${'a'.repeat(64)}`;

test('a descriptor URL decodes and its real pixels reach the canvas', async ({ page }) => {
  const requested = [];
  await page.route('**/api/textures/variants/**', async (route) => {
    requested.push(route.request().url());
    await route.fulfill({
      path: texturePath,
      contentType: 'image/png',
      headers: { 'access-control-allow-origin': '*' },
    });
  });

  await page.goto(process.env.SNAKETRON_WEB_URL || '/qa/skins');
  await page.waitForFunction(() => Boolean(window.wasm?.skinTemplatesV2));

  const result = await page.evaluate(
    async ({ contentRef, bytes }) => {
      const template = JSON.parse(window.wasm.skinTemplatesV2())[0].document;
      template.id = 'browser-texture@1';
      template.name = 'Browser texture proof';

      window.wasm.registerDraftSkin('draft:browser-fallback', JSON.stringify(template));
      template.textures = [
        {
          name: 'browser_coat',
          ref: contentRef,
          kind: 'coat',
          descriptor: {
            kind: 'coat',
            body_columns: 13,
            variants: [
              {
                content_ref: contentRef,
                url: `/api/textures/variants/${contentRef}.png`,
                width_px: 832,
                height_px: 64,
                bytes,
                texels_per_cell: 64,
              },
            ],
          },
        },
      ];
      template.layers.splice(2, 0, {
        name: 'Generated coat',
        boost_only: false,
        omit_on_single_cell: false,
        opacity: '1',
        transform: {
          translate_s: '0',
          translate_t: '0',
          scale_s: '1',
          scale_t: '1',
          rotate_turns: '0',
        },
        type: 'span',
        region: 'body',
        clip: 'silhouette',
        span: { from: 'whole', min: 0, priority: 0 },
        corner: 'fan',
        source: {
          type: 'image',
          texture: 'browser_coat',
          fit: { type: 'tile', cells_per_repeat: 13 },
          drift_cells: 0,
        },
      });
      window.wasm.registerDraftSkin('draft:browser-generated', JSON.stringify(template));

      const makeCanvas = () => {
        const canvas = document.createElement('canvas');
        canvas.width = 900;
        canvas.height = 300;
        document.body.append(canvas);
        return canvas;
      };
      const fallback = makeCanvas();
      const generated = makeCanvas();
      const render = (canvas, skin) =>
        window.wasm.renderSkinFixture(
          canvas,
          skin,
          'longer_than_head_gradient',
          'own',
          16,
          false,
          false,
          0,
          true,
        );

      render(fallback, 'draft:browser-fallback');
      render(generated, 'draft:browser-generated');
      const deadline = performance.now() + 10_000;
      while (window.wasm.skinAssetsPending() && performance.now() < deadline) {
        await new Promise(requestAnimationFrame);
      }
      render(generated, 'draft:browser-generated');

      const fallbackPixels = fallback
        .getContext('2d')
        .getImageData(0, 0, fallback.width, fallback.height).data;
      const generatedPixels = generated
        .getContext('2d')
        .getImageData(0, 0, generated.width, generated.height).data;
      let changedChannels = 0;
      for (let index = 0; index < fallbackPixels.length; index += 1) {
        if (fallbackPixels[index] !== generatedPixels[index]) changedChannels += 1;
      }
      return {
        changedChannels,
        status: JSON.parse(window.wasm.skinAssetsStatus()),
      };
    },
    { contentRef: variantRef, bytes: statSync(texturePath).size },
  );

  expect(requested).toEqual([
    `http://localhost:8080/api/textures/variants/${variantRef}.png`,
  ]);
  expect(result.status).toMatchObject({
    requested: 1,
    pending: 0,
    ready: 1,
    failed: 0,
    drawnImages: 1,
  });
  expect(result.status.drawCalls).toBeGreaterThan(0);
  expect(result.changedChannels).toBeGreaterThan(100);
});

test('a 64-row sheet addresses every row and loops to row zero in the browser', async ({
  page,
}) => {
  await page.goto(process.env.SNAKETRON_WEB_URL || '/qa/skins');
  await page.waitForFunction(() => Boolean(window.wasm?.skinTemplatesV2));

  // Make an exact 13-by-64 production-shaped grid in the browser. Each frame
  // is a visibly distinct 16px row, so the image is both valid evidence and a
  // useful failure artifact if source-rectangle sampling regresses.
  const png = await page.evaluate(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 13 * 16;
    canvas.height = 64 * 16;
    const context = canvas.getContext('2d');
    for (let row = 0; row < 64; row += 1) {
      context.fillStyle = `hsl(${(row * 47) % 360} 80% ${35 + (row % 4) * 10}%)`;
      context.fillRect(0, row * 16, canvas.width, 16);
    }
    return canvas.toDataURL('image/png').split(',')[1];
  });
  const sheetBytes = Buffer.from(png, 'base64');
  const sheetRef = `sha256:${'b'.repeat(64)}`;
  const sheetUrl = `http://localhost:8080/api/textures/variants/${sheetRef}.png`;
  const requested = [];
  await page.route('**/api/textures/variants/**', (route) => {
    requested.push(route.request().url());
    return route.fulfill({
      body: sheetBytes,
      contentType: 'image/png',
      headers: { 'access-control-allow-origin': '*' },
    });
  });

  const result = await page.evaluate(
    async ({ contentRef, bytes }) => {
      const periodMs = 2400;
      const template = JSON.parse(window.wasm.skinTemplatesV2())[0].document;
      template.id = 'browser-sheet@1';
      template.name = 'Browser sheet sampling proof';
      template.period_ms = periodMs;
      template.textures = [
        {
          name: 'browser_sheet',
          ref: contentRef,
          kind: 'sheet',
          descriptor: {
            kind: 'sheet',
            body_columns: 13,
            frame_rows: 64,
            variants: [
              {
                content_ref: contentRef,
                url: `/api/textures/variants/${contentRef}.png`,
                width_px: 13 * 16,
                height_px: 64 * 16,
                bytes,
                texels_per_cell: 16,
              },
            ],
          },
        },
      ];
      template.layers.splice(2, 0, {
        name: 'Generated sheet',
        boost_only: false,
        omit_on_single_cell: false,
        opacity: '1',
        transform: {
          translate_s: '0',
          translate_t: '0',
          scale_s: '1',
          scale_t: '1',
          rotate_turns: '0',
        },
        type: 'span',
        region: 'body',
        clip: 'silhouette',
        span: { from: 'whole', min: 0, priority: 0 },
        corner: 'fan',
        source: {
          type: 'image',
          texture: 'browser_sheet',
          fit: { type: 'tile', cells_per_repeat: 13 },
          drift_cells: 0,
        },
      });
      window.wasm.registerDraftSkin('draft:browser-sheet', JSON.stringify(template));

      const sampledRows = [];
      const drawImage = CanvasRenderingContext2D.prototype.drawImage;
      CanvasRenderingContext2D.prototype.drawImage = function instrumented(image, ...args) {
        if (image instanceof HTMLImageElement && image.src.includes(contentRef)) {
          sampledRows.push(args[1] / 16);
        }
        return drawImage.call(this, image, ...args);
      };

      const canvas = document.createElement('canvas');
      canvas.width = 900;
      canvas.height = 300;
      document.body.append(canvas);
      const render = (clock) =>
        window.wasm.renderSkinFixture(
          canvas,
          'draft:browser-sheet',
          'longer_than_head_gradient',
          'own',
          16,
          false,
          false,
          clock,
          false,
        );

      render(0);
      const deadline = performance.now() + 10_000;
      while (window.wasm.skinAssetsPending() && performance.now() < deadline) {
        await new Promise(requestAnimationFrame);
      }

      sampledRows.length = 0;
      const addressed = [];
      for (let row = 0; row < 64; row += 1) {
        const before = sampledRows.length;
        render((periodMs * (row + 0.25)) / 64);
        addressed.push([...new Set(sampledRows.slice(before))]);
      }
      const beforeLoop = sampledRows.length;
      render(periodMs);
      const loop = [...new Set(sampledRows.slice(beforeLoop))];
      CanvasRenderingContext2D.prototype.drawImage = drawImage;

      return {
        addressed,
        loop,
        status: JSON.parse(window.wasm.skinAssetsStatus()),
      };
    },
    { contentRef: sheetRef, bytes: sheetBytes.length },
  );

  expect(result.addressed).toEqual(Array.from({ length: 64 }, (_, row) => [row]));
  expect(result.loop).toEqual([0]);
  expect(result.status).toMatchObject({
    requested: 1,
    pending: 0,
    ready: 1,
    failed: 0,
    drawnImages: 1,
  });
  expect(result.status.drawCalls).toBeGreaterThanOrEqual(65);
  expect(requested).toEqual([sheetUrl]);
});
