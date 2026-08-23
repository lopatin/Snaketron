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

test('16x16 cells retain a bounded transverse bleed apron across live poses and Boost', async ({
  page,
}) => {
  await page.goto(process.env.SNAKETRON_WEB_URL || '/qa/skins');
  await page.waitForFunction(() => Boolean(window.wasm?.skinTemplatesV2));

  const png = await page.evaluate(() => {
    const canvas = document.createElement('canvas');
    canvas.width = 4 * 16;
    canvas.height = 24;
    const context = canvas.getContext('2d');
    context.fillStyle = '#ff00ff';
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = '#00ffff';
    context.fillRect(0, 4, canvas.width, 16);
    return canvas.toDataURL('image/png').split(',')[1];
  });
  const bytes = Buffer.from(png, 'base64');
  const contentRef = `sha256:${'c'.repeat(64)}`;
  await page.route('**/api/textures/variants/**', (route) =>
    route.fulfill({
      body: bytes,
      contentType: 'image/png',
      headers: { 'access-control-allow-origin': '*' },
    }),
  );

  const result = await page.evaluate(
    async ({ contentRef, byteLength }) => {
      const template = JSON.parse(window.wasm.skinTemplatesV2())[0].document;
      template.id = 'browser-bounded-bleed@1';
      template.name = 'Browser bounded bleed proof';
      template.textures = [
        {
          name: 'wide_coat',
          ref: contentRef,
          kind: 'coat',
          descriptor: {
            kind: 'coat',
            body_columns: 4,
            raster_overhang_px: 4,
            variants: [
              {
                content_ref: contentRef,
                url: `/api/textures/variants/${contentRef}.png`,
                width_px: 64,
                height_px: 24,
                bytes: byteLength,
                texels_per_cell: 16,
              },
            ],
          },
        },
      ];
      template.layers.splice(2, 0, {
        name: '16x16 coat with bleed apron',
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
          texture: 'wide_coat',
          fit: {
            type: 'tile',
            cells_per_repeat: 4,
            phase_origin: 'tail',
          },
          drift_cells: 0,
        },
      });
      window.wasm.registerDraftSkin('draft:browser-bounded-bleed', JSON.stringify(template));

      const background = '#112233';
      const makeCanvas = () => {
        const canvas = document.createElement('canvas');
        canvas.width = 300;
        canvas.height = 200;
        document.body.append(canvas);
        return canvas;
      };
      const render = (canvas, pose, cell, boost = false) =>
        window.wasm.renderSkinFixture(
          canvas,
          'draft:browser-bounded-bleed',
          pose,
          'own',
          cell,
          boost,
          false,
          0,
          true,
          background,
        );

      const warm = makeCanvas();
      render(warm, 'straight_horizontal', 16);
      const deadline = performance.now() + 10_000;
      while (window.wasm.skinAssetsPending() && performance.now() < deadline) {
        await new Promise(requestAnimationFrame);
      }

      const pixel = (canvas, x, y) =>
        [...canvas.getContext('2d').getImageData(x, y, 1, 1).data];
      const samples = {};
      for (const cell of [5, 10, 15]) {
        const straight = makeCanvas();
        render(straight, 'straight_horizontal', cell);
        const bodyTop = 3 * cell;
        const x = 5 * cell;
        const insideMarginY = Math.floor(bodyTop - (cell / 4) / 2);
        const outsideY = Math.max(0, Math.floor(bodyTop - cell / 4 - 2));
        samples[`straight-${cell}`] = {
          margin: pixel(straight, x, insideMarginY),
          outside: pixel(straight, x, outsideY),
        };
      }

      const single = makeCanvas();
      render(single, 'single_cell', 5);
      samples.single = {
        transverse: pixel(single, 17, 14),
        // Two pixels clear of the body also clears the engine-owned ordinary
        // 1px contour; a raster bug would reach four authored pixels here.
        beforeHead: pixel(single, 13, 17),
        afterTail: pixel(single, 22, 17),
      };

      const corner = makeCanvas();
      render(corner, 'single_corner', 15);
      let cornerOutsidePaint = 0;
      const cornerPixels = corner.getContext('2d').getImageData(0, 0, 300, 200).data;
      for (let y = 0; y < 200; y += 1) {
        for (let x = 0; x < 300; x += 1) {
          const index = (y * 300 + x) * 4;
          const magenta =
            cornerPixels[index] > 200 &&
            cornerPixels[index + 1] < 80 &&
            cornerPixels[index + 2] > 200;
          if (magenta && (x < 45 || y < 45 || x >= 135 || y >= 135)) {
            cornerOutsidePaint += 1;
          }
        }
      }

      const drawImage = CanvasRenderingContext2D.prototype.drawImage;
      const draws = [];
      CanvasRenderingContext2D.prototype.drawImage = function instrumented(image, ...args) {
        if (image instanceof HTMLImageElement && image.src.includes(contentRef)) {
          draws.push(args);
        }
        return drawImage.call(this, image, ...args);
      };
      const resting = makeCanvas();
      render(resting, 'straight_horizontal', 15, false);
      const restDraws = draws.splice(0);
      const boosting = makeCanvas();
      render(boosting, 'straight_horizontal', 15, true);
      const boostDraws = draws.splice(0);
      CanvasRenderingContext2D.prototype.drawImage = drawImage;

      const boostPixels = boosting
        .getContext('2d')
        .getImageData(0, 0, boosting.width, boosting.height).data;
      let yellowPixels = 0;
      for (let index = 0; index < boostPixels.length; index += 4) {
        if (
          boostPixels[index] > 200 &&
          boostPixels[index + 1] > 180 &&
          boostPixels[index + 2] < 100
        ) {
          yellowPixels += 1;
        }
      }

      return {
        samples,
        cornerOutsidePaint,
        restDraws,
        boostDraws,
        yellowPixels,
        status: JSON.parse(window.wasm.skinAssetsStatus()),
      };
    },
    { contentRef, byteLength: bytes.length },
  );

  const isMagenta = ([r, g, b, a]) => r > 200 && g < 80 && b > 200 && a === 255;
  const isBackground = ([r, g, b, a]) => r === 17 && g === 34 && b === 51 && a === 255;
  for (const cell of [5, 10, 15]) {
    expect(isMagenta(result.samples[`straight-${cell}`].margin)).toBe(true);
    expect(isBackground(result.samples[`straight-${cell}`].outside)).toBe(true);
  }
  expect(isMagenta(result.samples.single.transverse)).toBe(true);
  expect(isBackground(result.samples.single.beforeHead)).toBe(true);
  expect(isBackground(result.samples.single.afterTail)).toBe(true);
  expect(result.cornerOutsidePaint).toBeGreaterThan(0);
  expect(result.boostDraws).toEqual(result.restDraws);
  expect(result.yellowPixels).toBeGreaterThan(10);
  expect(result.status).toMatchObject({ pending: 0, ready: 1, failed: 0 });
});

test('RGBA bleed composites over an earlier snake instead of an opaque apron mask', async ({
  page,
}) => {
  await page.goto(process.env.SNAKETRON_WEB_URL || '/qa/skins');
  await page.waitForFunction(() => Boolean(window.wasm?.skinTemplatesV2));

  const encoded = await page.evaluate(() => {
    const encode = (red, green, blue, alpha) => {
      const canvas = document.createElement('canvas');
      canvas.width = 4 * 16;
      canvas.height = 24;
      const context = canvas.getContext('2d');
      context.clearRect(0, 0, canvas.width, canvas.height);
      context.fillStyle = `rgba(${red}, ${green}, ${blue}, ${alpha})`;
      context.fillRect(0, 0, canvas.width, canvas.height);
      return canvas.toDataURL('image/png').split(',')[1];
    };
    return {
      lower: encode(0, 0, 255, 1),
      transparent: encode(255, 0, 0, 0),
      partial: encode(255, 0, 0, 0.5),
      opaque: encode(255, 0, 0, 1),
    };
  });
  const refs = {
    lower: `sha256:${'d'.repeat(64)}`,
    transparent: `sha256:${'e'.repeat(64)}`,
    partial: `sha256:${'f'.repeat(64)}`,
    opaque: `sha256:${'0'.repeat(64)}`,
  };
  const pngByRef = Object.fromEntries(
    Object.entries(refs).map(([name, contentRef]) => [
      contentRef,
      Buffer.from(encoded[name], 'base64'),
    ]),
  );
  await page.route('**/api/textures/variants/**', (route) => {
    const contentRef = route.request().url().split('/').at(-1).replace(/\.png$/, '');
    return route.fulfill({
      body: pngByRef[contentRef],
      contentType: 'image/png',
      headers: { 'access-control-allow-origin': '*' },
    });
  });

  const result = await page.evaluate(
    async ({ refs, byteLengths }) => {
      const imageLayer = (texture) => ({
        name: `${texture} raster sentinel`,
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
          texture,
          fit: { type: 'tile', cells_per_repeat: 4, phase_origin: 'tail' },
          drift_cells: 0,
        },
      });
      const makeDocument = (name) => {
        const template = JSON.parse(window.wasm.skinTemplatesV2())[0].document;
        const contentRef = refs[name];
        const texture = `${name}_coat`;
        template.id = `browser-alpha-${name}@1`;
        template.name = `Browser alpha ${name}`;
        template.textures = [
          {
            name: texture,
            ref: contentRef,
            kind: 'coat',
            descriptor: {
              kind: 'coat',
              body_columns: 4,
              raster_overhang_px: 4,
              variants: [
                {
                  content_ref: contentRef,
                  url: `/api/textures/variants/${contentRef}.png`,
                  width_px: 64,
                  height_px: 24,
                  bytes: byteLengths[name],
                  texels_per_cell: 16,
                },
              ],
            },
          },
        ];
        template.layers.splice(2, 0, imageLayer(texture));
        return template;
      };

      for (const name of Object.keys(refs)) {
        window.wasm.registerDraftSkin(
          `draft:browser-alpha-${name}`,
          JSON.stringify(makeDocument(name)),
        );
      }

      const background = '#112233';
      const makeCanvas = () => {
        const canvas = document.createElement('canvas');
        canvas.width = 300;
        canvas.height = 200;
        document.body.append(canvas);
        return canvas;
      };
      const render = (canvas, name) =>
        window.wasm.renderSkinFixture(
          canvas,
          `draft:browser-alpha-${name}`,
          'straight_horizontal',
          'own',
          16,
          false,
          false,
          0,
          true,
          background,
        );

      for (const name of Object.keys(refs)) render(makeCanvas(), name);
      const deadline = performance.now() + 10_000;
      while (window.wasm.skinAssetsPending() && performance.now() < deadline) {
        await new Promise(requestAnimationFrame);
      }

      const samples = {};
      for (const name of ['transparent', 'partial', 'opaque']) {
        const canvas = makeCanvas();
        render(canvas, 'lower');

        // `renderSkinFixture` normally starts a fresh review tile. Suppress
        // only that one canvas-sized clear so this second call exercises the
        // same lower-then-upper ordering as two arena snakes. Every body and
        // contour occlusion operation still reaches the real browser canvas.
        const context = canvas.getContext('2d');
        const fillRect = CanvasRenderingContext2D.prototype.fillRect;
        let skippedTileClear = false;
        CanvasRenderingContext2D.prototype.fillRect = function preserveEarlierSnake(
          x,
          y,
          width,
          height,
        ) {
          if (
            !skippedTileClear &&
            this === context &&
            x === 0 &&
            y === 0 &&
            width === canvas.width &&
            height === canvas.height
          ) {
            skippedTileClear = true;
            return;
          }
          return fillRect.call(this, x, y, width, height);
        };
        try {
          render(canvas, name);
        } finally {
          CanvasRenderingContext2D.prototype.fillRect = fillRect;
        }
        if (!skippedTileClear) throw new Error('fixture tile clear was not intercepted');

        // The horizontal fixture's logical body starts at y=48. y=45 is in
        // its four-pixel raster apron, outside its fixed one-pixel contour,
        // and inside the already-painted lower snake's opaque blue apron.
        samples[name] = [
          ...context.getImageData(80, 45, 1, 1).data,
        ];
      }

      return {
        samples,
        status: JSON.parse(window.wasm.skinAssetsStatus()),
      };
    },
    {
      refs,
      byteLengths: Object.fromEntries(
        Object.entries(pngByRef).map(([contentRef, bytes]) => [
          Object.entries(refs).find(([, ref]) => ref === contentRef)[0],
          bytes.length,
        ]),
      ),
    },
  );

  expect(result.samples.transparent).toEqual([0, 0, 255, 255]);
  expect(result.samples.partial[0]).toBeGreaterThanOrEqual(127);
  expect(result.samples.partial[0]).toBeLessThanOrEqual(128);
  expect(result.samples.partial[1]).toBe(0);
  expect(result.samples.partial[2]).toBeGreaterThanOrEqual(127);
  expect(result.samples.partial[2]).toBeLessThanOrEqual(128);
  expect(result.samples.partial[3]).toBe(255);
  expect(result.samples.opaque).toEqual([255, 0, 0, 255]);
  expect(result.status).toMatchObject({ pending: 0, ready: 4, failed: 0 });
});
