// Capture the model geometry guide's native pixels through the real WASM
// renderer. The Python wrapper owns the local static server, deterministic PNG
// encoding, and 4x presentation transform; this helper only returns the exact
// 15 px/cell Canvas pixels as RGB bytes.
import { chromium } from "@playwright/test";
import { readFileSync } from "node:fs";
import { browserRuntimeEnvironment } from "./renderer-process-environment.mjs";

const [baseUrl, skinPath, fixture, role, cellRaw, widthRaw, heightRaw, field] =
  process.argv.slice(2);
if (
  !baseUrl ||
  !skinPath ||
  !fixture ||
  !role ||
  !cellRaw ||
  !widthRaw ||
  !heightRaw ||
  !field
) {
  throw new Error(
    "usage: render-prototype-reference.mjs <base-url> <skin-json> <fixture> " +
      "<role> <cell-px> <width-px> <height-px> <field-color>",
  );
}

const cell = Number(cellRaw);
const width = Number(widthRaw);
const height = Number(heightRaw);
if (![cell, width, height].every(Number.isInteger))
  throw new Error("cell and canvas dimensions must be integers");

const browser = await chromium.launch({
  headless: true,
  env: browserRuntimeEnvironment(),
});
try {
  const context = await browser.newContext({
    viewport: { width, height },
    deviceScaleFactor: 1,
    reducedMotion: "reduce",
    colorScheme: "light",
  });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/skin-schema/`, { waitUntil: "domcontentloaded" });
  const result = await page.evaluate(
    async ({
      moduleUrl,
      documentJson,
      fixture,
      role,
      cell,
      width,
      height,
      field,
    }) => {
      const wasm = await import(moduleUrl);
      await wasm.default();
      const handle = "draft:prototype-geometry-reference";
      wasm.registerDraftSkin(handle, documentJson);
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      wasm.renderSkinFixture(
        canvas,
        handle,
        fixture,
        role,
        cell,
        false,
        false,
        0,
        true,
        field,
      );
      const rgba = canvas
        .getContext("2d", { willReadFrequently: true })
        .getImageData(0, 0, width, height).data;
      const rgb = new Uint8Array(width * height * 3);
      for (let source = 0, target = 0; source < rgba.length; source += 4) {
        if (rgba[source + 3] !== 255)
          throw new Error(
            `renderer left alpha ${rgba[source + 3]} at pixel ${source / 4}`,
          );
        rgb[target++] = rgba[source];
        rgb[target++] = rgba[source + 1];
        rgb[target++] = rgba[source + 2];
      }
      return { width, height, rgb: Array.from(rgb) };
    },
    {
      moduleUrl: `${baseUrl}/client/pkg/client.js`,
      documentJson: readFileSync(skinPath, "utf8"),
      fixture,
      role,
      cell,
      width,
      height,
      field,
    },
  );
  process.stdout.write(
    JSON.stringify({
      width: result.width,
      height: result.height,
      rgb_base64: Buffer.from(result.rgb).toString("base64"),
    }),
  );
  await context.close();
} finally {
  await browser.close();
}
