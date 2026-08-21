// Capture one private first-class skin through the real WASM renderer.
//
// The service token is read from the process environment, never argv, because
// the latter is visible in process listings. The token can fetch the factory's
// private revision but has no approval or publication authority.
//
// Usage:
//   SNAKETRON_FACTORY_SERVICE_TOKEN=... node capture-authored-skin.mjs \
//     http://localhost:3000 http://localhost:8080 sha256:<doc> out/evidence
import { chromium } from "@playwright/test";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  attestRendererBundle,
  classifyRendererResponse,
  digestRendererBytes,
  parsePinnedRendererBundle,
} from "./renderer-bundle-attestation.mjs";
import {
  browserRuntimeEnvironment,
  fetchPrivateSkin,
} from "./renderer-process-environment.mjs";

const [webUrl, apiUrl, contentRef, outputDir] = process.argv.slice(2);
const token = process.env.SNAKETRON_FACTORY_SERVICE_TOKEN;
const bundleManifestRaw =
  process.env.SNAKETRON_FACTORY_RENDERER_BUNDLE_MANIFEST;
const bundleManifestSha = process.env.SNAKETRON_FACTORY_RENDERER_BUNDLE_SHA256;
if (!webUrl || !apiUrl || !contentRef || !outputDir || !token) {
  throw new Error(
    "usage: capture-authored-skin.mjs <web-url> <api-url> <content-ref> <output-dir>; " +
      "SNAKETRON_FACTORY_SERVICE_TOKEN is required",
  );
}
const bundleManifest = parsePinnedRendererBundle(
  bundleManifestRaw,
  bundleManifestSha,
);

mkdirSync(outputDir, { recursive: true });
// The private API fetch below is performed by capture Node and injects the
// document into the page. Chromium never needs the service token, provider
// credentials, or the renderer-attestation payload, so do not inherit them.
const browser = await chromium.launch({ env: browserRuntimeEnvironment() });
const context = await browser.newContext({
  viewport: { width: 1440, height: 1200 },
  deviceScaleFactor: 2,
  reducedMotion: "no-preference",
  recordVideo: { dir: outputDir, size: { width: 960, height: 720 } },
});
const page = await context.newPage();
const consoleErrors = [];
const rendererResponsePromises = [];
page.on("console", (message) => {
  if (message.type() === "error") consoleErrors.push(message.text());
});
page.on("response", (response) => {
  const request = response.request();
  const classified = classifyRendererResponse(
    response.url(),
    request.resourceType(),
    response.headers()["content-type"] || "",
    webUrl,
  );
  if (!classified || response.status() >= 300) return;
  if (classified.error) {
    rendererResponsePromises.push(Promise.resolve(classified));
    return;
  }
  rendererResponsePromises.push(
    response
      .body()
      .then((body) => ({
        ...classified,
        sha256: digestRendererBytes(body),
        size_bytes: body.length,
      }))
      .catch((error) => ({
        error: `could not read renderer asset ${response.url()}: ${error}`,
      })),
  );
});

await page.goto(`${webUrl.replace(/\/$/, "")}/qa/skins`, {
  waitUntil: "networkidle",
});
await page.waitForFunction(
  () => window.wasm?.registerAuthoredSkin !== undefined,
);
await page.waitForLoadState("networkidle");
const servedRenderer = attestRendererBundle(
  bundleManifest,
  bundleManifestSha,
  await Promise.all(rendererResponsePromises),
);
writeFileSync(
  join(outputDir, "renderer-attestation.json"),
  `${JSON.stringify(servedRenderer, null, 2)}\n`,
);
if (servedRenderer.errors.length > 0) {
  throw new Error(
    `renderer bundle attestation failed: ${servedRenderer.errors.join("; ")}`,
  );
}

const document = await fetchPrivateSkin(
  context.request,
  apiUrl,
  contentRef,
  token,
);
const periodMs = Number(JSON.parse(document).period_ms || 1000);

const setup = await page.evaluate(
  ({ ref, documentJson, periodMs }) => {
    window.wasm.registerAuthoredSkin(ref, documentJson);
    const fixtures = JSON.parse(window.wasm.skinFixtures());
    const root = document.createElement("main");
    root.id = "factory-skin-evidence";
    root.style.cssText =
      "display:grid;grid-template-columns:repeat(3,max-content);gap:18px;padding:24px;" +
      "background:#171923;color:white;font:14px system-ui";
    document.body.replaceChildren(root);

    const poseByName = Object.fromEntries(
      fixtures.poses.map((pose) => [pose.name, pose]),
    );
    const requestedPoses = [
      "single_cell",
      "short_straight",
      "longer_than_head_gradient",
      "zigzag",
    ].filter((name) => poseByName[name]);
    const tiles = [];
    const add = (pose, role, animMs, boost, dead, label) => {
      const fixture = poseByName[pose];
      const figure = document.createElement("figure");
      figure.style.margin = "0";
      const canvas = document.createElement("canvas");
      canvas.width = Math.max(220, (fixture.cellsWide + 4) * 16);
      canvas.height = Math.max(160, (fixture.cellsHigh + 4) * 16);
      canvas.dataset.label = label;
      const caption = document.createElement("figcaption");
      caption.textContent = label;
      figure.append(canvas, caption);
      root.append(figure);
      tiles.push({ canvas, pose, role, animMs, boost, dead, label });
    };

    for (const role of fixtures.roles) {
      add("longer_than_head_gradient", role, 0, false, false, `role: ${role}`);
    }
    for (const pose of requestedPoses)
      add(pose, "own", 0, false, false, `pose: ${pose}`);
    add(
      "longer_than_head_gradient",
      "enemy",
      0,
      true,
      false,
      "state: enemy boost",
    );
    add("zigzag", "own", 0, false, true, "state: dead turning");
    for (const [index, fraction] of [
      0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875,
    ].entries()) {
      add(
        "longer_than_head_gradient",
        "own",
        fraction * periodMs,
        false,
        false,
        `time sample ${index}`,
      );
    }

    window.__factoryPaint = (clockOffset = 0) => {
      for (const tile of tiles) {
        window.wasm.renderSkinFixture(
          tile.canvas,
          ref,
          tile.pose,
          tile.role,
          16,
          tile.boost,
          tile.dead,
          tile.animMs + clockOffset,
          false,
        );
      }
    };
    window.__factoryCanvasesHavePixels = () =>
      tiles.every(({ canvas }) => {
        const pixels = canvas
          .getContext("2d")
          .getImageData(0, 0, canvas.width, canvas.height).data;
        for (let index = 3; index < pixels.length; index += 4) {
          if (pixels[index] !== 0) return true;
        }
        return false;
      });
    window.__factoryPaint(0);
    return {
      poses: requestedPoses,
      roles: fixtures.roles,
      tiles: tiles.length,
    };
  },
  { ref: contentRef, documentJson: document, periodMs },
);

await page.waitForFunction(
  () => window.wasm.skinAssetsPending() === false,
  null,
  {
    timeout: 20_000,
  },
);
await page.evaluate(() => window.__factoryPaint(0));
await page.waitForTimeout(200);

const assetStatus = await page.evaluate(() =>
  JSON.parse(window.wasm.skinAssetsStatus()),
);
if (assetStatus.failed > 0) {
  throw new Error(
    `${assetStatus.failed} requested image assets failed to decode`,
  );
}
if (assetStatus.requested > 0 && assetStatus.drawnImages === 0) {
  throw new Error("image assets decoded but no image pixels reached a canvas");
}
if (!(await page.evaluate(() => window.__factoryCanvasesHavePixels()))) {
  throw new Error("one or more evidence canvases remained transparent");
}

const sheetPath = join(outputDir, "contact-sheet.png");
await page.locator("#factory-skin-evidence").screenshot({ path: sheetPath });

// Drive the real animation clock for one period. The video is supplemental;
// fixed samples in the contact sheet remain deterministic and diffable.
for (let frame = 0; frame < 60; frame += 1) {
  await page.evaluate(
    (clock) => window.__factoryPaint(clock),
    (frame / 60) * periodMs,
  );
  await page.waitForTimeout(16);
}
const video = page.video();
await page.close();
const videoPath = join(outputDir, "animation.webm");
if (video) await video.saveAs(videoPath);
await context.close();
await browser.close();

const digest = (path) =>
  createHash("sha256").update(readFileSync(path)).digest("hex");
const evidence = {
  schema_version: 2,
  content_ref: contentRef,
  period_ms: periodMs,
  setup,
  asset_status: assetStatus,
  console_errors: consoleErrors,
  served_renderer: servedRenderer,
  contact_sheet: { path: sheetPath, sha256: digest(sheetPath) },
  animation: { path: videoPath, sha256: digest(videoPath) },
};
writeFileSync(
  join(outputDir, "evidence.json"),
  `${JSON.stringify(evidence, null, 2)}\n`,
);
console.log(JSON.stringify(evidence));
