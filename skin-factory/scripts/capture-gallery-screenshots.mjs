#!/usr/bin/env node
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const requireFromWeb = createRequire(path.resolve(here, "../../client/web/package.json"));
const { chromium } = requireFromWeb("playwright");

const [baseUrl, reviewToken, fixturePath, outputDir] = process.argv.slice(2);
if (!baseUrl || !reviewToken || !fixturePath || !outputDir) {
  throw new Error("usage: capture-gallery-screenshots.mjs BASE TOKEN FIXTURE_JSON OUTPUT_DIR");
}
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1050 },
    deviceScaleFactor: 1,
    colorScheme: "dark",
    extraHTTPHeaders: {
      authorization: `Bearer ${reviewToken}`,
      "x-review-actor": "human:documentation",
    },
  });
  const page = await context.newPage();

  async function settle(url) {
    await page.goto(url, { waitUntil: "networkidle" });
    await page.locator("main").waitFor({ state: "visible" });
    await page.evaluate(async () => {
      await Promise.all(
        [...document.images]
          .filter((image) => !image.complete)
          .map((image) => new Promise((resolve) => {
            image.addEventListener("load", resolve, { once: true });
            image.addEventListener("error", resolve, { once: true });
          })),
      );
    });
  }

  await settle(`${baseUrl}/?view=all`);
  await page.screenshot({ path: path.join(outputDir, "gallery-all.png"), fullPage: true });

  await settle(`${baseUrl}/?view=machine-rejected`);
  await page.screenshot({
    path: path.join(outputDir, "gallery-machine-rejected.png"),
    fullPage: true,
  });

  await settle(`${baseUrl}/attempts/${fixture.review_attempt}`);
  await page.screenshot({
    path: path.join(outputDir, "prototype-review-detail.png"),
    fullPage: false,
  });

  await settle(`${baseUrl}/attempts/${fixture.soft_reject_attempt}`);
  await page.screenshot({
    path: path.join(outputDir, "soft-triage-override.png"),
    fullPage: true,
  });

  await context.close();
} finally {
  await browser.close();
}
