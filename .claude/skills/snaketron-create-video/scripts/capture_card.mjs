#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const SKILL_DIR = resolve(SCRIPT_DIR, "..");
const CARD_ALIASES = new Map([
  ["rank-up", "rank-up.html"],
  ["rank-up-card", "rank-up.html"],
  ["leaderboard", "leaderboard.html"],
  ["leaderboard-card", "leaderboard.html"],
]);

function usage(message) {
  if (message) process.stderr.write(`${message}\n\n`);
  process.stderr.write(
    "Usage: node scripts/capture_card.mjs (--card <rank-up|leaderboard> | --url <route>) --out <clip-dir> " +
      "[--capture-vfps 60] [--duration-ms 4000] [--width 1920] [--height 1080] " +
      "[--param key=value] [--headless-shell PATH] [--virtual-time]\n",
  );
  process.exit(message ? 2 : 0);
}

function parseArgs(argv) {
  const values = {};
  const params = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") usage();
    if (token === "--virtual-time") {
      values.virtualTime = true;
      continue;
    }
    if (!token.startsWith("--") || index + 1 >= argv.length) {
      usage(`Invalid argument: ${token}`);
    }
    const key = token.slice(2);
    const value = argv[index + 1];
    index += 1;
    if (key === "param") params.push(value);
    else values[key] = value;
  }
  if (!values.out) usage("--out is required");
  const routeUrl = values.url ?? null;
  if (!values.card && !routeUrl) usage("--card or --url is required");
  const cardFile = values.card ? CARD_ALIASES.get(values.card) : null;
  if (values.card && !cardFile) usage("--card must be rank-up or leaderboard");
  const captureVfps = Number(values["capture-vfps"] ?? 60);
  const width = Number(values.width ?? 1920);
  const height = Number(values.height ?? 1080);
  const durationMs = values["duration-ms"] === undefined ? null : Number(values["duration-ms"]);
  if (!Number.isInteger(captureVfps) || captureVfps < 1 || captureVfps > 240) {
    usage("--capture-vfps must be an integer from 1 to 240");
  }
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 64 || height < 64) {
    usage("--width and --height must be integers >= 64");
  }
  if (durationMs !== null && (!Number.isFinite(durationMs) || durationMs <= 0)) {
    usage("--duration-ms must be positive");
  }
  const query = new URLSearchParams();
  for (const item of params) {
    const separator = item.indexOf("=");
    if (separator <= 0) usage(`--param must be key=value, got ${item}`);
    query.append(item.slice(0, separator), item.slice(separator + 1));
  }
  return {
    fixtureId:
      values.id ??
      (routeUrl
        ? new URL(routeUrl).searchParams.get("card") ?? "route-card"
        : values.card.startsWith("rank")
          ? "rank-up-card"
          : "leaderboard-card"),
    routeUrl,
    cardPath: cardFile ? resolve(SKILL_DIR, "assets", "cards", cardFile) : null,
    outDir: resolve(values.out),
    captureVfps,
    durationMs,
    width,
    height,
    query,
    executablePath: values["headless-shell"] ?? process.env.SNAKETRON_HEADLESS_SHELL,
    forceVirtualTime: Boolean(values.virtualTime),
  };
}

function findRepoRoot() {
  let cursor = SCRIPT_DIR;
  for (;;) {
    if (existsSync(join(cursor, "client", "web", "package.json"))) return cursor;
    const parent = dirname(cursor);
    if (parent === cursor) break;
    cursor = parent;
  }
  throw new Error("cannot find the SnakeTron repository root from the skill directory");
}

function sha256(parts) {
  const digest = createHash("sha256");
  for (const part of parts) digest.update(part);
  return digest.digest("hex");
}

async function sourceDigest(cardPath, query) {
  // A route-backed card has no single source file; its identity is the URL.
  // The app bundle behind it is covered by the dev server, not this digest.
  if (cardPath === null) return sha256([Buffer.from(query.toString())]);
  const fontDir = resolve(SKILL_DIR, "assets", "fonts");
  return sha256(
    await Promise.all([
      readFile(cardPath),
      readFile(resolve(fontDir, "BarlowCondensed-ExtraBoldItalic.ttf")),
      readFile(resolve(fontDir, "Inter-Variable.ttf")),
      Buffer.from(query.toString()),
    ]),
  );
}

async function writePipe(stream, bytes) {
  if (!stream.write(bytes)) await once(stream, "drain");
}

function startEncoder(output, fps) {
  const args = [
    "-hide_banner",
    "-loglevel",
    "error",
    "-y",
    "-fflags",
    "+bitexact",
    "-f",
    "image2pipe",
    "-framerate",
    String(fps),
    "-vcodec",
    "png",
    "-i",
    "pipe:0",
    "-an",
    "-bitexact",
    "-c:v",
    "libx264rgb",
    "-qp",
    "0",
    "-preset",
    "medium",
    "-flags:v",
    "+bitexact",
    "-map_metadata",
    "-1",
    "-metadata",
    "snaketron_color_pipeline=lossless-rgb",
    "-f",
    "matroska",
    output,
  ];
  const child = spawn(process.env.FFMPEG ?? "ffmpeg", args, {
    stdio: ["pipe", "inherit", "pipe"],
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const completion = new Promise((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else rejectPromise(new Error(`ffmpeg failed (${signal ?? code}): ${stderr.trim()}`));
    });
  });
  return { child, completion };
}

async function beginFrameSession(page, forceFallback, explicitHeadlessShell) {
  if (forceFallback) return null;
  const session = await page.context().newCDPSession(page);
  try {
    await session.send("HeadlessExperimental.enable");
    const probe = await session.send("HeadlessExperimental.beginFrame", {
      frameTimeTicks: 0,
      noDisplayUpdates: false,
      screenshot: { format: "png" },
    });
    if (!probe.screenshotData) throw new Error("BeginFrame returned no screenshot");
    return session;
  } catch (error) {
    await session.detach().catch(() => {});
    if (explicitHeadlessShell) {
      throw new Error(
        `headless-shell does not expose HeadlessExperimental.beginFrame: ${error.message}`,
      );
    }
    return null;
  }
}

function normalizedAnchors(cueTrack, durationSeconds) {
  const source = cueTrack?.anchors;
  if (!source || typeof source !== "object" || Array.isArray(source)) return {};
  return Object.fromEntries(
    Object.entries(source)
      .filter(([, value]) => Number.isFinite(value) && value >= 0 && value <= durationSeconds)
      .sort(([left], [right]) => left.localeCompare(right)),
  );
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const repoRoot = findRepoRoot();
  const requireFromWeb = createRequire(resolve(repoRoot, "client", "web", "package.json"));
  const { chromium } = requireFromWeb("@playwright/test");
  await mkdir(options.outDir, { recursive: true });
  const master = resolve(options.outDir, "master.mkv");
  const temporaryMaster = `${master}.partial`;
  await rm(temporaryMaster, { force: true });

  const browser = await chromium.launch({
    headless: true,
    executablePath: options.executablePath,
    args: [
      "--deterministic-mode",
      "--disable-gpu",
      "--disable-dev-shm-usage",
      "--hide-scrollbars",
      "--force-device-scale-factor=1",
      "--allow-file-access-from-files",
    ],
  });
  try {
    const context = await browser.newContext({
      viewport: { width: options.width, height: options.height },
      deviceScaleFactor: 1,
      colorScheme: "dark",
      reducedMotion: "reduce",
    });
    const page = await context.newPage();
    page.on("pageerror", (error) => {
      process.stderr.write(`browser page error: ${error.stack ?? error.message}\n`);
    });
    const routeOrigin = options.routeUrl ? new URL(options.routeUrl).origin : null;
    await page.route("**/*", async (route) => {
      const url = new URL(route.request().url());
      const allowed = ["file:", "data:", "blob:"].includes(url.protocol) ||
        (routeOrigin !== null && url.origin === routeOrigin);
      if (allowed) await route.continue();
      else await route.abort("blockedbyclient");
    });
    const target = options.routeUrl
      ? new URL(options.routeUrl)
      : pathToFileURL(options.cardPath);
    if (options.query.toString()) {
      for (const [key, value] of options.query) target.searchParams.set(key, value);
    }
    await page.goto(target.href, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.waitForFunction(() => Boolean(window.__SNAKETRON_CAPTURE__), null, {
      timeout: 30_000,
    });
    await page.evaluate(() => window.__SNAKETRON_CAPTURE__.ready());
    const initial = await page.evaluate(() => ({
      durationMs: window.__SNAKETRON_CAPTURE__.durationMs?.() ?? null,
      renderedTick: window.__SNAKETRON_CAPTURE__.renderedTick(),
      cueTrack: window.__SNAKETRON_CAPTURE__.cueTrack(),
    }));
    const durationMs =
      options.durationMs ?? initial.durationMs ?? Number(initial.cueTrack?.duration) * 1000;
    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      throw new Error("fixture did not expose a positive duration; pass --duration-ms");
    }

    const cdp = await beginFrameSession(
      page,
      options.forceVirtualTime,
      Boolean(options.executablePath),
    );
    const frameMs = 1000 / options.captureVfps;
    const frameCount = Math.ceil(durationMs / frameMs);
    const encoder = startEncoder(temporaryMaster, options.captureVfps);
    try {
      for (let frame = 0; frame < frameCount; frame += 1) {
        await page.evaluate((dt) => window.__SNAKETRON_CAPTURE__.stepMs(dt), frameMs);
        let png;
        if (cdp) {
          const result = await cdp.send("HeadlessExperimental.beginFrame", {
            frameTimeTicks: (frame + 1) * frameMs,
            noDisplayUpdates: false,
            screenshot: { format: "png" },
          });
          if (!result.screenshotData) throw new Error(`BeginFrame ${frame} returned no pixels`);
          png = Buffer.from(result.screenshotData, "base64");
        } else {
          png = await page.screenshot({ type: "png", animations: "disabled" });
        }
        await writePipe(encoder.child.stdin, png);
      }
      encoder.child.stdin.end();
      await encoder.completion;
    } catch (error) {
      encoder.child.stdin.destroy();
      encoder.child.kill("SIGTERM");
      await encoder.completion.catch(() => {});
      await rm(temporaryMaster, { force: true });
      throw error;
    }
    await cdp?.detach().catch(() => {});
    await rename(temporaryMaster, master);

    const finalState = await page.evaluate(() => ({
      renderedTick: window.__SNAKETRON_CAPTURE__.renderedTick(),
      cueTrack: window.__SNAKETRON_CAPTURE__.cueTrack(),
    }));
    const durationSeconds = durationMs / 1000;
    const anchors = normalizedAnchors(finalState.cueTrack, durationSeconds);
    const masterBytes = await readFile(master);
    const meta = {
      schema_version: 1,
      scenario_id: options.fixtureId,
      fixture_id: options.fixtureId,
      source_kind: "fixture-card",
      source_file: options.cardPath ? basename(options.cardPath) : options.routeUrl,
      source_sha256: await sourceDigest(options.cardPath, options.query),
      source_params: Object.fromEntries(options.query),
      capture_vfps: options.captureVfps,
      encoded_fps: options.captureVfps,
      width: options.width,
      height: options.height,
      duration: durationSeconds,
      duration_ms: durationMs,
      frame_count: frameCount,
      capture_path: cdp ? "beginframe" : "contract-screenshot",
      deterministic_scope: "same-machine-software-raster",
      rendered_tick: finalState.renderedTick,
      anchors,
      cue_track: finalState.cueTrack,
      cue_timeline: Object.entries(anchors).map(([type, atMasterSeconds]) => ({
        type,
        at_master_seconds: atMasterSeconds,
      })),
      master_sha256: sha256([masterBytes]),
      master_file: basename(master),
    };
    const metaFile = resolve(options.outDir, "meta.json");
    await writeFile(metaFile, `${JSON.stringify(meta, null, 2)}\n`);
    process.stdout.write(
      `${JSON.stringify(
        {
          fixture_id: meta.fixture_id,
          capture_path: meta.capture_path,
          frame_count: meta.frame_count,
          duration_ms: meta.duration_ms,
          master_sha256: meta.master_sha256,
          master_file: master,
          meta_file: metaFile,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  process.stderr.write(`capture_card failed: ${error.stack ?? error}\n`);
  process.exitCode = 1;
});
