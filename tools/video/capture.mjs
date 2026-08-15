#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { once } from 'node:events';
import { createRequire } from 'node:module';
import { basename, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import {
  buildStarHeadFrames,
  CAPTURE_BROWSER_ARGS,
  FALLBACK_SCREENSHOT_OPTIONS,
} from './capture-metadata.mjs';

const requireFromWeb = createRequire(new URL('../../client/web/package.json', import.meta.url));
const { chromium } = requireFromWeb('@playwright/test');

// Lay the page out as a real browser and reach 1080p with deviceScaleFactor
// rather than by zooming the arena. The game caps its cell size at 15 CSS px
// (client/web/components/GameArena.tsx:679), so a 640x360 CSS viewport at DSF 3
// renders 1920x1080 device pixels with the arena, boost meter and callouts in
// exactly the proportions the product shows on a large monitor.
const VIEWPORT = { width: 480, height: 270 };
const DEVICE_SCALE_FACTOR = 4;
const OUTPUT_SIZE = {
  width: VIEWPORT.width * DEVICE_SCALE_FACTOR,
  height: VIEWPORT.height * DEVICE_SCALE_FACTOR,
};
const DEFAULT_URL = 'http://127.0.0.1:3000';
const STARTUP_FRAME_INTERVAL_MS = 1000 / 60;
const CAPTURE_FRAME_TIME_BASE_MS = 1_000_000;

function usage(message) {
  if (message) process.stderr.write(`${message}\n\n`);
  process.stderr.write(
    'Usage: node tools/video/capture.mjs --scenario <id> [--url URL] [--out DIR] ' +
    '[--capture-vfps 60] [--duration-ms N] [--headless-shell PATH] [--virtual-time] ' +
    '[--viewer-timing]\n',
  );
  process.exit(message ? 2 : 0);
}

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--help' || token === '-h') usage();
    if (token === '--virtual-time') {
      values.virtualTime = true;
      continue;
    }
    if (token === '--viewer-timing') {
      values.viewerTiming = true;
      continue;
    }
    if (!token.startsWith('--') || index + 1 >= argv.length) usage(`Invalid argument: ${token}`);
    values[token.slice(2)] = argv[index + 1];
    index += 1;
  }
  if (!values.scenario) usage('--scenario is required');
  const captureVfps = Number(values['capture-vfps'] ?? 60);
  if (!Number.isInteger(captureVfps) || captureVfps < 1 || captureVfps > 240) {
    usage('--capture-vfps must be an integer from 1 to 240');
  }
  const durationMs = values['duration-ms'] === undefined ? null : Number(values['duration-ms']);
  if (durationMs !== null && (!Number.isFinite(durationMs) || durationMs <= 0)) {
    usage('--duration-ms must be positive');
  }
  return {
    scenario: values.scenario,
    baseUrl: values.url ?? DEFAULT_URL,
    outDir: resolve(values.out ?? `tools/video/clips/${values.scenario}`),
    captureVfps,
    durationMs,
    executablePath: values['headless-shell'] ?? process.env.SNAKETRON_HEADLESS_SHELL,
    forceVirtualTime: Boolean(values.virtualTime),
    viewerTiming: Boolean(values.viewerTiming),
  };
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

async function sha256File(file) {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(file)) digest.update(chunk);
  return digest.digest('hex');
}

async function resolveCaptureSource(requested) {
  const requestedPath = resolve(requested);
  let sourceJson;
  try {
    sourceJson = await readFile(requestedPath, 'utf8');
  } catch (error) {
    const looksLikeFile = requested.endsWith('.json') || requested.includes('/') || requested.includes('\\');
    if (error?.code !== 'ENOENT' || looksLikeFile) {
      throw new Error(`Cannot read scenario/clip file ${requestedPath}: ${error.message}`);
    }
    return {
      id: requested,
      kind: 'registry',
      injected: null,
      sourceSha256: null,
      sourceFile: null,
      cameraFocusTick: null,
    };
  }

  let parsed;
  try {
    parsed = JSON.parse(sourceJson);
  } catch (error) {
    throw new Error(`Scenario/clip file is not valid JSON (${requestedPath}): ${error.message}`);
  }
  const isHighlight = Number.isInteger(parsed?.clip_format_version) &&
    parsed?.anchor && parsed?.window && Array.isArray(parsed?.messages);
  const isScenario = Number.isInteger(parsed?.format_version) &&
    typeof parsed?.id === 'string' && parsed?.world && parsed?.pose;
  if (!isHighlight && !isScenario) {
    throw new Error(
      `Unsupported capture JSON ${requestedPath}: expected ScenarioScript or HighlightClip`,
    );
  }

  const kind = isHighlight ? 'highlight' : 'script';
  return {
    id: isHighlight ? `highlight-${parsed.game_id}` : parsed.id,
    kind: `${kind}-file`,
    injected: { kind, json: sourceJson },
    sourceSha256: sha256(sourceJson),
    sourceFile: basename(requestedPath),
    cameraFocusTick: isHighlight ? parsed.window.focus_tick : null,
  };
}

async function writePipe(stream, bytes) {
  if (!stream.write(bytes)) await once(stream, 'drain');
}

function startEncoder(output, fps) {
  const args = [
    '-hide_banner', '-loglevel', 'error', '-y',
    '-fflags', '+bitexact',
    '-f', 'image2pipe', '-framerate', String(fps), '-vcodec', 'png', '-i', 'pipe:0',
    '-an', '-bitexact', '-c:v', 'libx264rgb', '-qp', '0', '-preset', 'medium',
    '-flags:v', '+bitexact',
    '-map_metadata', '-1',
    '-metadata', 'snaketron_color_pipeline=lossless-rgb',
    '-f', 'matroska', output,
  ];
  const child = spawn(process.env.FFMPEG ?? 'ffmpeg', args, {
    stdio: ['pipe', 'inherit', 'pipe'],
  });
  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const completion = new Promise((resolvePromise, rejectPromise) => {
    child.once('error', rejectPromise);
    child.once('exit', (code, signal) => {
      if (code === 0) resolvePromise();
      else rejectPromise(new Error(`ffmpeg failed (${signal ?? code}): ${stderr.trim()}`));
    });
  });
  return { child, completion };
}

async function waitForCapturePredicate(page, cdp, predicate, timeoutMs = 30_000) {
  if (!cdp) {
    await page.waitForFunction(predicate, null, { timeout: timeoutMs });
    return;
  }

  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await page.evaluate(predicate)) return;
    // chrome-headless-shell does not run React passive effects or rAF work
    // until the embedder supplies frames. Keep startup externally clocked too.
    cdp.frameTimeTicks += STARTUP_FRAME_INTERVAL_MS;
    await cdp.session.send('HeadlessExperimental.beginFrame', {
      frameTimeTicks: cdp.frameTimeTicks,
      interval: STARTUP_FRAME_INTERVAL_MS,
      noDisplayUpdates: false,
    });
    await page.waitForTimeout(10);
  }
  throw new Error(`timed out after ${timeoutMs}ms`);
}

async function captureContract(page, cdp) {
  try {
    await waitForCapturePredicate(
      page,
      cdp,
      () => Boolean(window.__SNAKETRON_CAPTURE__),
    );
  } catch (error) {
    const diagnosis = await page.evaluate(() => ({
      url: window.location.href,
      ready_state: document.readyState,
      title: document.title,
      root_text: document.getElementById('root')?.textContent?.slice(0, 500) ?? null,
      root_html: document.getElementById('root')?.innerHTML.slice(0, 500) ?? null,
      capture_dataset: document.documentElement.dataset.scenarioCapture ?? null,
      scripts: [...document.scripts].map((script) => script.src || '[inline]'),
    })).catch(() => null);
    throw new Error(
      `Capture contract was not installed within 30s: ${error.message}; ` +
      `page=${JSON.stringify(diagnosis)}`,
    );
  }
  if (cdp) {
    await page.evaluate(() => {
      window.__SNAKETRON_CAPTURE_READY_STATE__ = { status: 'pending', error: null };
      void window.__SNAKETRON_CAPTURE__.ready().then(
        () => { window.__SNAKETRON_CAPTURE_READY_STATE__.status = 'ready'; },
        (error) => {
          window.__SNAKETRON_CAPTURE_READY_STATE__.status = 'error';
          window.__SNAKETRON_CAPTURE_READY_STATE__.error = String(error?.stack ?? error);
        },
      );
    });
    await waitForCapturePredicate(
      page,
      cdp,
      () => window.__SNAKETRON_CAPTURE_READY_STATE__?.status !== 'pending',
    );
    const readyState = await page.evaluate(() => {
      const state = window.__SNAKETRON_CAPTURE_READY_STATE__;
      delete window.__SNAKETRON_CAPTURE_READY_STATE__;
      return state;
    });
    if (readyState?.status !== 'ready') {
      throw new Error(`Capture surface failed to become ready: ${readyState?.error ?? 'unknown'}`);
    }
  } else {
    await page.evaluate(() => window.__SNAKETRON_CAPTURE__.ready());
  }
  const manifest = await page.evaluate(() => ({
    durationMs: window.__SNAKETRON_CAPTURE__.durationMs?.() ?? null,
    viewerDurationMs: window.__SNAKETRON_CAPTURE__.viewerDurationMs?.() ?? null,
    starSnakeId: window.__SNAKETRON_CAPTURE__.starSnakeId?.() ?? null,
    renderedTick: window.__SNAKETRON_CAPTURE__.renderedTick(),
    cueTrack: window.__SNAKETRON_CAPTURE__.cueTrack(),
  }));
  return manifest;
}

const masterSecondsForTick = (cueTrack, tick) => (
  Math.max(0, tick - cueTrack.start_tick) * cueTrack.tick_duration_ms / 1000
);

function buildCueTimeline(cueTrack, durationMs) {
  const timeline = [];
  const add = (type, cue) => timeline.push({
    type,
    at_master_seconds: masterSecondsForTick(cueTrack, cue.tick),
    ...cue,
  });
  for (const cue of cueTrack.crashes ?? []) add('crash', cue);
  for (const cue of cueTrack.goals ?? []) add('bank', cue);
  for (const cue of cueTrack.pickups ?? []) add('pickup', cue);
  for (const cue of cueTrack.deaths ?? []) add('death', cue);
  timeline.sort((left, right) => (
    left.at_master_seconds - right.at_master_seconds ||
    (left.sequence ?? 0) - (right.sequence ?? 0) ||
    left.type.localeCompare(right.type)
  ));
  return timeline.filter((cue) => cue.at_master_seconds * 1000 <= durationMs);
}

function killerSnakeId(cause) {
  return cause?.SnakeBody?.killer_snake_id ?? cause?.HeadToHead?.other_snake_id ?? null;
}

function buildNormalizedAnchors(cueTrack, starSnakeId, durationMs) {
  if (!Number.isInteger(starSnakeId) || starSnakeId < 0) return {};
  const timestamp = (cue) => masterSecondsForTick(cueTrack, cue.tick);
  const withinMaster = (cue) => timestamp(cue) * 1000 <= durationMs;
  const firstByTick = (cues) => [...cues]
    .filter(withinMaster)
    .sort((left, right) => (
      left.tick - right.tick || (left.sequence ?? 0) - (right.sequence ?? 0)
    ))[0];

  const kill = firstByTick((cueTrack.deaths ?? []).filter((death) => (
    death.snake_id !== starSnakeId && killerSnakeId(death.cause) === starSnakeId
  )));
  const bank = firstByTick((cueTrack.goals ?? []).filter((goal) => (
    goal.snake_id === starSnakeId
  )));
  const combo = firstByTick((cueTrack.pickups ?? []).filter((pickup) => (
    pickup.snake_id === starSnakeId && pickup.combo_chain >= 2
  )));
  const boost = firstByTick((cueTrack.pickups ?? []).filter((pickup) => (
    pickup.snake_id === starSnakeId && pickup.boost_active === true
  )));

  return Object.fromEntries([
    ['kill', kill],
    ['bank', bank],
    ['combo', combo],
    ['boost', boost],
  ].flatMap(([name, cue]) => cue ? [[name, timestamp(cue)]] : []));
}

async function beginFrameSession(page, forceFallback, explicitHeadlessShell) {
  if (forceFallback) return null;
  const session = await page.context().newCDPSession(page);
  try {
    await session.send('HeadlessExperimental.enable');
    const probe = await session.send('HeadlessExperimental.beginFrame', {
      frameTimeTicks: 0,
      noDisplayUpdates: false,
      screenshot: { format: 'png' },
    });
    if (!probe.screenshotData) throw new Error('BeginFrame returned no screenshot');
    return { session, frameTimeTicks: 0 };
  } catch (error) {
    await session.detach().catch(() => {});
    if (explicitHeadlessShell) {
      throw new Error(
        `Pinned headless-shell does not expose HeadlessExperimental.beginFrame: ${error.message}. ` +
        'Install it with `npx playwright install chromium-headless-shell` and pass its executable path.',
      );
    }
    return null;
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const captureSource = await resolveCaptureSource(options.scenario);
  await mkdir(options.outDir, { recursive: true });
  const master = resolve(options.outDir, 'master.mkv');
  const temporaryMaster = `${master}.partial`;
  await rm(temporaryMaster, { force: true });
  const launchOptions = {
    headless: true,
    executablePath: options.executablePath,
    args: [...CAPTURE_BROWSER_ARGS],
  };
  const browser = await chromium.launch(launchOptions);
  try {
    const context = await browser.newContext({
      viewport: VIEWPORT,
      deviceScaleFactor: DEVICE_SCALE_FACTOR,
      // SnakeTron renders light; capturing under a dark preference invites a
      // component to show a theme the trailer never advertises.
      colorScheme: 'light',
      reducedMotion: 'no-preference',
    });
    const page = await context.newPage();
    const appOrigin = new URL(options.baseUrl).origin;
    page.on('console', (message) => {
      if (
        message.type() === 'error' &&
        !message.text().includes('net::ERR_BLOCKED_BY_CLIENT')
      ) {
        process.stderr.write(`browser console: ${message.text()}\n`);
      }
    });
    page.on('pageerror', (error) => {
      process.stderr.write(`browser page error: ${error.stack ?? error.message}\n`);
    });
    page.on('response', (response) => {
      if (response.status() >= 400 && new URL(response.url()).origin === appOrigin) {
        process.stderr.write(`browser response ${response.status()}: ${response.url()}\n`);
      }
    });
    page.on('requestfailed', (request) => {
      if (new URL(request.url()).origin === appOrigin) {
        process.stderr.write(
          `browser request failed: ${request.url()} (${request.failure()?.errorText ?? 'unknown'})\n`,
        );
      }
    });
    if (captureSource.injected) {
      await page.addInitScript(({ kind, json }) => {
        window.__SNAKETRON_CAPTURE_SOURCE__ = kind === 'highlight'
          ? { kind, clip: json }
          : { kind, script: json };
      }, captureSource.injected);
    }
    await page.route('**/*', async (route) => {
      const url = new URL(route.request().url());
      if (url.origin === appOrigin || url.protocol === 'data:' || url.protocol === 'blob:') {
        await route.continue();
      } else {
        await route.abort('blockedbyclient');
      }
    });
    const target = new URL('/qa/scenario-player', options.baseUrl);
    target.searchParams.set('capture', '1');
    target.searchParams.set('scenario', captureSource.id);
    target.searchParams.set('autoplay', '0');
    await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    // In chrome-headless-shell, compositor work is externally clocked. The
    // probe frame must happen before waiting for React passive effects, or the
    // capture API's effect cannot install itself and readiness deadlocks.
    const cdp = await beginFrameSession(
      page,
      options.forceVirtualTime,
      Boolean(options.executablePath),
    );
    const initial = await captureContract(page, cdp);
    const authoredDurationMs = options.viewerTiming
      ? initial.viewerDurationMs
      : initial.durationMs;
    const durationMs = options.durationMs ?? authoredDurationMs;
    if (!Number.isFinite(durationMs) || durationMs <= 0) {
      throw new Error('Capture route did not expose a positive durationMs; pass --duration-ms');
    }

    const fallbackCdp = cdp ? null : await page.context().newCDPSession(page);
    const capturePath = cdp ? 'beginframe' : 'virtualtime';
    const frameMs = 1000 / options.captureVfps;
    const frameCount = Math.ceil(durationMs / frameMs);
    const progressEvery = Math.max(1, Math.floor(frameCount / 10));
    const captureFrameTimeBase = cdp
      ? Math.max(CAPTURE_FRAME_TIME_BASE_MS, cdp.frameTimeTicks)
      : 0;
    const encoder = startEncoder(temporaryMaster, options.captureVfps);
    try {
      for (let frame = 0; frame < frameCount; frame += 1) {
        await page.evaluate(({ dt, viewerTiming }) => (
          viewerTiming
            ? window.__SNAKETRON_CAPTURE__.stepViewerMs(dt)
            : window.__SNAKETRON_CAPTURE__.stepMs(dt)
        ), { dt: frameMs, viewerTiming: options.viewerTiming });
        let png;
        if (cdp) {
          const result = await cdp.session.send('HeadlessExperimental.beginFrame', {
            frameTimeTicks: captureFrameTimeBase + (frame + 1) * frameMs,
            interval: frameMs,
            noDisplayUpdates: false,
            screenshot: { format: 'png' },
          });
          if (!result.screenshotData) throw new Error(`BeginFrame ${frame} returned no pixels`);
          png = Buffer.from(result.screenshotData, 'base64');
        } else {
          // The fallback is intentionally labeled perceptual-only in meta.json.
          await fallbackCdp.send('Emulation.setVirtualTimePolicy', {
            policy: 'advance',
            budget: frameMs,
            maxVirtualTimeTaskStarvationCount: 10_000,
          }).catch(() => {});
          // Virtual time already advances CSS animations by exactly frameMs.
          // Disabling animations here would fast-forward finite effects and make
          // fallback frames diverge from the BeginFrame capture path.
          png = await page.screenshot(FALLBACK_SCREENSHOT_OPTIONS);
        }
        await writePipe(encoder.child.stdin, png);
        if ((frame + 1) % progressEvery === 0 || frame + 1 === frameCount) {
          process.stderr.write(`captured ${frame + 1}/${frameCount} frames\n`);
        }
      }
      encoder.child.stdin.end();
      await encoder.completion;
    } catch (error) {
      encoder.child.stdin.destroy();
      encoder.child.kill('SIGTERM');
      await encoder.completion.catch(() => {});
      await rm(temporaryMaster, { force: true });
      throw error;
    }
    await fallbackCdp?.detach().catch(() => {});
    await rename(temporaryMaster, master);

    const finalCapture = await page.evaluate(() => ({
      renderedTick: window.__SNAKETRON_CAPTURE__.renderedTick(),
      cueTrack: window.__SNAKETRON_CAPTURE__.cueTrack(),
    }));
    const sourceCueTimeline = buildCueTimeline(finalCapture.cueTrack, initial.durationMs);
    const sourceAnchors = buildNormalizedAnchors(
      finalCapture.cueTrack,
      initial.starSnakeId,
      initial.durationMs,
    );
    const focusSourceMs = Number.isInteger(captureSource.cameraFocusTick)
      ? Math.max(
        0,
        captureSource.cameraFocusTick - finalCapture.cueTrack.start_tick,
      ) * finalCapture.cueTrack.tick_duration_ms
      : null;
    const projection = options.viewerTiming
      ? await page.evaluate(({
        anchors,
        cueTimeline,
        focusMs,
        count,
        millisecondsPerFrame,
      }) => {
        const capture = window.__SNAKETRON_CAPTURE__;
        return {
          anchors: Object.fromEntries(Object.entries(anchors).map(([name, seconds]) => [
            name,
            capture.viewerMsForSourceMs(Number(seconds) * 1000) / 1000,
          ])),
          cueTimeline: cueTimeline.map((cue) => ({
            ...cue,
            at_master_seconds: capture.viewerMsForSourceMs(
              cue.at_master_seconds * 1000,
            ) / 1000,
          })),
          focusMasterSeconds: focusMs === null
            ? null
            : capture.viewerMsForSourceMs(focusMs) / 1000,
          virtualFrameTimesMs: Array.from(
            { length: count },
            (_, frame) => capture.sourceMsForViewerMs((frame + 1) * millisecondsPerFrame),
          ),
        };
      }, {
        anchors: sourceAnchors,
        cueTimeline: sourceCueTimeline,
        focusMs: focusSourceMs,
        count: frameCount,
        millisecondsPerFrame: frameMs,
      })
      : {
        anchors: sourceAnchors,
        cueTimeline: sourceCueTimeline,
        focusMasterSeconds: focusSourceMs === null ? null : focusSourceMs / 1000,
        virtualFrameTimesMs: null,
      };
    const anchors = Object.fromEntries(
      Object.entries(projection.anchors).filter(([, seconds]) => (
        Number(seconds) * 1000 <= durationMs
      )),
    );
    const cueTimeline = projection.cueTimeline.filter((cue) => (
      cue.at_master_seconds * 1000 <= durationMs
    ));
    const meta = {
      schema_version: 1,
      scenario_id: captureSource.id,
      source_kind: captureSource.kind,
      source_file: captureSource.sourceFile,
      source_sha256: captureSource.sourceSha256,
      capture_vfps: options.captureVfps,
      encoded_fps: options.captureVfps,
      width: OUTPUT_SIZE.width,
      height: OUTPUT_SIZE.height,
      duration: durationMs / 1000,
      duration_ms: durationMs,
      scenario_duration_ms: initial.durationMs,
      viewer_duration_ms: initial.viewerDurationMs,
      timing_mode: options.viewerTiming ? 'viewer' : 'source',
      frame_count: frameCount,
      capture_path: capturePath,
      deterministic_scope: cdp ? 'same-machine-software-raster' : 'perceptual-only',
      rendered_tick: finalCapture.renderedTick,
      star_snake_id: initial.starSnakeId,
      camera_focus_tick: captureSource.cameraFocusTick,
      camera_focus_master_seconds: projection.focusMasterSeconds,
      anchors,
      cue_track: finalCapture.cueTrack,
      cue_timeline: cueTimeline,
      star_head_frames: buildStarHeadFrames(
        finalCapture.cueTrack,
        initial.starSnakeId,
        frameCount,
        frameMs,
        initial.durationMs,
        captureSource.cameraFocusTick,
        projection.virtualFrameTimesMs,
      ),
      master_sha256: await sha256File(master),
      master_file: basename(master),
    };
    const metaFile = resolve(options.outDir, 'meta.json');
    await writeFile(metaFile, `${JSON.stringify(meta, null, 2)}\n`);
    process.stdout.write(`${JSON.stringify({
      scenario_id: meta.scenario_id,
      capture_path: meta.capture_path,
      deterministic_scope: meta.deterministic_scope,
      frame_count: meta.frame_count,
      duration_ms: meta.duration_ms,
      master_sha256: meta.master_sha256,
      master_file: master,
      meta_file: metaFile,
    }, null, 2)}\n`);
  } finally {
    await browser.close();
  }
}

main().catch(async (error) => {
  process.stderr.write(`capture failed: ${error.stack ?? error}\n`);
  process.exitCode = 1;
});
