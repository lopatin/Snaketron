const { defineConfig, devices } = require('@playwright/test');

/**
 * The skin pixel suite — `specs/skin-shading-prd.md` Gate 2.
 *
 * Separate from `playwright.config.js` on purpose. That config runs headed,
 * across three browsers, against a manually-managed server, because it is
 * driving real gameplay. None of that is right here: a pixel comparison wants
 * exactly one deterministic renderer, and comparing Chromium's Skia against
 * WebKit's would produce differences that mean nothing about the code.
 *
 *   npm run test:skins            # compare against committed baselines
 *   SKIN_BASELINE_BLESS=1 \
 *     npm run test:skins          # re-record them; the diff is the review
 */
/**
 * Deliberately not 3000.
 *
 * Several worktrees run a dev server, they all default to 3000, and whichever
 * bound `127.0.0.1` most specifically wins — so a suite that reused "whatever
 * is on 3000" could silently measure a different branch's bundle and report the
 * difference as a pixel regression here. Its own port, and never reused.
 */
const SKIN_QA_PORT = 3210;

module.exports = defineConfig({
  testDir: './tests/skins',
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  // A pixel comparison that passes on retry was flaky, which means the number
  // it reports is not a fact. Never retry.
  retries: 0,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: `http://localhost:${SKIN_QA_PORT}`,
    headless: true,
    // Device pixel ratio 1: the arena canvas is not DPR-scaled, so baselines
    // captured at 2x would describe pixels no player sees.
    deviceScaleFactor: 1,
    viewport: { width: 1280, height: 900 },
    trace: 'off',
    screenshot: 'off',
    video: 'off',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: `npx webpack-dev-server --port ${SKIN_QA_PORT}`,
    port: SKIN_QA_PORT,
    reuseExistingServer: false,
    timeout: 180 * 1000,
  },
  timeout: 120 * 1000,
  expect: { timeout: 10 * 1000 },
});
