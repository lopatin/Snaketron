const { defineConfig, devices } = require('@playwright/test');

const canaryDurationMs = Number.parseInt(
  process.env.SNAKETRON_STRESS_CANARY_DURATION_MS || '600000',
  10,
);

if (!Number.isSafeInteger(canaryDurationMs) || canaryDurationMs < 90_000) {
  throw new Error('SNAKETRON_STRESS_CANARY_DURATION_MS must be at least 90000');
}

module.exports = defineConfig({
  testDir: './tests/e2e/specs',
  testMatch: 'stress-canary.spec.js',
  fullyParallel: false,
  forbidOnly: true,
  workers: 1,
  retries: 0,
  reporter: [['list'], ['json', { outputFile: 'test-results/stress-canary.json' }]],
  timeout: canaryDurationMs + 5 * 60_000,
  expect: { timeout: 15_000 },
  use: {
    baseURL: 'http://127.0.0.1:3101',
    headless: true,
    // Guest bootstrap carries the stable derived stress admission header.
    // Never persist Playwright network traces for this live-target canary.
    trace: 'off',
    viewport: { width: 1280, height: 720 },
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'npx webpack serve --config webpack.config.js --host 127.0.0.1 --port 3101',
    url: 'http://127.0.0.1:3101',
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
