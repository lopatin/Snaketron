const { test, expect } = require('@playwright/test');
const { WebSocketMonitor } = require('../e2e/fixtures/helpers/websocket-monitor.js');

test.describe('WebSocket Connection', () => {
  test('client connects to WebSocket server on app load', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app
    await page.goto('http://localhost:3000');
    
    // Wait for page to load
    await page.waitForSelector('img[alt="Snaketron"]');
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    
    // Verify connection state
    const connectionState = await wsMonitor.getConnectionState();
    expect(connectionState).toBe('connected');
  });

  test('WebSocket connects to correct URL', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    await page.goto('http://localhost:3000');
    await wsMonitor.waitForConnection();
    
    // Check connection details
    const connections = await page.evaluate(() => window.__wsMonitor?.connections || []);
    expect(connections.length).toBeGreaterThan(0);
    // The last connection should be to port 8080 (after reconnects)
    const lastConnection = connections[connections.length - 1];
    expect(lastConnection.url).toBe('ws://localhost:8080/ws');
  });
});