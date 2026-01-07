const { test, expect } = require('@playwright/test');
const { TestEnvironment } = require('../e2e/fixtures/test-environment.js');
const { WebSocketMonitor } = require('../e2e/fixtures/helpers/websocket-monitor.js');

test.describe('WebSocket Connection', () => {
  let env;

  test.beforeAll(async () => {
    env = new TestEnvironment();
    await env.setup();
  });

  test.afterAll(async () => {
    await env.teardown();
  });

  test('client connects to WebSocket server on app load', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app
    await page.goto(env.getAppUrl());
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    
    // Verify connection state
    const connectionState = await wsMonitor.getConnectionState();
    expect(connectionState).toBe('connected');
  });

  test('client can send and receive ping/pong messages', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    await page.goto(env.getAppUrl());
    await wsMonitor.waitForConnection();
    
    // Send a ping message with timestamp for clock sync
    await page.evaluate(() => {
      // Access the WebSocket connection through the React context
      const wsContext = window.__wsContext;
      if (wsContext && wsContext.sendMessage) {
        wsContext.sendMessage({
          Ping: { client_time: Date.now() }
        });
      }
    });
    
    // Wait for pong response
    const pongMessage = await wsMonitor.waitForMessage('Pong', 'received', 5000);
    expect(pongMessage).toBeTruthy();
    expect(pongMessage.messageType).toBe('Pong');
    expect(pongMessage.parsed).toHaveProperty('Pong');
    expect(pongMessage.parsed.Pong).toMatchObject({
      client_time: expect.any(Number),
      server_time: expect.any(Number)
    });
  });

  test('WebSocket reconnects after disconnection', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    await page.goto(env.getAppUrl());
    await wsMonitor.waitForConnection();
    
    // Get initial connection count
    const initialConnections = await page.evaluate(() => 
      window.__wsMonitor?.connections.length || 0
    );
    
    // Force disconnect by closing the WebSocket
    await page.evaluate(() => {
      const connections = window.__wsMonitor?.connections || [];
      const lastConnection = connections[connections.length - 1];
      if (lastConnection && window.__wsInstance) {
        window.__wsInstance.close();
      }
    });
    
    // Wait a bit for reconnection (auto-reconnect has 2 second delay)
    await page.waitForTimeout(3000);
    
    // Check that a new connection was established
    const newConnections = await page.evaluate(() => 
      window.__wsMonitor?.connections.length || 0
    );
    
    expect(newConnections).toBeGreaterThan(initialConnections);
    
    // Verify new connection is active
    const connectionState = await wsMonitor.getConnectionState();
    expect(connectionState).toBe('connected');
  });
});
