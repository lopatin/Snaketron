const { test, expect } = require('@playwright/test');
const { WebSocketMonitor } = require('./e2e/fixtures/helpers/websocket-monitor.js');

test('debug WebSocket messages during game creation', async ({ page }) => {
  const wsMonitor = new WebSocketMonitor(page);
  await wsMonitor.setup();
  
  // Add console listener
  page.on('console', msg => {
    if (msg.text().includes('WebSocket') || msg.text().includes('message')) {
      console.log('Browser:', msg.text());
    }
  });
  
  await page.goto('http://localhost:3000');
  await wsMonitor.waitForConnection();
  
  // Navigate to custom game
  await page.click('[data-testid="custom-game-button"]');
  await page.waitForURL(/\/custom$/);
  
  // Create game
  await page.click('[data-testid="create-game-button"]');
  
  // Wait a bit to see what happens
  await page.waitForTimeout(3000);
  
  // Get all messages
  const messages = await wsMonitor.getAllMessages();
  console.log('\n=== All WebSocket Messages ===');
  console.log('Sent:', JSON.stringify(messages.sent, null, 2));
  console.log('Received:', JSON.stringify(messages.received, null, 2));
});