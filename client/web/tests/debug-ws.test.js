const { test, expect } = require('@playwright/test');

test('debug WebSocket connection', async ({ page }) => {
  // Add console listener
  page.on('console', msg => console.log('Browser console:', msg.text()));
  page.on('pageerror', err => console.log('Page error:', err.message));
  
  // Navigate to app
  await page.goto('http://localhost:3000');
  
  // Wait for page to load
  await page.waitForSelector('img[alt="Snaketron"]');
  
  // Check if WebSocket exists in window
  const hasWebSocket = await page.evaluate(() => {
    return typeof window.WebSocket !== 'undefined';
  });
  console.log('WebSocket API available:', hasWebSocket);
  
  // Try to manually create a WebSocket
  const wsState = await page.evaluate(() => {
    try {
      const ws = new WebSocket('ws://localhost:8080/ws');
      return new Promise((resolve) => {
        ws.onopen = () => resolve('connected');
        ws.onerror = (err) => resolve('error: ' + err.toString());
        ws.onclose = () => resolve('closed');
        setTimeout(() => resolve('timeout'), 5000);
      });
    } catch (err) {
      return 'exception: ' + err.toString();
    }
  });
  
  console.log('Manual WebSocket test result:', wsState);
  
  // Check if the app created any WebSocket connections
  const wsConnections = await page.evaluate(() => {
    // Check for React context
    const reactRoot = document.getElementById('root');
    return {
      hasReactRoot: !!reactRoot,
      wsContext: typeof window.__wsContext !== 'undefined',
      wsInstance: typeof window.__wsInstance !== 'undefined'
    };
  });
  
  console.log('App WebSocket state:', wsConnections);
  
  // Wait a bit to see console logs
  await page.waitForTimeout(2000);
});