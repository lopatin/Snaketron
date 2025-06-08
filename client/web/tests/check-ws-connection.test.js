const { test, expect } = require('@playwright/test');

test('check WebSocket connection URL', async ({ page }) => {
  // Add console listener
  page.on('console', msg => {
    if (msg.text().includes('WebSocket')) {
      console.log('Browser console:', msg.text());
    }
  });
  
  // Navigate to app
  await page.goto('http://localhost:3000');
  
  // Wait for page to load
  await page.waitForSelector('img[alt="Snaketron"]');
  
  // Force reload to ensure latest code
  await page.reload();
  
  // Wait a bit for WebSocket to connect
  await page.waitForTimeout(2000);
  
  // Check what URL the WebSocket is using
  const wsInfo = await page.evaluate(() => {
    if (window.__wsInstance) {
      return {
        url: window.__wsInstance.url,
        readyState: window.__wsInstance.readyState,
        readyStateText: ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][window.__wsInstance.readyState]
      };
    }
    return null;
  });
  
  console.log('WebSocket info:', wsInfo);
  
  // The URL should be port 8080
  expect(wsInfo).not.toBeNull();
  expect(wsInfo.url).toBe('ws://localhost:8080/ws');
  expect(wsInfo.readyStateText).toBe('OPEN');
});