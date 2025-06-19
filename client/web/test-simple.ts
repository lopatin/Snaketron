import { chromium } from '@playwright/test';

async function testWebSocket() {
  console.log('Starting simple WebSocket test...');
  
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();
  
  // Inject WebSocket monitoring
  await page.evaluateOnNewDocument(() => {
    console.log('Injecting WebSocket monitor...');
    window.__wsMonitor = {
      state: 'disconnected',
      messages: []
    };
    
    const OriginalWebSocket = window.WebSocket;
    window.WebSocket = class extends OriginalWebSocket {
      constructor(...args) {
        super(...args);
        console.log('WebSocket created:', args[0]);
        
        this.addEventListener('open', () => {
          console.log('WebSocket opened');
          window.__wsMonitor.state = 'connected';
        });
        
        this.addEventListener('message', (event) => {
          console.log('WebSocket message:', event.data);
          window.__wsMonitor.messages.push(event.data);
        });
        
        this.addEventListener('error', (error) => {
          console.error('WebSocket error:', error);
          window.__wsMonitor.state = 'error';
        });
        
        this.addEventListener('close', () => {
          console.log('WebSocket closed');
          window.__wsMonitor.state = 'disconnected';
        });
      }
    };
  });
  
  console.log('Navigating to http://localhost:3000...');
  await page.goto('http://localhost:3000');
  
  // Wait a bit for WebSocket to connect
  await page.waitForTimeout(3000);
  
  // Check WebSocket state
  const wsState = await page.evaluate(() => window.__wsMonitor?.state);
  console.log('WebSocket state:', wsState);
  
  const messages = await page.evaluate(() => window.__wsMonitor?.messages || []);
  console.log('WebSocket messages:', messages);
  
  // Check console logs
  page.on('console', msg => console.log('Browser console:', msg.text()));
  
  await page.waitForTimeout(5000);
  
  await browser.close();
}

testWebSocket().catch(console.error);