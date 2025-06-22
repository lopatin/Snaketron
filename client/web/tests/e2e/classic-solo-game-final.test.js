const { test, expect } = require('@playwright/test');
const { WebSocketMonitor } = require('./fixtures/helpers/websocket-monitor.js');

test.describe('Classic Solo Game', () => {
  test('user can play classic solo game until game over', async ({ page }) => {
    // Set up WebSocket monitoring
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app
    await page.goto('http://localhost:3000');
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    console.log('WebSocket connected');
    
    // Click the SOLO button
    await page.click('button:has-text("SOLO")');
    console.log('Clicked SOLO button');
    
    // Wait for navigation to game modes
    await page.waitForURL('**/game-modes/solo');
    console.log('Navigated to solo game modes');
    
    // Use a simple username that's less likely to exist
    const timestamp = Date.now();
    const username = `solo${timestamp}`;
    const password = 'test123';
    
    // Enter username
    await page.fill('input[placeholder="Username"]', username);
    console.log('Entered username:', username);
    
    // Wait for username availability check
    await page.waitForTimeout(1500); // Give it time to check
    
    // Check what the auth message says
    const authMessageElement = await page.locator('.auth-message').first();
    const authMessage = await authMessageElement.textContent();
    console.log('Auth message:', authMessage);
    
    // Enter password regardless of whether user exists or not
    await page.fill('input[placeholder="Password"]', password);
    console.log('Entered password');
    
    // Clear any existing error by waiting
    await page.waitForTimeout(500);
    
    // Click the CLASSIC button to start the game
    console.log('Clicking CLASSIC button...');
    await page.click('button:has-text("CLASSIC")');
    
    // Wait for navigation to game - be more patient
    console.log('Waiting for navigation to game...');
    
    try {
      // Wait for either game creation or navigation
      await page.waitForURL('**/play/*', { timeout: 15000 });
      console.log('Successfully navigated to game');
    } catch (err) {
      // If navigation failed, check for error messages
      const errorVisible = await page.locator('text=Invalid username or password').isVisible();
      if (errorVisible) {
        console.log('Authentication failed - trying with different credentials');
        
        // Try again with a different approach - use a known good username pattern
        const newUsername = `player${timestamp}`;
        await page.fill('input[placeholder="Username"]', newUsername);
        await page.waitForTimeout(1500);
        await page.fill('input[placeholder="Password"]', password);
        await page.waitForTimeout(500);
        await page.click('button:has-text("CLASSIC")');
        
        // Wait for navigation again
        await page.waitForURL('**/play/*', { timeout: 10000 });
      } else {
        throw err;
      }
    }
    
    // Get game ID from URL
    const url = page.url();
    const gameId = url.split('/play/')[1];
    console.log('Game ID:', gameId);
    
    // Wait for game canvas to be visible
    await page.waitForSelector('canvas', { state: 'visible', timeout: 10000 });
    console.log('Canvas is visible');
    
    // Wait for game to initialize
    await page.waitForTimeout(2000);
    
    // Check if game is rendering by looking for the canvas content
    const canvasRect = await page.locator('canvas').boundingBox();
    console.log('Canvas dimensions:', canvasRect);
    
    // Wait a bit before turning
    await page.waitForTimeout(2000); // 10 ticks
    
    // Turn the snake down
    await page.keyboard.press('ArrowDown');
    console.log('Pressed ArrowDown');
    
    // Wait for the snake to crash into the wall
    // The snake starts at y=20 moving right, after turning down it needs to travel 20 cells
    // At 5 ticks per second, this should take about 4 seconds
    console.log('Waiting for game over...');
    
    // Wait for game over modal with longer timeout
    const gameOverModal = await page.waitForSelector('div:has(h2:has-text("Game Over!"))', {
      state: 'visible',
      timeout: 15000
    });
    
    expect(gameOverModal).toBeTruthy();
    console.log('Game over modal appeared!');
    
    // Verify modal content
    await expect(page.locator('h2:has-text("Game Over!")')).toBeVisible();
    
    // Check for final score
    const scoreText = await page.locator('p:has-text("Final Score:")').textContent();
    expect(scoreText).toMatch(/Final Score: \d+/);
    console.log('Score:', scoreText);
    
    // Verify Play Again button exists
    await expect(page.locator('button:has-text("Play Again")')).toBeVisible();
    
    // Click Play Again to return to main menu
    await page.click('button:has-text("Play Again")');
    await page.waitForURL('**/', { timeout: 5000 });
    
    console.log('Test completed successfully!');
  });
});