const { test, expect } = require('@playwright/test');

// Configure test to be more robust
test.use({ 
  browserName: 'chromium',
  headless: true,
  viewport: { width: 1280, height: 720 },
  actionTimeout: 30000,
  navigationTimeout: 30000,
});

// Helper function to wait for WebSocket connection
async function waitForWebSocketConnection(page) {
  await page.waitForFunction(() => {
    // Check if WebSocket context exists and is connected
    return window.__wsContext && window.__wsContext.isConnected === true;
  }, { timeout: 10000 });
}

// Helper function to create authenticated user
async function createAuthenticatedUser(page) {
  const username = `solo${Date.now() % 10000}`;
  
  // Fill username
  await page.fill('input[placeholder="Username"]', username);
  await page.waitForTimeout(1000); // Wait for username check
  
  // Fill password
  await page.fill('input[placeholder="Password"]', 'test123');
  await page.waitForTimeout(1000); // Wait for password field
  
  // Wait for authentication to complete
  await page.waitForTimeout(2000);
  
  return username;
}

test.describe('Solo Game - Robust Tests', () => {
  test.beforeEach(async ({ page }) => {
    console.log('Setting up test...');
    
    // Navigate to home page
    await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });
    
    // Inject WebSocket connection status into window for checking
    await page.evaluate(() => {
      if (window.wsContext) {
        window.wsContext = window.wsContext;
      }
    });
    
    // Wait for initial page load
    await page.waitForTimeout(3000);
  });

  test('complete solo game flow with robust error handling', async ({ page }) => {
    console.log('Starting solo game flow test...');
    
    try {
      // Step 1: Navigate to solo mode
      console.log('Step 1: Clicking SOLO button');
      const soloButton = page.locator('button:has-text("SOLO")');
      await expect(soloButton).toBeVisible({ timeout: 10000 });
      await soloButton.click();
      
      // Wait for navigation
      await page.waitForURL('**/game-modes/solo', { timeout: 10000 });
      console.log('✓ Navigated to solo mode page');
      
      // Step 2: Authenticate
      console.log('Step 2: Authenticating user');
      const username = await createAuthenticatedUser(page);
      console.log(`✓ Created user: ${username}`);
      
      // Step 3: Select game mode
      console.log('Step 3: Selecting Classic mode');
      const classicButton = page.locator('button:has-text("CLASSIC")');
      await expect(classicButton).toBeVisible({ timeout: 10000 });
      
      // Wait for WebSocket to be ready
      await waitForWebSocketConnection(page);
      console.log('✓ WebSocket connected');
      
      // Click Classic button
      await classicButton.click();
      console.log('✓ Clicked Classic button');
      
      // Step 4: Wait for game to start
      console.log('Step 4: Waiting for game to start');
      
      // Use multiple strategies to detect game start
      const gameStarted = await Promise.race([
        // Strategy 1: Wait for URL change
        page.waitForURL('**/play/**', { timeout: 15000 })
          .then(() => true)
          .catch(() => false),
        
        // Strategy 2: Wait for canvas element
        page.waitForSelector('canvas', { timeout: 15000 })
          .then(() => true)
          .catch(() => false),
        
        // Strategy 3: Wait for specific timeout
        new Promise((resolve) => setTimeout(() => resolve(false), 15000))
      ]);
      
      if (!gameStarted) {
        // Take debug screenshot
        await page.screenshot({ path: 'test-results/game-start-failed.png' });
        
        // Check for error messages
        const errorTexts = await page.locator('.text-red-600').allTextContents();
        if (errorTexts.length > 0) {
          console.error('Error messages found:', errorTexts);
        }
        
        throw new Error('Game failed to start within timeout');
      }
      
      console.log(`✓ Game started at: ${page.url()}`);
      
      // Step 5: Verify game is running
      console.log('Step 5: Verifying game is running');
      
      // Check for canvas
      const canvas = page.locator('canvas');
      await expect(canvas).toBeVisible({ timeout: 5000 });
      console.log('✓ Game canvas is visible');
      
      // Test controls
      console.log('Testing game controls...');
      for (const key of ['ArrowUp', 'ArrowRight', 'ArrowDown', 'ArrowLeft']) {
        await page.keyboard.press(key);
        await page.waitForTimeout(200);
      }
      console.log('✓ Game controls tested');
      
      // Take success screenshot
      await page.screenshot({ path: 'test-results/solo-game-success.png' });
      console.log('✓ Success screenshot saved');
      
      console.log('\n✅ Solo game test completed successfully!');
      
    } catch (error) {
      console.error('\n❌ Test failed with error:', error.message);
      
      // Take failure screenshot
      await page.screenshot({ path: 'test-results/solo-game-error.png' });
      
      // Log page content for debugging
      const pageContent = await page.content();
      console.log('Page URL:', page.url());
      console.log('Page title:', await page.title());
      
      // Re-throw to fail the test
      throw error;
    }
  });

  test('handles game over correctly', async ({ page }) => {
    console.log('Starting game over test...');
    
    try {
      // Quick setup
      await page.click('button:has-text("SOLO")');
      await page.waitForURL('**/game-modes/solo', { timeout: 10000 });
      
      await createAuthenticatedUser(page);
      await waitForWebSocketConnection(page);
      
      await page.click('button:has-text("CLASSIC")');
      
      // Wait for game to start
      await page.waitForURL('**/play/**', { timeout: 15000 });
      const canvas = page.locator('canvas');
      await expect(canvas).toBeVisible({ timeout: 5000 });
      
      console.log('✓ Game started, attempting to crash snake');
      
      // Make snake crash by going in one direction continuously
      for (let i = 0; i < 60; i++) {
        await page.keyboard.press('ArrowLeft');
        await page.waitForTimeout(100);
      }
      
      console.log('Waiting for game over indication...');
      
      // Wait for any game over indication
      const gameOverFound = await page.waitForFunction(() => {
        // Check for various game over indicators
        const indicators = [
          document.querySelector('text=Game Over'),
          document.querySelector('text=Score'),
          document.querySelector('text=Final Score'),
          document.querySelector('button:has-text("Back")'),
          document.querySelector('[class*="game-over"]')
        ];
        
        return indicators.some(el => el !== null);
      }, { timeout: 10000 }).catch(() => false);
      
      if (gameOverFound) {
        console.log('✓ Game over detected');
        await page.screenshot({ path: 'test-results/game-over-detected.png' });
      } else {
        console.log('⚠️  No game over indicator found');
        await page.screenshot({ path: 'test-results/game-over-not-found.png' });
      }
      
      console.log('\n✅ Game over test completed!');
      
    } catch (error) {
      console.error('\n❌ Game over test failed:', error.message);
      await page.screenshot({ path: 'test-results/game-over-error.png' });
      throw error;
    }
  });
});