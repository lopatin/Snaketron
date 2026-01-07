const { test, expect } = require('@playwright/test');

test.use({ 
  headless: true,
  screenshot: 'only-on-failure',
  video: 'retain-on-failure'
});

test('solo game basic flow', async ({ page }) => {
  // Wait for server to be ready
  await page.waitForTimeout(3000);
  
  // Go to home page
  await page.goto('http://localhost:3000');
  
  // Wait for WebSocket connection
  await page.waitForTimeout(2000);
  
  // Click Solo button
  await page.click('button:has-text("SOLO")');
  
  // Verify we're on solo game mode page
  await expect(page).toHaveURL(/\/game-modes\/solo/);
  
  // Create a short username to avoid length issues
  const username = `u${Date.now() % 1000}`;
  await page.fill('input[placeholder="Username"]', username);
  await page.waitForTimeout(1500); // Wait for username check
  
  await page.fill('input[placeholder="Password"]', 'pass123');
  await page.waitForTimeout(500);
  
  // Click Classic button
  await page.click('button:has-text("CLASSIC")');
  
  // Wait for possible navigation or error
  await page.waitForTimeout(3000);
  
  // Check if we got an error or navigated
  const currentUrl = page.url();
  console.log('Current URL after clicking Classic:', currentUrl);
  
  // Check for any error messages
  const errorMessages = await page.locator('.text-red-600').allTextContents();
  if (errorMessages.length > 0) {
    console.log('Error messages found:', errorMessages);
  }
  
  // Check if we're still on the same page
  if (currentUrl.includes('/game-modes/solo')) {
    // We didn't navigate, something went wrong
    await page.screenshot({ path: 'test-results/solo-game-failed.png' });
    throw new Error('Failed to create solo game - no navigation occurred');
  }
  
  // If we navigated to /play/:id, verify the game arena
  if (currentUrl.match(/\/play\/\d+/)) {
    console.log('Successfully navigated to game arena');
    
    // Wait for canvas to load
    const canvas = page.locator('canvas');
    await expect(canvas).toBeVisible({ timeout: 5000 });
    
    // Test keyboard input
    await page.keyboard.press('ArrowUp');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowRight');
    
    console.log('Game controls tested successfully');
  }
});