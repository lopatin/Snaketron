const { test, expect } = require('@playwright/test');

// Run tests in serial mode to avoid conflicts
test.describe.serial('Solo Game Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to home page
    await page.goto('http://localhost:3000');
    
    // Wait for page to load and WebSocket to connect
    await page.waitForTimeout(2000);
  });

  test('should create and play solo game', async ({ page }) => {
    // Click Solo button
    await page.click('button:has-text("SOLO")');
    
    // Verify we're on solo game mode page
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    // Create a unique username
    const username = `u${Math.floor(Math.random() * 10000)}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1500); // Wait for username check
    
    await page.fill('input[placeholder="Password"]', 'test123');
    await page.waitForTimeout(500);
    
    // Click Classic mode
    await page.click('button:has-text("CLASSIC")');
    
    // Wait for navigation to game arena
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // Verify canvas is visible
    const canvas = page.locator('canvas');
    await expect(canvas).toBeVisible();
    
    // Test keyboard controls
    await page.keyboard.press('ArrowUp');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowRight');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowDown');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowLeft');
    
    // Verify we're still in the game
    await expect(page).toHaveURL(/\/play\/\d+/);
  });

  test('should show game over when snake crashes into wall', async ({ page }) => {
    // Quick game setup
    await page.click('button:has-text("SOLO")');
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    const username = `u${Math.floor(Math.random() * 10000)}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1500);
    await page.fill('input[placeholder="Password"]', 'test123');
    await page.waitForTimeout(500);
    await page.click('button:has-text("CLASSIC")');
    
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // Make the snake crash into the wall by going in one direction continuously
    // The snake starts in the middle, so going left continuously will hit the wall
    for (let i = 0; i < 50; i++) {
      await page.keyboard.press('ArrowLeft');
      await page.waitForTimeout(50);
    }
    
    // Wait for game over
    // Look for any indication that the game has ended
    // This might be a "Game Over" text, a score display, or a button to restart
    const gameEndedIndicators = [
      page.locator('text=Game Over'),
      page.locator('text=Score:'),
      page.locator('button:has-text("Back to Menu")'),
      page.locator('text=Final Score')
    ];
    
    let gameEnded = false;
    for (const indicator of gameEndedIndicators) {
      try {
        await indicator.waitFor({ timeout: 5000, state: 'visible' });
        gameEnded = true;
        break;
      } catch {
        // Continue to next indicator
      }
    }
    
    expect(gameEnded).toBe(true);
  });

  test('should allow navigation back to menu', async ({ page }) => {
    // Quick game setup
    await page.click('button:has-text("SOLO")');
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    const username = `u${Math.floor(Math.random() * 10000)}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1500);
    await page.fill('input[placeholder="Password"]', 'test123');
    await page.waitForTimeout(500);
    await page.click('button:has-text("CLASSIC")');
    
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // For now, just verify we can navigate away from the game
    // by using browser back or clicking the logo
    const logo = page.locator('img[alt="Snaketron"]');
    if (await logo.isVisible()) {
      await logo.click();
      await expect(page).toHaveURL('http://localhost:3000/');
    } else {
      // Use browser back
      await page.goBack();
      await page.waitForTimeout(1000);
      await page.goBack();
      await expect(page).toHaveURL('http://localhost:3000/');
    }
    
    // Verify we're back on home page
    await expect(page.locator('button:has-text("SOLO")')).toBeVisible();
  });
});