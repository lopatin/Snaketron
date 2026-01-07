const { test, expect } = require('@playwright/test');

test.describe('Solo Game - Simple', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the home page
    await page.goto('http://localhost:3000');
  });

  test('should start solo classic game', async ({ page }) => {
    // Step 1: Click on Solo button
    await page.click('button:has-text("SOLO")');
    
    // Step 2: Wait for navigation to game mode selector
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    // Step 3: Fill in username and password
    const username = `solo${Date.now() % 10000}`;
    await page.fill('input[placeholder="Username"]', username);
    
    // Wait for username check to complete
    await page.waitForTimeout(1000);
    
    // The system will prompt for password if user exists
    // Fill password in all cases
    await page.fill('input[placeholder="Password"]', 'testpass123');
    
    // Wait a bit for auth to process
    await page.waitForTimeout(500);
    
    // Step 4: Click on Classic mode
    await page.click('button:has-text("CLASSIC")');
    
    // Step 5: Wait for navigation to game arena
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // Step 6: Verify canvas is visible
    const canvas = page.locator('canvas');
    await expect(canvas).toBeVisible();
    
    // Step 7: Test keyboard input
    await page.keyboard.press('ArrowUp');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowRight');
    
    // Step 8: Take a screenshot for debugging
    await page.screenshot({ path: 'test-results/solo-game.png' });
  });

  test('should display game over when snake crashes', async ({ page }) => {
    // Start a solo game first
    await page.click('button:has-text("SOLO")');
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    const username = `crash${Date.now() % 10000}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1000);
    await page.fill('input[placeholder="Password"]', 'testpass123');
    await page.waitForTimeout(500);
    await page.click('button:has-text("CLASSIC")');
    
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // Make the snake crash by moving in a small circle repeatedly
    // This should eventually cause the snake to hit itself
    const moves = ['ArrowUp', 'ArrowRight', 'ArrowDown', 'ArrowLeft'];
    
    for (let i = 0; i < 50; i++) {
      await page.keyboard.press(moves[i % 4]);
      await page.waitForTimeout(50);
    }
    
    // Wait for game over text to appear (with increased timeout)
    const gameOverText = page.locator('text=Game Over');
    await expect(gameOverText).toBeVisible({ timeout: 30000 });
    
    // Verify score is displayed
    const scoreText = page.locator('text=/Score:\\s*\\d+/');
    await expect(scoreText).toBeVisible();
  });

  test('should navigate back to menu from game over screen', async ({ page }) => {
    // Start a game and make it end quickly
    await page.click('button:has-text("SOLO")');
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    const username = `nav${Date.now() % 10000}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1000);
    await page.fill('input[placeholder="Password"]', 'testpass123');
    await page.waitForTimeout(500);
    await page.click('button:has-text("CLASSIC")');
    
    await page.waitForURL(/\/play\/\d+/, { timeout: 10000 });
    
    // Force a quick crash
    await page.keyboard.press('ArrowUp');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowLeft');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowDown');
    await page.waitForTimeout(100);
    await page.keyboard.press('ArrowRight');
    await page.waitForTimeout(100);
    // Repeat to increase chance of crash
    for (let i = 0; i < 20; i++) {
      await page.keyboard.press('ArrowUp');
      await page.waitForTimeout(50);
      await page.keyboard.press('ArrowRight');
      await page.waitForTimeout(50);
    }
    
    // Wait for game over
    await expect(page.locator('text=Game Over')).toBeVisible({ timeout: 30000 });
    
    // Click back to menu button
    await page.click('button:has-text("Back to Menu")');
    
    // Verify we're back at home
    await expect(page).toHaveURL('http://localhost:3000/');
    await expect(page.locator('button:has-text("SOLO")')).toBeVisible();
  });
});