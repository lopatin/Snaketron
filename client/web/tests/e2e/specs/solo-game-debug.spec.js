const { test, expect } = require('@playwright/test');

test.describe('Solo Game - Debug', () => {
  test('debug solo game creation', async ({ page }) => {
    // Enable console logging
    page.on('console', msg => console.log('Browser console:', msg.text()));
    page.on('pageerror', err => console.log('Page error:', err));

    // Navigate to the home page
    await page.goto('http://localhost:3000');
    
    // Click on Solo button
    await page.click('button:has-text("SOLO")');
    
    // Wait for navigation to game mode selector
    await expect(page).toHaveURL(/\/game-modes\/solo/);
    
    // Fill in username and password
    const username = `test${Date.now() % 1000}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.waitForTimeout(1000);
    await page.fill('input[placeholder="Password"]', 'test123');
    await page.waitForTimeout(500);
    
    // Debug: Check network activity
    page.on('request', request => {
      if (request.url().includes('ws://') || request.url().includes('api')) {
        console.log('Request:', request.method(), request.url());
      }
    });
    
    page.on('response', response => {
      if (response.url().includes('api')) {
        console.log('Response:', response.status(), response.url());
      }
    });
    
    // Click on Classic mode
    console.log('Clicking Classic button...');
    await page.click('button:has-text("CLASSIC")');
    
    // Wait for any response or navigation
    await page.waitForTimeout(5000);
    
    // Check current URL
    console.log('Current URL:', page.url());
    
    // Take screenshot
    await page.screenshot({ path: 'test-results/debug-solo-game.png' });
    
    // Check for any error messages
    const errorMessages = await page.locator('.text-red-600').allTextContents();
    console.log('Error messages:', errorMessages);
  });
});