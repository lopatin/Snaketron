const { test, expect } = require('@playwright/test');

test('simple connection test', async ({ page }) => {
  // Navigate to app
  await page.goto('http://localhost:3000');
  
  // Wait for page to load
  await page.waitForSelector('img[alt="Snaketron"]');
  
  // Check if custom game button exists
  const customGameButton = await page.locator('[data-testid="custom-game-button"]');
  await expect(customGameButton).toBeVisible();
  
  console.log('Page loaded successfully!');
});