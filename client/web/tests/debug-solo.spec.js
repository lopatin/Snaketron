const { test, expect } = require('@playwright/test');

test('debug solo game step by step', async ({ page }) => {
  // Enable all console logs
  page.on('console', msg => console.log('BROWSER:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err));
  page.on('request', request => {
    if (request.url().includes('localhost')) {
      console.log('REQUEST:', request.method(), request.url());
    }
  });
  page.on('response', response => {
    if (response.url().includes('localhost') && response.status() !== 200) {
      console.log('RESPONSE:', response.status(), response.url());
    }
  });
  
  console.log('Step 1: Navigating to http://localhost:3000');
  await page.goto('http://localhost:3000');
  await page.waitForTimeout(1000);
  
  console.log('Step 2: Checking for Solo button');
  const soloButton = page.locator('button:has-text("SOLO")');
  const soloVisible = await soloButton.isVisible();
  console.log('Solo button visible:', soloVisible);
  
  if (!soloVisible) {
    console.log('Page content:', await page.content());
    throw new Error('Solo button not found');
  }
  
  console.log('Step 3: Clicking Solo button');
  await soloButton.click();
  await page.waitForTimeout(1000);
  
  console.log('Current URL:', page.url());
  
  console.log('Step 4: Checking for username input');
  const usernameInput = page.locator('input[placeholder="Username"]');
  const usernameVisible = await usernameInput.isVisible();
  console.log('Username input visible:', usernameVisible);
  
  if (!usernameVisible) {
    console.log('Page content:', await page.content());
    throw new Error('Username input not found');
  }
  
  console.log('Step 5: Filling username');
  const username = `test${Date.now() % 1000}`;
  await usernameInput.fill(username);
  await page.waitForTimeout(2000);
  
  console.log('Step 6: Filling password');
  const passwordInput = page.locator('input[placeholder="Password"]');
  await passwordInput.fill('test123');
  await page.waitForTimeout(1000);
  
  console.log('Step 7: Checking for Classic button');
  const classicButton = page.locator('button:has-text("CLASSIC")');
  const classicVisible = await classicButton.isVisible();
  console.log('Classic button visible:', classicVisible);
  
  console.log('Step 8: Clicking Classic button');
  await classicButton.click();
  
  console.log('Step 9: Waiting for navigation or error');
  await page.waitForTimeout(5000);
  
  console.log('Final URL:', page.url());
  
  // Check for any error messages
  const errorElements = await page.locator('.text-red-600').allTextContents();
  if (errorElements.length > 0) {
    console.log('Error messages found:', errorElements);
  }
  
  // Take screenshot
  await page.screenshot({ path: 'test-results/debug-final-state.png' });
  console.log('Screenshot saved to test-results/debug-final-state.png');
});