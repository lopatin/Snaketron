const { test, expect } = require('@playwright/test');

test('verify headless mode', async ({ page, browserName }) => {
  console.log(`Running in ${browserName} browser`);
  console.log(`Headless mode: ${await page.evaluate(() => navigator.webdriver)}`);
  
  await page.goto('http://localhost:3000');
  await expect(page).toHaveTitle(/SnakeTron/i);
  
  console.log('✓ Headless test completed successfully');
});