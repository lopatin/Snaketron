const { test, expect } = require('@playwright/test');

test('the final standings render durable death attribution only when present', async ({ page }) => {
  await page.goto('/qa/rating-reveal');

  await expect(page.getByTestId('death-attribution-1')).toHaveText('Demolished by You');
  await expect(page.getByTestId('death-attribution-0')).toHaveCount(0);
});
