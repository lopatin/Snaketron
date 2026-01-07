class GameArenaPage {
  constructor(page) {
    this.page = page;
    this.canvas = page.locator('canvas');
    this.gameOverText = page.locator('text=Game Over');
    this.scoreText = page.locator('text=Score:');
    this.backButton = page.locator('button:has-text("Back to Menu")');
  }

  async waitForCanvas() {
    await this.canvas.waitFor({ state: 'visible' });
  }

  async pressKey(key) {
    await this.page.keyboard.press(key);
  }

  async isGameOver() {
    return await this.gameOverText.isVisible();
  }

  async getScore() {
    const scoreElement = await this.scoreText.textContent();
    const match = scoreElement.match(/Score:\s*(\d+)/);
    return match ? parseInt(match[1]) : null;
  }

  async clickBackToMenu() {
    await this.backButton.click();
  }

  async waitForGameToLoad() {
    // Wait for canvas to be visible and have non-zero dimensions
    await this.canvas.waitFor({ state: 'visible' });
    await this.page.waitForFunction(() => {
      const canvas = document.querySelector('canvas');
      return canvas && canvas.width > 0 && canvas.height > 0;
    });
  }

  async takeScreenshot(name) {
    await this.page.screenshot({ path: `test-results/screenshots/${name}.png` });
  }
}

module.exports = { GameArenaPage };