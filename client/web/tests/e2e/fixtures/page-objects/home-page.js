class HomePage {
  constructor(page) {
    this.page = page;
    
    // Define selectors - prefer data-testid, fallback to text/role
    this.selectors = {
      customGameButton: '[data-testid="custom-game-button"], button:has-text("Custom Game")',
      joinGameButton: '[data-testid="join-game-button"], button:has-text("Join Existing Game")',
      quickPlayButton: '[data-testid="quick-play-button"], button:has-text("Quick Play")',
      soloButton: '[data-testid="solo-button"], button:has-text("SOLO")',
      logo: 'img[alt="SnakeTron Logo"]',
      title: 'h1'
    };
  }

  async navigate() {
    await this.page.goto('/');
    await this.waitForLoad();
  }

  async goto() {
    await this.navigate();
  }

  async waitForLoad() {
    // Wait for the main content to be visible
    await this.page.waitForSelector(this.selectors.logo, { state: 'visible' });
  }

  async clickCustomGame() {
    await this.page.click(this.selectors.customGameButton);
    // Wait for navigation
    await this.page.waitForURL('**/custom');
  }

  async clickJoinGame() {
    await this.page.click(this.selectors.joinGameButton);
    // The modal should appear, not navigate
  }

  async clickQuickPlay() {
    await this.page.click(this.selectors.quickPlayButton);
  }

  async clickSolo() {
    await this.page.click(this.selectors.soloButton);
    // Wait for navigation to game mode selector
    await this.page.waitForURL('**/game-modes/solo');
  }

  async isCustomGameButtonVisible() {
    return await this.page.isVisible(this.selectors.customGameButton);
  }

  async isJoinGameButtonVisible() {
    return await this.page.isVisible(this.selectors.joinGameButton);
  }

  async getTitle() {
    return await this.page.textContent(this.selectors.title);
  }
}

module.exports = { HomePage };