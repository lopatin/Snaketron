class CustomGamePage {
  constructor(page) {
    this.page = page;
    
    // Define selectors
    this.selectors = {
      // Game mode selector
      gameModeSelect: '[data-testid="game-mode-select"], select:near(:text("Game Mode"))',
      
      // Arena size controls
      arenaSizeSlider: '[data-testid="arena-size-slider"], input[type="range"]:near(:text("Arena Size"))',
      arenaSizeValue: '[data-testid="arena-size-value"], span:near(:text("Arena Size"))',
      
      // Max players controls
      maxPlayersSlider: '[data-testid="max-players-slider"], input[type="range"]:near(:text("Max Players"))',
      maxPlayersValue: '[data-testid="max-players-value"], span:near(:text("Max Players"))',
      
      // Game speed controls
      gameSpeedSlider: '[data-testid="game-speed-slider"], input[type="range"]:near(:text("Game Speed"))',
      gameSpeedValue: '[data-testid="game-speed-value"], span:near(:text("Game Speed"))',
      
      // Buttons
      createGameButton: '[data-testid="create-game-button"], button:has-text("Create Game")',
      backButton: '[data-testid="back-button"], button:has-text("Back")',
      
      // Loading state
      loadingSpinner: '[data-testid="loading-spinner"], .animate-spin',
      
      // Title
      pageTitle: 'h1:has-text("Custom Game")'
    };
  }

  async waitForLoad() {
    await this.page.waitForSelector(this.selectors.pageTitle, { state: 'visible' });
    // Ensure form elements are loaded
    await this.page.waitForSelector(this.selectors.createGameButton, { state: 'visible' });
  }

  async selectGameMode(mode) {
    await this.page.selectOption(this.selectors.gameModeSelect, mode);
  }

  async setArenaSize(size) {
    const slider = await this.page.locator(this.selectors.arenaSizeSlider);
    
    // Set the value directly for consistency
    await slider.evaluate((el, value) => {
      el.value = value;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }, size.toString());
  }

  async setMaxPlayers(count) {
    const slider = await this.page.locator(this.selectors.maxPlayersSlider);
    
    await slider.evaluate((el, value) => {
      el.value = value;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }, count.toString());
  }

  async setGameSpeed(speed) {
    const slider = await this.page.locator(this.selectors.gameSpeedSlider);
    
    // Map speed names to values if needed
    const speedMap = {
      'Slow': '1',
      'Normal': '2',
      'Fast': '3',
      'Extreme': '4'
    };
    
    const speedValue = speedMap[speed] || speed;
    
    await slider.evaluate((el, value) => {
      el.value = value;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }, speedValue.toString());
  }

  async createGame() {
    // Click create game button
    await this.page.click(this.selectors.createGameButton);
    
    // Wait for loading state or navigation
    await Promise.race([
      this.page.waitForURL('**/game/**', { timeout: 10000 }),
      this.page.waitForSelector(this.selectors.loadingSpinner, { state: 'visible', timeout: 1000 })
        .then(() => this.page.waitForURL('**/game/**', { timeout: 10000 }))
        .catch(() => {}) // Ignore if no loading spinner
    ]);
  }

  async clickBack() {
    await this.page.click(this.selectors.backButton);
    await this.page.waitForURL('/');
  }

  async getArenaSize() {
    const text = await this.page.textContent(this.selectors.arenaSizeValue);
    return parseInt(text);
  }

  async getMaxPlayers() {
    const text = await this.page.textContent(this.selectors.maxPlayersValue);
    return parseInt(text);
  }

  async getGameSpeed() {
    return await this.page.textContent(this.selectors.gameSpeedValue);
  }

  async getSelectedGameMode() {
    return await this.page.inputValue(this.selectors.gameModeSelect);
  }

  async isCreateButtonEnabled() {
    return await this.page.isEnabled(this.selectors.createGameButton);
  }

  async createSoloGame(options = {}) {
    await this.waitForLoad();
    
    // Set game mode
    if (options.mode) {
      await this.selectGameMode(options.mode);
    }
    
    // Set arena size
    if (options.size) {
      await this.setArenaSize(options.size);
    }
    
    // Set max players to 1 for solo
    await this.setMaxPlayers(1);
    
    // Set game speed
    if (options.speed) {
      await this.setGameSpeed(options.speed);
    }
    
    // Create the game
    await this.createGame();
  }
}

module.exports = { CustomGamePage };