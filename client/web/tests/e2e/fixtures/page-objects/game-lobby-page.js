class GameLobbyPage {
  constructor(page) {
    this.page = page;
    
    // Define selectors
    this.selectors = {
      // Game info
      gameCode: '[data-testid="game-code"], .text-6xl.tracking-widest',
      gameCodeLabel: 'p:has-text("Game Code:")',
      
      // Player list
      playersList: '[data-testid="players-list"], div:has(> h3:text("Players"))',
      playerItem: '[data-testid="player-item"], .bg-black-90',
      hostBadge: '[data-testid="host-badge"], :text("HOST")',
      
      // Settings display
      settingsSection: '[data-testid="game-settings"], div:has(> h3:text("Game Settings"))',
      gameModeValue: '[data-testid="game-mode-value"], :text("Game Mode:") + span',
      arenaSizeValue: '[data-testid="arena-size-value"], :text("Arena Size:") + span',
      maxPlayersValue: '[data-testid="max-players-value"], :text("Max Players:") + span',
      gameSpeedValue: '[data-testid="game-speed-value"], :text("Game Speed:") + span',
      
      // Action buttons
      startGameButton: '[data-testid="start-game-button"], button:has-text("Start Game")',
      leaveGameButton: '[data-testid="leave-game-button"], button:has-text("Leave Game")',
      copyCodeButton: '[data-testid="copy-code-button"], button:has-text("Copy Code")',
      
      // Loading/status
      waitingMessage: ':text("Waiting for host to start the game...")',
      loadingSpinner: '.animate-spin',
      
      // Canvas (for when game starts)
      gameCanvas: 'canvas'
    };
  }

  async waitForLoad() {
    // Wait for game code to be visible
    await this.page.waitForSelector(this.selectors.gameCode, { state: 'visible' });
  }

  async getGameCode() {
    await this.waitForLoad();
    const codeElement = await this.page.locator(this.selectors.gameCode);
    return await codeElement.textContent();
  }

  async isHost() {
    // Check if host badge is visible
    const hostBadges = await this.page.locator(this.selectors.hostBadge).count();
    return hostBadges > 0;
  }

  async startGame() {
    // Only available for host
    await this.page.click(this.selectors.startGameButton);
    
    // Wait for game to start (canvas should appear)
    await this.page.waitForSelector(this.selectors.gameCanvas, { 
      state: 'visible',
      timeout: 10000 
    });
  }

  async leaveGame() {
    await this.page.click(this.selectors.leaveGameButton);
    // Should navigate back to home
    await this.page.waitForURL('/');
  }

  async copyGameCode() {
    await this.page.click(this.selectors.copyCodeButton);
    
    // Check clipboard if possible
    try {
      const clipboardText = await this.page.evaluate(() => navigator.clipboard.readText());
      return clipboardText;
    } catch {
      // Clipboard API might not be available in test environment
      return null;
    }
  }

  async getPlayerCount() {
    const players = await this.page.locator(this.selectors.playerItem).count();
    return players;
  }

  async getPlayerNames() {
    const playerElements = await this.page.locator(this.selectors.playerItem).all();
    const names = [];
    
    for (const element of playerElements) {
      const text = await element.textContent();
      // Extract player name (remove HOST badge if present)
      const name = text.replace('HOST', '').trim();
      names.push(name);
    }
    
    return names;
  }

  async getGameSettings() {
    return {
      mode: await this.page.textContent(this.selectors.gameModeValue),
      arenaSize: await this.page.textContent(this.selectors.arenaSizeValue),
      maxPlayers: await this.page.textContent(this.selectors.maxPlayersValue),
      gameSpeed: await this.page.textContent(this.selectors.gameSpeedValue)
    };
  }

  async isStartButtonVisible() {
    return await this.page.isVisible(this.selectors.startGameButton);
  }

  async isStartButtonEnabled() {
    return await this.page.isEnabled(this.selectors.startGameButton);
  }

  async isWaitingForHost() {
    return await this.page.isVisible(this.selectors.waitingMessage);
  }

  async waitForGameToStart(timeout = 10000) {
    await this.page.waitForSelector(this.selectors.gameCanvas, { 
      state: 'visible',
      timeout 
    });
  }

  async isGameStarted() {
    return await this.page.isVisible(this.selectors.gameCanvas);
  }

  async isLoaded() {
    try {
      await this.page.waitForSelector(this.selectors.gameCode, { 
        state: 'visible',
        timeout: 1000 
      });
      return true;
    } catch {
      return false;
    }
  }
}

module.exports = { GameLobbyPage };