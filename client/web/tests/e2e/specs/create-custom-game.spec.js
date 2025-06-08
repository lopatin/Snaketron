const { test, expect } = require('@playwright/test');
const { TestEnvironment } = require('../fixtures/test-environment.js');
const { HomePage, CustomGamePage, GameLobbyPage } = require('../fixtures/page-objects/index.js');
const { WebSocketMonitor } = require('../fixtures/helpers/websocket-monitor.js');

test.describe('Custom Game Creation', () => {
  let env;

  test.beforeAll(async () => {
    env = new TestEnvironment();
    await env.setup();
  });

  test.afterAll(async () => {
    await env.teardown();
  });

  test('user can create and join a solo custom game', async ({ page }) => {
    // Setup WebSocket monitoring
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app
    await page.goto(env.getAppUrl());
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    
    // Initialize page objects
    const homePage = new HomePage(page);
    const customGamePage = new CustomGamePage(page);
    const gameLobbyPage = new GameLobbyPage(page);

    // Step 1: Navigate to custom game creator
    await homePage.clickCustomGame();
    await expect(page).toHaveURL(/\/custom$/);
    await customGamePage.waitForLoad();

    // Step 2: Configure game settings
    // Select solo mode
    await customGamePage.selectGameMode('solo');
    
    // Set arena size to 20x20
    await customGamePage.setArenaSize(20);
    
    // Verify max players is hidden for solo mode
    const maxPlayersSlider = page.locator('[data-testid="max-players-slider"]');
    await expect(maxPlayersSlider).not.toBeVisible();
    
    // Set game speed to normal (should be default)
    const currentSpeed = await customGamePage.getSelectedGameMode();
    expect(currentSpeed).toBe('solo');

    // Step 3: Create the game
    await customGamePage.createGame();
    
    // Step 4: Verify WebSocket message was sent
    const createMessage = await wsMonitor.waitForMessage('CreateCustomGame', 'sent');
    expect(createMessage.parsed).toMatchObject({
      type: 'CreateCustomGame',
      settings: {
        arena_width: 20,
        arena_height: 20,
        max_players: 1,
        game_mode: 'Solo',
        is_private: true,
        allow_spectators: true,
      }
    });

    // Step 5: Wait for server response with game code
    const responseMessage = await wsMonitor.waitForMessage('CustomGameCreated', 'received', 10000);
    expect(responseMessage.parsed).toMatchObject({
      type: 'CustomGameCreated',
      game_id: expect.any(String),
      game_code: expect.stringMatching(/^[A-Z0-9]{8}$/)
    });

    // Step 6: Verify navigation to game lobby
    const gameCode = responseMessage.parsed.game_code;
    await expect(page).toHaveURL(`/game/${gameCode}`);
    
    // Step 7: Verify game lobby displays correctly
    await gameLobbyPage.waitForLoad();
    
    // Check game code is displayed
    const displayedCode = await gameLobbyPage.getGameCode();
    expect(displayedCode).toBe(gameCode);
    
    // Check player count (should be 1 for solo)
    const playerCount = await gameLobbyPage.getPlayerCount();
    expect(playerCount).toBe(1);
    
    // Check if user is host
    const isHost = await gameLobbyPage.isHost();
    expect(isHost).toBe(true);
    
    // Check start button is visible and enabled
    const isStartVisible = await gameLobbyPage.isStartButtonVisible();
    expect(isStartVisible).toBe(true);
    
    const isStartEnabled = await gameLobbyPage.isStartButtonEnabled();
    expect(isStartEnabled).toBe(true);
    
    // Check game settings are displayed correctly
    const settings = await gameLobbyPage.getGameSettings();
    expect(settings).toMatchObject({
      mode: expect.stringContaining('Solo'),
      arenaSize: expect.stringContaining('20'),
      maxPlayers: expect.stringContaining('1'),
      gameSpeed: expect.stringContaining('Normal')
    });

    // Step 8: Start the game
    await gameLobbyPage.startGame();
    
    // Verify start game message was sent
    const startMessage = await wsMonitor.waitForMessage('StartCustomGame', 'sent');
    expect(startMessage.parsed).toMatchObject({
      type: 'StartCustomGame'
    });
    
    // Wait for game state update
    const gameStateMessage = await wsMonitor.waitForMessage('GameState', 'received', 10000);
    expect(gameStateMessage.parsed).toMatchObject({
      type: 'GameState',
      state: expect.objectContaining({
        status: expect.any(String)
      })
    });
    
    // Verify canvas is visible (game has started)
    const isGameStarted = await gameLobbyPage.isGameStarted();
    expect(isGameStarted).toBe(true);
  });

  test('user can navigate back from custom game creator', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();
    
    await page.goto(env.getAppUrl());
    await wsMonitor.waitForConnection();
    
    const homePage = new HomePage(page);
    const customGamePage = new CustomGamePage(page);
    
    // Navigate to custom game creator
    await homePage.clickCustomGame();
    await expect(page).toHaveURL(/\/custom$/);
    
    // Click back/cancel
    await customGamePage.clickBack();
    
    // Should be back at home page
    await expect(page).toHaveURL('/');
    await expect(homePage.isCustomGameButtonVisible()).resolves.toBe(true);
  });

  test('game settings affect WebSocket message correctly', async ({ page }) => {
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();
    
    await page.goto(env.getAppUrl());
    await wsMonitor.waitForConnection();
    
    const homePage = new HomePage(page);
    const customGamePage = new CustomGamePage(page);
    
    // Navigate to custom game creator
    await homePage.clickCustomGame();
    await customGamePage.waitForLoad();
    
    // Configure specific settings
    await customGamePage.selectGameMode('freeForAll');
    await customGamePage.setArenaSize(40);
    await customGamePage.setMaxPlayers(6);
    await customGamePage.setGameSpeed('fast');
    
    // Create the game
    await customGamePage.createGame();
    
    // Verify the correct message was sent
    const createMessage = await wsMonitor.waitForMessage('CreateCustomGame', 'sent');
    expect(createMessage.parsed.settings).toMatchObject({
      arena_width: 40,
      arena_height: 40,
      max_players: 6,
      tick_duration_ms: 200, // Fast speed
      game_mode: {
        FreeForAll: {
          max_players: 6
        }
      }
    });
  });
});