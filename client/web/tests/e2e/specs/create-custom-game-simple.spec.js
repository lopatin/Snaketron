const { test, expect } = require('@playwright/test');
const { HomePage, CustomGamePage, GameLobbyPage } = require('../fixtures/page-objects/index.js');
const { WebSocketMonitor } = require('../fixtures/helpers/websocket-monitor.js');
const { AuthHelper } = require('../fixtures/helpers/auth-helper.js');

test.describe('Custom Game Creation', () => {
  test('user can create and join a solo custom game', async ({ page }) => {
    // Setup authentication helper
    const authHelper = new AuthHelper(page);
    
    // Authenticate user first
    const authData = await authHelper.authenticateUser();
    console.log('Authenticated as:', authData.user.username);
    
    // Setup WebSocket monitoring
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app (this will reload with auth token in localStorage)
    await page.goto('http://localhost:3000');
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    
    // The app should automatically send the JWT token from localStorage
    // Wait for the token message to be sent
    const tokenMessage = await wsMonitor.waitForMessage('Token', 'sent', 5000);
    console.log('Token sent to server');
    
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
    
    // Step 3: Create the game
    await customGamePage.createGame();
    
    // Step 4: Verify WebSocket message was sent
    const createMessage = await wsMonitor.waitForMessage('CreateCustomGame', 'sent');
    console.log('Create game message:', createMessage.parsed);
    
    expect(createMessage.parsed).toMatchObject({
      CreateCustomGame: {
        settings: expect.objectContaining({
          arena_width: expect.any(Number),
          arena_height: expect.any(Number),
          max_players: 1,
          game_mode: 'Solo'
        })
      }
    });

    // Step 5: Wait for server response with game code
    const responseMessage = await wsMonitor.waitForMessage('CustomGameCreated', 'received', 10000);
    console.log('Server response:', responseMessage.parsed);
    
    expect(responseMessage.parsed).toMatchObject({
      CustomGameCreated: {
        game_id: expect.any(Number),
        game_code: expect.stringMatching(/^[A-Z0-9]{8}$/)
      }
    });

    // Step 6: Verify navigation to game lobby
    const gameCode = responseMessage.parsed.CustomGameCreated.game_code;
    await expect(page).toHaveURL(`/game/${gameCode}`);
    
    // Step 7: Verify game lobby displays correctly
    await gameLobbyPage.waitForLoad();
    
    // Check game code is displayed
    const displayedCode = await gameLobbyPage.getGameCode();
    expect(displayedCode).toBe(gameCode);
    
    console.log('Test completed successfully! Game code:', gameCode);
  });
});