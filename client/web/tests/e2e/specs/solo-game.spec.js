const { test, expect } = require('@playwright/test');
const { setupTestEnvironment, teardownTestEnvironment } = require('../fixtures/test-environment.js');
const { HomePage, GameArenaPage } = require('../fixtures/page-objects/index.js');
const { AuthHelper } = require('../fixtures/helpers/auth-helper.js');
const { WebSocketMonitor } = require('../fixtures/helpers/websocket-monitor.js');

test.describe('Solo Game', () => {
  let testEnv;
  let authHelper;
  let wsMonitor;

  test.beforeAll(async () => {
    testEnv = await setupTestEnvironment();
  });

  test.afterAll(async () => {
    await teardownTestEnvironment(testEnv);
  });

  test.beforeEach(async ({ page, context }) => {
    authHelper = new AuthHelper(page, context);
    wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.initialize();
  });

  test('should allow user to play solo classic game', async ({ page }) => {
    // Step 1: Navigate to home page
    const homePage = new HomePage(page);
    await homePage.goto();

    // Step 2: Click on Solo button
    await homePage.clickSolo();

    // Step 3: Register a new user
    const username = `solo_player_${Date.now()}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.fill('input[placeholder="Password"]', 'testpassword123');

    // Step 4: Click on Classic mode
    await page.click('button:has-text("CLASSIC")');

    // Step 5: Wait for WebSocket message and navigation
    const soloGameCreatedPromise = wsMonitor.waitForMessage('SoloGameCreated');
    const navigationPromise = page.waitForURL(/\/play\/\d+/);

    // Wait for both game creation and navigation
    const [soloGameCreated] = await Promise.all([
      soloGameCreatedPromise,
      navigationPromise
    ]);

    // Verify we received the correct message
    expect(soloGameCreated).toHaveProperty('SoloGameCreated');
    expect(soloGameCreated.SoloGameCreated).toHaveProperty('game_id');

    // Step 6: Verify we're on the game arena page
    const gameArenaPage = new GameArenaPage(page);
    await expect(page).toHaveURL(/\/play\/\d+/);
    
    // Step 7: Verify game canvas is visible
    await gameArenaPage.waitForCanvas();

    // Step 8: Test snake movement
    await gameArenaPage.pressKey('ArrowUp');
    
    // Wait for game command to be sent
    const gameCommandPromise = wsMonitor.waitForMessage('GameCommand');
    const gameCommand = await gameCommandPromise;
    
    expect(gameCommand).toHaveProperty('GameCommand');
    expect(gameCommand.GameCommand.command).toEqual({ Direction: 'Up' });

    // Step 9: Wait for game state update
    const gameEventPromise = wsMonitor.waitForMessage('GameEvent');
    const gameEvent = await gameEventPromise;
    
    expect(gameEvent).toHaveProperty('GameEvent');
    expect(gameEvent.GameEvent.event).toHaveProperty('Snapshot');
  });

  test('should end game when snake crashes', async ({ page }) => {
    // Step 1: Navigate to home page and start a solo game
    const homePage = new HomePage(page);
    await homePage.goto();
    await homePage.clickSolo();

    // Register user and start game
    const username = `crash_test_${Date.now()}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.fill('input[placeholder="Password"]', 'testpassword123');
    await page.click('button:has-text("CLASSIC")');

    // Wait for game to start
    await page.waitForURL(/\/play\/\d+/);
    const gameArenaPage = new GameArenaPage(page);
    await gameArenaPage.waitForCanvas();

    // Step 2: Make the snake crash into itself
    // Send rapid conflicting directions to cause a crash
    await gameArenaPage.pressKey('ArrowUp');
    await page.waitForTimeout(100);
    await gameArenaPage.pressKey('ArrowLeft');
    await page.waitForTimeout(100);
    await gameArenaPage.pressKey('ArrowDown');
    await page.waitForTimeout(100);
    await gameArenaPage.pressKey('ArrowRight');

    // Step 3: Wait for game end event
    const gameEndPromise = wsMonitor.waitForMessage('GameEvent', (msg) => {
      return msg.GameEvent?.event?.SoloGameEnded !== undefined;
    });

    // Keep the snake moving in a tight pattern to force a crash
    for (let i = 0; i < 20; i++) {
      await gameArenaPage.pressKey('ArrowUp');
      await page.waitForTimeout(50);
      await gameArenaPage.pressKey('ArrowRight');
      await page.waitForTimeout(50);
      await gameArenaPage.pressKey('ArrowDown');
      await page.waitForTimeout(50);
      await gameArenaPage.pressKey('ArrowLeft');
      await page.waitForTimeout(50);
    }

    const gameEndEvent = await gameEndPromise;
    
    // Verify game ended with score and duration
    expect(gameEndEvent.GameEvent.event).toHaveProperty('SoloGameEnded');
    expect(gameEndEvent.GameEvent.event.SoloGameEnded).toHaveProperty('score');
    expect(gameEndEvent.GameEvent.event.SoloGameEnded).toHaveProperty('duration');

    // Step 4: Verify game over UI is displayed
    await expect(page.locator('text=Game Over')).toBeVisible();
    await expect(page.locator('text=Score:')).toBeVisible();
  });

  test('should handle solo tactical mode', async ({ page }) => {
    // Step 1: Navigate to home page
    const homePage = new HomePage(page);
    await homePage.goto();

    // Step 2: Click on Solo button
    await homePage.clickSolo();

    // Step 3: Register a new user
    const username = `tactical_player_${Date.now()}`;
    await page.fill('input[placeholder="Username"]', username);
    await page.fill('input[placeholder="Password"]', 'testpassword123');

    // Step 4: Click on Tactical mode
    await page.click('button:has-text("TACTICAL")');

    // Step 5: Wait for WebSocket message and navigation
    const soloGameCreatedPromise = wsMonitor.waitForMessage('SoloGameCreated');
    const navigationPromise = page.waitForURL(/\/play\/\d+/);

    const [soloGameCreated] = await Promise.all([
      soloGameCreatedPromise,
      navigationPromise
    ]);

    // Verify tactical mode was created
    expect(soloGameCreated).toHaveProperty('SoloGameCreated');
    expect(soloGameCreated.SoloGameCreated).toHaveProperty('game_id');

    // Step 6: Verify we're on the game arena page
    const gameArenaPage = new GameArenaPage(page);
    await gameArenaPage.waitForCanvas();

    // Tactical mode should have different movement mechanics
    // but for now just verify the game started
    await expect(page).toHaveURL(/\/play\/\d+/);
  });
});