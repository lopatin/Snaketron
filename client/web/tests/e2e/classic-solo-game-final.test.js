const { test, expect } = require('@playwright/test');
const { WebSocketMonitor } = require('./fixtures/helpers/websocket-monitor.js');

test.describe('Classic Solo Game', () => {
  test('user can play classic solo game until game over', async ({ page, viewport }) => {
    // Set fullscreen viewport
    await page.setViewportSize({ width: 1920, height: 1080 });
    
    // Log viewport size
    const actualViewport = await page.viewportSize();
    console.log('Viewport size:', actualViewport);
    // Set up WebSocket monitoring
    const wsMonitor = new WebSocketMonitor(page);
    await wsMonitor.setup();

    // Navigate to the app
    await page.goto('http://localhost:3000');
    
    // Wait for WebSocket connection
    await wsMonitor.waitForConnection();
    console.log('WebSocket connected');
    
    // Click the SOLO button
    await page.click('button:has-text("SOLO")');
    console.log('Clicked SOLO button');
    
    // Wait for navigation to game modes
    await page.waitForURL('**/game-modes/solo');
    console.log('Navigated to solo game modes');
    
    // Use a simple username that's less likely to exist
    const timestamp = Date.now();
    const username = `solo${timestamp}`;
    const password = 'test123';
    
    // Enter username
    await page.fill('input[placeholder="Username"]', username);
    console.log('Entered username:', username);
    
    // Wait for username availability check
    await page.waitForTimeout(1500); // Give it time to check
    
    // Check what the auth message says
    const authMessageElement = await page.locator('.auth-message').first();
    const authMessage = await authMessageElement.textContent();
    console.log('Auth message:', authMessage);
    
    // Enter password regardless of whether user exists or not
    await page.fill('input[placeholder="Password"]', password);
    console.log('Entered password');
    
    // Clear any existing error by waiting
    await page.waitForTimeout(500);
    
    // Click the CLASSIC button to start the game
    console.log('Clicking CLASSIC button...');
    await page.click('button:has-text("CLASSIC")');
    
    // Wait for navigation to game - be more patient
    console.log('Waiting for navigation to game...');
    
    try {
      // Wait for either game creation or navigation
      await page.waitForURL('**/play/*', { timeout: 15000 });
      console.log('Successfully navigated to game');
    } catch (err) {
      // If navigation failed, check for error messages
      const errorVisible = await page.locator('text=Invalid username or password').isVisible();
      if (errorVisible) {
        console.log('Authentication failed - trying with different credentials');
        
        // Try again with a different approach - use a known good username pattern
        const newUsername = `player${timestamp}`;
        await page.fill('input[placeholder="Username"]', newUsername);
        await page.waitForTimeout(1500);
        await page.fill('input[placeholder="Password"]', password);
        await page.waitForTimeout(500);
        await page.click('button:has-text("CLASSIC")');
        
        // Wait for navigation again
        await page.waitForURL('**/play/*', { timeout: 10000 });
      } else {
        throw err;
      }
    }
    
    // Get game ID from URL
    const url = page.url();
    const gameId = url.split('/play/')[1];
    console.log('Game ID:', gameId);
    
    // Wait for game canvas to be visible
    await page.waitForSelector('canvas', { state: 'visible', timeout: 10000 });
    console.log('Canvas is visible');
    
    // Wait for game to initialize
    await page.waitForTimeout(2000);
    
    // Check if game is rendering by looking for the canvas content
    const canvasRect = await page.locator('canvas').boundingBox();
    console.log('Canvas dimensions:', canvasRect);
    
    // Wait a bit before turning
    await page.waitForTimeout(2000); // 10 ticks
    
    // Turn the snake down
    await page.keyboard.press('ArrowDown');
    console.log('Pressed ArrowDown');
    
    // Wait for the snake to crash into the wall
    // The snake starts at y=20 moving right, after turning down it needs to travel 20 cells
    // At 5 ticks per second, this should take about 4 seconds
    console.log('Waiting for game over...');
    
    // Take a screenshot just before game over to capture snake position
    await page.waitForTimeout(3500); // Wait almost until crash
    
    // Capture the visual state before the crash
    const visualStateBeforeCrash = await page.evaluate(() => {
      const canvas = document.querySelector('canvas');
      if (!canvas) return null;
      
      const ctx = canvas.getContext('2d');
      if (!ctx) return null;
      
      // Get a data URL of the current canvas state
      return canvas.toDataURL();
    });
    
    await page.screenshot({ path: 'test-results/snake-before-crash.png' });
    
    // Wait for game over modal with longer timeout
    const gameOverModal = await page.waitForSelector('div:has(h2:has-text("Game Over!"))', {
      state: 'visible',
      timeout: 15000
    });
    
    expect(gameOverModal).toBeTruthy();
    console.log('Game over modal appeared!');
    
    // IMPORTANT: Check snake position when game ends
    // This should verify that the snake has actually crashed into the wall
    // The arena height is 40, and the snake should be at y=39 (bottom wall) when it crashes
    console.log('Checking snake position at game over...');
    
    // Also take a screenshot after game over
    await page.screenshot({ path: 'test-results/game-over-position.png' });
    
    // Get the actual game state from the exposed window object
    const gameStateAnalysis = await page.evaluate(() => {
      // Access the game state that we exposed in GameArena component
      const gameState = window.__gameArenaState;
      
      if (!gameState) {
        return { error: 'Game state not found on window.__gameArenaState' };
      }
      
      // Get snake information
      const snake = gameState.arena?.snakes?.[0];
      if (!snake) {
        return { error: 'No snake found in game state' };
      }
      
      // Get the head position (first element in the body array)
      const headPosition = snake.body[0];
      if (!headPosition) {
        return { error: 'Snake has no head position' };
      }
      
      // Arena dimensions
      const arenaHeight = gameState.arena.height || 40;
      const arenaWidth = gameState.arena.width || 40;
      
      // Calculate distance from walls
      const distanceFromBottomWall = (arenaHeight - 1) - headPosition.y;
      const distanceFromTopWall = headPosition.y;
      const distanceFromLeftWall = headPosition.x;
      const distanceFromRightWall = (arenaWidth - 1) - headPosition.x;
      
      // Check if snake is at any wall
      const isAtBottomWall = headPosition.y === arenaHeight - 1;
      const isAtTopWall = headPosition.y === 0;
      const isAtLeftWall = headPosition.x === 0;
      const isAtRightWall = headPosition.x === arenaWidth - 1;
      const isAtAnyWall = isAtBottomWall || isAtTopWall || isAtLeftWall || isAtRightWall;
      
      return {
        success: true,
        gameState: {
          tick: gameState.tick,
          status: gameState.status,
          arenaWidth: arenaWidth,
          arenaHeight: arenaHeight
        },
        snake: {
          headPosition: headPosition,
          direction: snake.direction,
          isAlive: snake.is_alive,
          bodyLength: snake.body.length
        },
        wallDistances: {
          fromBottom: distanceFromBottomWall,
          fromTop: distanceFromTopWall,
          fromLeft: distanceFromLeftWall,
          fromRight: distanceFromRightWall
        },
        wallCollision: {
          isAtBottomWall: isAtBottomWall,
          isAtTopWall: isAtTopWall,
          isAtLeftWall: isAtLeftWall,
          isAtRightWall: isAtRightWall,
          isAtAnyWall: isAtAnyWall
        }
      };
    });
    
    console.log('Game state analysis:', JSON.stringify(gameStateAnalysis, null, 2));
    
    if (gameStateAnalysis.success) {
      const { snake, wallDistances, wallCollision } = gameStateAnalysis;
      
      console.log(`Snake head position: (${snake.headPosition.x}, ${snake.headPosition.y})`);
      console.log(`Snake was moving: ${snake.direction}`);
      console.log(`Distance from bottom wall: ${wallDistances.fromBottom} cells`);
      
      // The game state shows the snake is at the wall
      expect(wallCollision.isAtBottomWall).toBe(true);
      expect(wallCollision.isAtAnyWall).toBe(true);
      
      // However, there's a visual bug where the rendered snake appears to be 
      // several cells away from the wall when the game ends.
      // This test validates the logical game state is correct, but the visual
      // rendering shows the snake dying before it appears to reach the wall.
      
      // To demonstrate the visual bug, let's wait a moment for rendering to settle
      await page.waitForTimeout(100);
      
      // Take a screenshot after hiding the modal for analysis
      await page.evaluate(() => {
        const modal = document.querySelector('[class*="absolute"][class*="inset-0"]');
        if (modal) modal.style.display = 'none';
      });
      await page.waitForTimeout(50);
      await page.screenshot({ path: 'test-results/game-over-canvas-only.png' });
      
      // Now analyze the canvas to find where the snake appears visually
      const visualAnalysis = await page.evaluate(() => {
        const canvas = document.querySelector('canvas');
        if (!canvas || !canvas.getContext) return { error: 'No canvas found' };
        
        const ctx = canvas.getContext('2d');
        if (!ctx) return { error: 'No context' };
        
        // Get accurate cell size
        const cellSize = canvas.width / 40;
        
        // Get the full canvas image data
        const fullImageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
        
        // Find all red pixels (snake)
        let lowestRedY = -1;
        let redPixelCount = 0;
        let snakePositions = [];
        
        for (let y = 0; y < canvas.height; y++) {
          for (let x = 0; x < canvas.width; x++) {
            const i = (y * canvas.width + x) * 4;
            const r = fullImageData.data[i];
            const g = fullImageData.data[i + 1];
            const b = fullImageData.data[i + 2];
            
            // Check for red snake color
            if (r > 200 && g < 100 && b < 100) {
              redPixelCount++;
              const gridX = Math.floor(x / cellSize);
              const gridY = Math.floor(y / cellSize);
              
              // Track unique grid positions
              const posKey = `${gridX},${gridY}`;
              if (!snakePositions.find(p => p.key === posKey)) {
                snakePositions.push({ x: gridX, y: gridY, key: posKey });
              }
              
              if (y > lowestRedY) {
                lowestRedY = y;
              }
            }
          }
        }
        
        // Calculate the grid position of the lowest red pixel
        const visualGridY = lowestRedY >= 0 ? Math.floor(lowestRedY / cellSize) : -1;
        
        // Also check the very bottom edge more carefully
        const bottomEdgeY = canvas.height - 1;
        const bottomEdgeData = ctx.getImageData(0, bottomEdgeY, canvas.width, 1);
        let hasRedAtBottomEdge = false;
        
        for (let x = 0; x < bottomEdgeData.data.length; x += 4) {
          if (bottomEdgeData.data[x] > 200 && bottomEdgeData.data[x + 1] < 100 && bottomEdgeData.data[x + 2] < 100) {
            hasRedAtBottomEdge = true;
            break;
          }
        }
        
        return {
          lowestRedPixel: lowestRedY,
          visualGridY: visualGridY,
          cellSize: cellSize,
          canvasWidth: canvas.width,
          canvasHeight: canvas.height,
          redPixelCount: redPixelCount,
          snakeGridPositions: snakePositions,
          hasRedAtBottomEdge: hasRedAtBottomEdge,
          pixelsFromBottom: canvas.height - 1 - lowestRedY
        };
      });
      
      console.log('\nDetailed Visual Analysis:');
      console.log('Canvas dimensions:', visualAnalysis.canvasWidth, 'x', visualAnalysis.canvasHeight);
      console.log('Cell size:', visualAnalysis.cellSize);
      console.log('Red pixels found:', visualAnalysis.redPixelCount);
      console.log('Snake grid positions found:', visualAnalysis.snakeGridPositions);
      console.log('Lowest red pixel Y:', visualAnalysis.lowestRedPixel);
      console.log('Pixels from bottom edge:', visualAnalysis.pixelsFromBottom);
      
      if (visualAnalysis.visualGridY >= 0) {
        const logicalY = snake.headPosition.y;
        const visualY = visualAnalysis.visualGridY;
        const visualDiscrepancy = logicalY - visualY;
        
        console.log(`\n=== BUG ANALYSIS ===`);
        console.log(`Logical snake position: Y=${logicalY}`);
        console.log(`Visual snake position: Y=${visualY}`);
        console.log(`Discrepancy: ${Math.abs(visualDiscrepancy)} cells`);
        
        if (visualDiscrepancy !== 0) {
          console.log(`\nBUG CONFIRMED: Snake appears ${Math.abs(visualDiscrepancy)} cells ${visualDiscrepancy > 0 ? 'higher' : 'lower'} than its logical position!`);
        }
        
        // This assertion demonstrates the bug
        // The visual position should match the logical position
        expect(visualY).toBe(logicalY);
      } else {
        console.log('\nWARNING: Could not find snake in visual analysis');
      }
    } else {
      // If we couldn't get the game state, fail the test
      throw new Error(`Could not analyze game state: ${gameStateAnalysis.error}`);
    }
    
    // Verify modal content
    await expect(page.locator('h2:has-text("Game Over!")')).toBeVisible();
    
    // Check for final score
    const scoreText = await page.locator('p:has-text("Final Score:")').textContent();
    expect(scoreText).toMatch(/Final Score: \d+/);
    console.log('Score:', scoreText);
    
    // Verify Play Again button exists
    await expect(page.locator('button:has-text("Play Again")')).toBeVisible();
    
    // Click Play Again to return to main menu
    await page.click('button:has-text("Play Again")');
    await page.waitForURL('**/', { timeout: 5000 });
    
    console.log('Test completed successfully!');
  });
});