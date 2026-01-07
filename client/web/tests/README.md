# UI Integration Tests

This directory contains end-to-end (E2E) and integration tests for the SnakeTron web client.

## Prerequisites

1. **Docker** must be installed and running
2. **Node.js** and npm installed
3. The server container must be running (handled automatically by tests)

## Test Structure

```
tests/
├── e2e/                     # End-to-end tests
│   ├── fixtures/           # Test utilities and helpers
│   │   ├── test-environment.js    # Docker/server management
│   │   ├── helpers/              # Test helpers
│   │   │   └── websocket-monitor.js   # WebSocket monitoring
│   │   └── page-objects/         # Page object models
│   │       ├── home-page.js
│   │       ├── custom-game-page.js
│   │       └── game-lobby-page.js
│   └── specs/              # Test specifications
│       └── create-custom-game.spec.js
└── integration/            # Integration tests
    └── websocket-connection.test.js
```

## Running Tests

### First Time Setup

```bash
# Install dependencies (if not already done)
npm install

# Install Playwright browsers
npx playwright install
```

### Running All Tests

```bash
# Run all tests in headless mode
npm test

# Run tests with UI mode (interactive)
npm run test:ui

# Run tests in headed mode (see browser)
npm run test:headed

# Run tests with debugging
npm run test:debug
```

### Running Specific Tests

```bash
# Run only E2E tests
npx playwright test tests/e2e

# Run only integration tests
npx playwright test tests/integration

# Run a specific test file
npx playwright test tests/e2e/specs/create-custom-game.spec.js

# Run tests matching a pattern
npx playwright test -g "solo game"
```

## Test Environment

The tests automatically:
1. Check if Docker is running
2. Start the server containers if needed (using docker-compose)
3. Wait for the server to be healthy
4. Start the React dev server
5. Run the tests
6. Keep containers running for subsequent tests

## Writing New Tests

### Page Object Pattern

Tests use the Page Object Model pattern for maintainability:

```javascript
// Don't do this - brittle selector
await page.click('button.custom-game-btn');

// Do this - use page object
const homePage = new HomePage(page);
await homePage.clickCustomGame();
```

### WebSocket Monitoring

Tests can monitor WebSocket messages:

```javascript
const wsMonitor = new WebSocketMonitor(page);
await wsMonitor.setup();

// Wait for specific message
const message = await wsMonitor.waitForMessage('CustomGameCreated', 'received');

// Assert on message content
expect(message.parsed.game_code).toMatch(/^[A-Z0-9]{8}$/);
```

### Data Test IDs

UI elements should have `data-testid` attributes for reliable selection:

```jsx
<button data-testid="create-game-button">Create Game</button>
```

## Debugging Tests

### View Test Report

After running tests, view the HTML report:

```bash
npx playwright show-report
```

### Debug Mode

Run tests in debug mode to step through:

```bash
npm run test:debug
```

### VS Code Integration

Install the Playwright extension for VS Code to run and debug tests directly from the editor.

## CI/CD Integration

Tests can be run in CI environments. The configuration supports:
- Headless execution
- Automatic retries on failure
- Screenshot/video capture on failure
- Parallel test execution

## Troubleshooting

### Docker Not Running

Error: "Docker is not running"
- Solution: Start Docker Desktop

### Port Conflicts

Error: "Port 3000/8080 already in use"
- Solution: Stop conflicting services or change ports in configuration

### WebSocket Connection Failures

- Check server logs: `docker-compose logs server`
- Ensure server is healthy: `docker-compose ps`
- Verify WebSocket URL in App.jsx matches Docker configuration

### Timeout Errors

- Increase timeouts in playwright.config.js
- Check if server is responding slowly
- Verify Docker resources are adequate