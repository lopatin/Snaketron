#!/bin/bash

# Run solo game tests in headless mode

echo "Running Solo Game Tests in Headless Mode..."
echo "=========================================="
echo ""

# Ensure test-results directory exists
mkdir -p test-results

# Clear old test results
rm -f test-results/*.png

# Run the robust test (headless is default)
echo "Running robust solo game tests..."
npx playwright test tests/solo-game-robust.spec.js --project=chromium --workers=1 --reporter=list

echo ""
echo "Test Results:"
echo "============="
ls -la test-results/*.png 2>/dev/null || echo "No screenshots were generated"
echo ""
echo "Tests completed. Check the output above for results."