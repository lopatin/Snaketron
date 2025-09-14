#!/bin/bash
# Script to run matchmaking integration tests with the correct environment

echo "Running matchmaking integration tests..."
echo "Note: Make sure Redis and PostgreSQL are running (e.g., via docker-compose)"
echo ""

# Try to clear Redis state (ignore errors if redis-cli is not installed)
if command -v redis-cli &> /dev/null; then
    echo "Clearing Redis state..."
    redis-cli FLUSHDB 2>/dev/null || echo "Warning: Could not flush Redis (might not be running)"
else
    echo "Warning: redis-cli not found, skipping Redis cleanup"
    echo "You may want to install redis-cli or manually clear Redis state"
fi

# Set the environment to 'test' for proper channel isolation
export SNAKETRON_ENV=test

# Run the tests with serial execution to avoid race conditions
echo "Running tests serially (--test-threads=1) to avoid server startup conflicts..."
cargo test -p server --test matchmaking_integration_tests -- --test-threads=1 "$@"