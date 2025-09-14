#!/bin/bash
# Script to run quickmatch/duel game tests with the correct environment
# 
# DynamoDB tables are created automatically when the server starts.
# Just ensure Redis and LocalStack containers are running.

echo "Running quickmatch/duel game tests..."
echo "Note: Make sure Redis and LocalStack are running"
echo "(DynamoDB tables will be created automatically if they don't exist)"
echo ""

# Try to clear Redis test database (database 1)
if command -v redis-cli &> /dev/null; then
    echo "Clearing Redis test database (database 1)..."
    redis-cli -n 1 FLUSHDB 2>/dev/null || echo "Warning: Could not flush Redis database 1 (might not be running)"
else
    echo "Warning: redis-cli not found, skipping Redis cleanup"
    echo "You may want to install redis-cli or manually clear Redis test database"
fi

# Set the Redis URL to use database 1 for tests
export SNAKETRON_REDIS_URL="redis://127.0.0.1:6379/1"

# Set AWS/LocalStack environment variables for DynamoDB
export AWS_ENDPOINT_URL="http://localhost:4566"
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"

# Run the tests with serial execution to avoid race conditions
echo "Running tests serially (--test-threads=1) to avoid server startup conflicts..."
cargo test -p server --test duel_game_test -- --test-threads=1 "$@"