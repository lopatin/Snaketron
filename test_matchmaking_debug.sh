#!/bin/bash

# Debug script to test matchmaking manually

echo "=== Testing Redis Connection ==="
docker exec snaketron-redis redis-cli PING

echo ""
echo "=== Checking Redis Keys ==="
docker exec snaketron-redis redis-cli KEYS "matchmaking:lobby:*"

echo ""
echo "=== Running Test with Debug Logs (first 10 seconds) ==="
RUST_LOG=server=debug cargo test -p server test_two_single_lobbies_create_1v1 -- --nocapture --test-threads=1 2>&1 | head -200

