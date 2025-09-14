#!/bin/bash
set -e

echo "Starting test dependencies..."

# Start Redis and LocalStack
docker-compose up -d redis localstack

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 3

# Initialize DynamoDB tables
echo "Initializing DynamoDB tables..."
./scripts/init-dynamodb.sh

echo "Test dependencies are ready!"
echo ""
echo "Services running:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "redis|localstack"