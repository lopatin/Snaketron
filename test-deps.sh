#!/bin/bash
set -e

echo "Starting test dependencies (LocalStack DynamoDB and Redis)..."

# Start Redis and LocalStack
docker-compose up -d redis localstack

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 5

# Check if services are healthy
echo "Checking service health..."
docker-compose exec -T redis redis-cli ping || { echo "Redis not ready"; exit 1; }

# Wait for LocalStack health check
for i in {1..30}; do
    if curl -s http://localhost:4566/_localstack/health | grep -q '"dynamodb": "available"'; then
        echo "LocalStack DynamoDB is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "LocalStack failed to become ready"
        exit 1
    fi
    echo "Waiting for LocalStack... ($i/30)"
    sleep 1
done

# Initialize DynamoDB tables
echo "Initializing DynamoDB tables..."
./scripts/init-dynamodb.sh

echo ""
echo "✓ Test dependencies are ready!"
echo ""
echo "Services running:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "redis|localstack"
echo ""
echo "Run 'docker-compose down' when done with testing"