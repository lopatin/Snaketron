#!/bin/bash
# Start only database and redis containers for running tests

echo "Starting test dependencies (PostgreSQL and Redis)..."
docker-compose up -d db redis

# Wait for services to be healthy
echo "Waiting for services to be ready..."
docker-compose exec -T db pg_isready -U snaketron
docker-compose exec -T redis redis-cli ping

echo "Test dependencies are ready!"
echo "Run 'docker-compose down' when done with testing"