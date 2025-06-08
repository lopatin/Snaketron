#!/bin/bash
# Development script for fast local development

echo "Starting SnakeTron in development mode with hot reloading..."
echo "Changes to Rust code will automatically restart the server."
echo ""

# Use both docker-compose files
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up --build

# Optional: Add a cleanup on exit
trap 'docker-compose -f docker-compose.yml -f docker-compose.dev.yml down' EXIT