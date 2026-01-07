#!/bin/bash
# Test script to verify Redis reconnection behavior

echo "Starting Redis reconnection test..."
echo "This will:"
echo "1. Start the stack with docker-compose"
echo "2. Wait for leader election"
echo "3. Stop Redis container"
echo "4. Wait a bit"
echo "5. Start Redis container again"
echo "6. Check if leader election recovers"
echo ""
echo "Press Ctrl+C to stop at any time"
echo ""

# Start the stack
echo "Starting docker-compose..."
docker-compose up -d

# Wait for services to start
echo "Waiting for services to start..."
sleep 10

# Check initial logs
echo "Initial leader election status:"
docker-compose logs server | grep -i "leader\|redis" | tail -10

# Stop Redis
echo ""
echo "Stopping Redis container..."
docker-compose stop redis

# Wait and check logs
echo "Waiting 10 seconds with Redis down..."
sleep 10

echo "Server logs while Redis is down:"
docker-compose logs server | grep -i "redis\|leader" | tail -10

# Start Redis again
echo ""
echo "Starting Redis container again..."
docker-compose start redis

# Wait for reconnection
echo "Waiting 10 seconds for reconnection..."
sleep 10

echo ""
echo "Server logs after Redis restart:"
docker-compose logs server | grep -i "redis\|leader\|reconnect" | tail -20

echo ""
echo "Test complete. Run 'docker-compose down' to stop all services."