#!/bin/bash
set -e

echo "Initializing DynamoDB tables..."

# Create main table
awslocal dynamodb create-table \
    --table-name snaketron-main \
    --attribute-definitions \
        AttributeName=pk,AttributeType=S \
        AttributeName=sk,AttributeType=S \
        AttributeName=gsi1pk,AttributeType=S \
        AttributeName=gsi1sk,AttributeType=S \
        AttributeName=gsi2pk,AttributeType=S \
        AttributeName=gsi2sk,AttributeType=S \
    --key-schema \
        AttributeName=pk,KeyType=HASH \
        AttributeName=sk,KeyType=RANGE \
    --global-secondary-indexes \
        "IndexName=GSI1,KeySchema=[{AttributeName=gsi1pk,KeyType=HASH},{AttributeName=gsi1sk,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
        "IndexName=GSI2,KeySchema=[{AttributeName=gsi2pk,KeyType=HASH},{AttributeName=gsi2sk,KeyType=RANGE}],Projection={ProjectionType=ALL}" \
    --billing-mode PAY_PER_REQUEST || true

# Create username index table
awslocal dynamodb create-table \
    --table-name snaketron-usernames \
    --attribute-definitions \
        AttributeName=username,AttributeType=S \
    --key-schema \
        AttributeName=username,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

# Create game code index table
awslocal dynamodb create-table \
    --table-name snaketron-game-codes \
    --attribute-definitions \
        AttributeName=gameCode,AttributeType=S \
    --key-schema \
        AttributeName=gameCode,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

# Enable TTL on main table
awslocal dynamodb update-time-to-live \
    --table-name snaketron-main \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" || true

echo "DynamoDB tables initialized successfully!"

# List tables to confirm
echo "Available tables:"
awslocal dynamodb list-tables