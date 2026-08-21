#!/bin/bash
set -e

echo "Initializing DynamoDB tables..."

REPLAY_BUCKET="${SNAKETRON_REPLAY_S3_BUCKET:-snaketron-replays-dev}"
REPLAY_PREFIX="${SNAKETRON_REPLAY_S3_PREFIX:-recordings}"
TEXTURE_BUCKET="${SNAKETRON_TEXTURE_S3_BUCKET:-snaketron-textures-dev}"
TEXTURE_PREFIX="${SNAKETRON_TEXTURE_S3_PREFIX:-textures}"
TABLE_PREFIX="${DYNAMODB_TABLE_PREFIX:-snaketron}"

# Create main table
awslocal dynamodb create-table \
    --table-name "${TABLE_PREFIX}-main" \
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
    --table-name "${TABLE_PREFIX}-usernames" \
    --attribute-definitions \
        AttributeName=username,AttributeType=S \
    --key-schema \
        AttributeName=username,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

# Create game code index table
awslocal dynamodb create-table \
    --table-name "${TABLE_PREFIX}-game-codes" \
    --attribute-definitions \
        AttributeName=gameCode,AttributeType=S \
    --key-schema \
        AttributeName=gameCode,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

# Enable TTL on main table
awslocal dynamodb update-time-to-live \
    --table-name "${TABLE_PREFIX}-main" \
    --time-to-live-specification "Enabled=true,AttributeName=ttl" || true

echo "Initializing private replay bucket..."

awslocal s3api create-bucket --bucket "$REPLAY_BUCKET" || true

awslocal s3api put-public-access-block \
    --bucket "$REPLAY_BUCKET" \
    --public-access-block-configuration \
    'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'

awslocal s3api put-bucket-encryption \
    --bucket "$REPLAY_BUCKET" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":false}]}'

awslocal s3api put-bucket-lifecycle-configuration \
    --bucket "$REPLAY_BUCKET" \
    --lifecycle-configuration \
    "{\"Rules\":[{\"ID\":\"abort-incomplete-replay-uploads\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"${REPLAY_PREFIX}/\"},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}"

echo "Initializing private texture bucket..."

awslocal s3api create-bucket --bucket "$TEXTURE_BUCKET" || true

awslocal s3api put-public-access-block \
    --bucket "$TEXTURE_BUCKET" \
    --public-access-block-configuration \
    'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true'

awslocal s3api put-bucket-encryption \
    --bucket "$TEXTURE_BUCKET" \
    --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":false}]}'

awslocal s3api put-bucket-lifecycle-configuration \
    --bucket "$TEXTURE_BUCKET" \
    --lifecycle-configuration \
    "{\"Rules\":[{\"ID\":\"abort-incomplete-texture-uploads\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"${TEXTURE_PREFIX}/\"},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}"

echo "LocalStack resources initialized successfully!"

# List tables to confirm
echo "Available tables:"
awslocal dynamodb list-tables

echo "Available buckets:"
awslocal s3api list-buckets
