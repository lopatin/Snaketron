#!/bin/bash
set -euo pipefail

# AWS Setup Script for Snaketron
# This script helps set up the initial AWS infrastructure

export AWS_PROFILE=snaketron

# Configuration
PROJECT_NAME="snaketron"
REGION="${AWS_REGION:-us-east-1}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    exit 1
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        error "AWS CLI is not installed. Please install it first."
    fi
    
    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        error "Terraform is not installed. Please install it first."
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS credentials not configured. Please run 'aws configure' first."
    fi
    
    # Get AWS account ID
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    info "AWS Account ID: $AWS_ACCOUNT_ID"

    log "Prerequisites check passed"
}

# Create ECR repositories
create_ecr_repositories() {
    log "Creating ECR repositories..."
    
    # Create server repository
    if aws ecr describe-repositories --repository-names "$PROJECT_NAME-server" --region "$REGION" 2>/dev/null; then
        info "ECR repository $PROJECT_NAME-server already exists"
    else
        aws ecr create-repository \
            --repository-name "$PROJECT_NAME-server" \
            --region "$REGION" \
            --image-scanning-configuration scanOnPush=true \
            --image-tag-mutability MUTABLE
        log "Created ECR repository: $PROJECT_NAME-server"
    fi
}

# Create S3 bucket for Terraform state
create_terraform_backend() {
    log "Setting up Terraform backend..."
    
    local bucket_name="$PROJECT_NAME-terraform-state"
    local table_name="$PROJECT_NAME-terraform-locks"
    
    # Create S3 bucket
    if aws s3api head-bucket --bucket "$bucket_name" 2>/dev/null; then
        info "S3 bucket $bucket_name already exists"
    else
        aws s3api create-bucket \
            --bucket "$bucket_name" \
            --region "$REGION" \
            $([ "$REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$REGION")
        
        # Enable versioning
        aws s3api put-bucket-versioning \
            --bucket "$bucket_name" \
            --versioning-configuration Status=Enabled
        
        # Enable encryption
        aws s3api put-bucket-encryption \
            --bucket "$bucket_name" \
            --server-side-encryption-configuration '{
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }]
            }'
        
        log "Created S3 bucket for Terraform state: $bucket_name"
    fi
    
    # Create DynamoDB table for state locking
    if aws dynamodb describe-table --table-name "$table_name" --region "$REGION" 2>/dev/null; then
        info "DynamoDB table $table_name already exists"
    else
        aws dynamodb create-table \
            --table-name "$table_name" \
            --attribute-definitions AttributeName=LockID,AttributeType=S \
            --key-schema AttributeName=LockID,KeyType=HASH \
            --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
            --region "$REGION"
        
        log "Created DynamoDB table for Terraform locks: $table_name"
    fi
}

# Initialize Terraform
init_terraform() {
    log "Initializing Terraform..."
    
    cd ../terraform
    
    # Create terraform.tfvars file if it doesn't exist
    if [ ! -f terraform.tfvars ]; then
        cat > terraform.tfvars <<EOF
# Auto-generated Terraform variables
aws_region = "$REGION"
environment = "prod"
project_name = "$PROJECT_NAME"

# TODO: Set these values:
jwt_secret = "CHANGE_THIS_TO_A_SECURE_SECRET"
# certificate_arn = "arn:aws:acm:$REGION:$AWS_ACCOUNT_ID:certificate/..."
# cloudfront_certificate_arn = "arn:aws:acm:us-east-1:$AWS_ACCOUNT_ID:certificate/..."
# client_domain_name = "play.example.com"
EOF
        warn "Created terraform.tfvars - Please update the JWT secret and other values"
    fi
    
    terraform init -backend-config="bucket=$PROJECT_NAME-terraform-state" \
                   -backend-config="key=prod/terraform.tfstate" \
                   -backend-config="region=$REGION" \
                   -backend-config="dynamodb_table=$PROJECT_NAME-terraform-locks"
    
    cd ..
    log "Terraform initialized"
}

# Create GitHub secrets template
create_github_secrets() {
    log "Creating GitHub secrets template..."
    
    cat > ../deploy/github-secrets.txt <<EOF
# GitHub Secrets Required for Deployment
# Add these secrets to your GitHub repository settings

AWS_ACCOUNT_ID=$AWS_ACCOUNT_ID
AWS_ACCESS_KEY_ID=<your-access-key>
AWS_SECRET_ACCESS_KEY=<your-secret-key>
AWS_REGION=$REGION
DATABASE_URL=<will-be-available-after-terraform-apply>
JWT_SECRET=<same-as-terraform.tfvars>
S3_BUCKET_NAME=$PROJECT_NAME-web-client
CLOUDFRONT_DISTRIBUTION_ID=<will-be-available-after-terraform-apply>
CODEDEPLOY_APP_NAME=<will-be-available-after-terraform-apply>
CODEDEPLOY_DEPLOYMENT_GROUP=<will-be-available-after-terraform-apply>
DB_HOST=<rds-endpoint-from-terraform>
DB_PORT=5432
DB_USER=snaketron
DB_PASS=<from-terraform-secrets-manager>
DB_NAME=snaketron
SLACK_WEBHOOK_URL=<optional>
EOF
    
    info "GitHub secrets template created at deploy/github-secrets.txt"
}

# Main execution
main() {
    info "Setting up AWS infrastructure for SnakeTron"
    info "Region: $REGION"
    
    check_prerequisites
    create_ecr_repositories
    create_terraform_backend
    init_terraform
    create_github_secrets
    
    log "AWS setup completed!"
    info "Next steps:"
    info "1. Update terraform/terraform.tfvars with your values"
    info "2. Run 'cd terraform && terraform plan' to review infrastructure"
    info "3. Run 'cd terraform && terraform apply' to create infrastructure"
    info "4. Add secrets from deploy/github-secrets.txt to your GitHub repository"
    info "5. Push to the 'release' branch to trigger deployment"
}

# Run main function
main "$@"