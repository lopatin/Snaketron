# SnakeTron AWS Deployment Guide

This directory contains all the scripts and configurations needed to deploy Snaketron to AWS using Fargate, following the plan in `specs/AWS_Fargate_Deployment_Plan.md`.

## Prerequisites

1. **AWS CLI** installed and configured with credentials
2. **Terraform** >= 1.0 installed
3. **Docker** installed for local testing
4. An AWS account with appropriate permissions
5. A GitHub repository with Actions enabled

## Quick Start

### 1. Initial AWS Setup

Run the setup script to create the basic AWS resources:

```bash
./deploy/setup-aws.sh
```

This will:
- Create ECR repositories for container images
- Set up S3 bucket and DynamoDB table for Terraform state
- Initialize Terraform with backend configuration
- Generate templates for GitHub secrets

### 2. Configure Terraform

Edit `terraform/terraform.tfvars` and set:
- `jwt_secret` - A secure secret for JWT token signing
- `certificate_arn` - (Optional) ACM certificate for HTTPS on ALB
- `cloudfront_certificate_arn` - (Optional) ACM certificate for CloudFront (must be in us-east-1)
- `client_domain_name` - (Optional) Custom domain for web client

### 3. Deploy Infrastructure

```bash
cd terraform
terraform plan  # Review the infrastructure to be created
terraform apply # Create the infrastructure
```

This creates:
- VPC and networking (uses default VPC)
- RDS PostgreSQL database (db.t4g.micro)
- ECS cluster and service (3 Fargate Spot tasks)
- Application Load Balancer
- S3 bucket and CloudFront for web client
- Auto-scaling configuration (3-9 tasks)

### 4. Configure GitHub Secrets

Add the following secrets to your GitHub repository (Settings → Secrets → Actions):

```
AWS_ACCOUNT_ID
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
DATABASE_URL         # From Terraform output
JWT_SECRET          # Same as terraform.tfvars
S3_BUCKET_NAME      # From Terraform output
CLOUDFRONT_DISTRIBUTION_ID  # From Terraform output
SLACK_WEBHOOK_URL   # Optional
```

### 5. Deploy Application

Push to the `release` branch to trigger automatic deployment:

```bash
git checkout -b release
git push origin release
```

The GitHub Actions workflow will:
1. Build and test the code
2. Build Docker images and push to ECR
3. Register new ECS task definition
4. Deploy game servers using AWS CodeDeploy (blue/green deployment)
5. Build and deploy web client to S3/CloudFront

## Architecture Overview

### Game Servers (ECS Fargate)
- 3-9 auto-scaling Fargate Spot containers
- Each container runs a game server with:
  - WebSocket endpoint (port 8080)
  - gRPC for inter-server communication (port 50051)
  - Raft consensus protocol (port 50052)
- Direct IP communication between containers
- SQL-based service discovery

### Web Client (Serverless)
- Static files hosted on S3
- CloudFront CDN for global distribution
- Automatic cache invalidation on deployment

### Database
- RDS PostgreSQL (db.t4g.micro)
- Used for:
  - Service discovery (server IPs)
  - User authentication
  - Game state persistence
  - Matchmaking

### Networking
- Containers discover their own IP via platform-specific methods
- Register IP in database on startup
- Query database to discover peer IPs
- Direct container-to-container communication

## Deployment Scripts

### `codedeploy-deploy.sh`
Triggers deployments using AWS CodeDeploy for blue/green deployments:
```bash
# Deploy using current task definition
./deploy/codedeploy-deploy.sh

# Deploy specific task definition
./deploy/codedeploy-deploy.sh --task-definition arn:aws:ecs:us-east-1:123456789012:task-definition/snaketron-server:42

# Deploy with custom description
./deploy/codedeploy-deploy.sh --description "Deploy version 2.0.1 with bug fixes"
```

Features:
- Blue/green deployments with traffic shifting
- Automatic rollback on failure
- Health check validation at each stage
- CloudWatch alarms monitoring
- Maintains Raft cluster quorum throughout deployment

### `setup-aws.sh`
Initial setup script for AWS resources:
```bash
./deploy/setup-aws.sh
```

## Cost Optimization

The deployment is optimized for low cost (~$50-60/month):
- Fargate Spot pricing (70% discount)
- Minimal container sizes (0.25 vCPU, 0.5GB RAM)
- db.t4g.micro RDS instance
- CloudWatch Logs with 1-day retention
- Serverless web hosting

## Monitoring

- CloudWatch Logs: `/ecs/snaketron-server`
- ECS task metrics in CloudWatch
- ALB metrics and access logs
- CloudFront distribution metrics

## Troubleshooting

### Container Can't Discover IP
- Check `IS_FARGATE` environment variable is set to `true`
- Verify ECS task has proper IAM permissions
- Check CloudWatch logs for IP discovery errors

### Raft Cluster Issues
- Ensure at least 3 containers are running
- Check database for registered servers
- Verify security groups allow ports 50051-50052

### Database Connection Failed
- Verify RDS security group allows access from ECS tasks
- Check DATABASE_URL format in task definition
- Ensure database migrations have run

### Web Client Not Updating
- Check CloudFront invalidation completed
- Verify S3 bucket policy allows CloudFront access
- Clear browser cache

## CodeDeploy Blue/Green Deployment

The deployment uses AWS CodeDeploy for zero-downtime blue/green deployments:

1. **Traffic Shifting**: Gradually shifts traffic from blue to green environment (10% every minute)
2. **Health Validation**: Validates health at each stage with automated rollback
3. **Raft Awareness**: Maintains cluster quorum throughout deployment
4. **CloudWatch Alarms**: Monitors response time and unhealthy hosts

### Deployment Stages
1. **BeforeInstall**: Verifies Raft cluster has minimum healthy nodes
2. **AfterInstall**: Confirms new containers registered successfully
3. **AfterAllowTestTraffic**: Validates new deployment with test traffic
4. **BeforeAllowTraffic**: Final health check before production traffic
5. **AfterAllowTraffic**: Verifies deployment success after traffic shift

## Rollback

CodeDeploy automatically handles rollback in case of:
- Deployment failures
- CloudWatch alarm triggers
- Manual stop request

To manually rollback:
```bash
# Stop ongoing deployment (triggers automatic rollback)
aws deploy stop-deployment --deployment-id <deployment-id> --auto-rollback-enabled

# Or redeploy previous version
./deploy/codedeploy-deploy.sh --task-definition <previous-task-definition-arn>
```

## Local Development

The same code works locally with Docker Compose:
```bash
docker-compose up
```

Containers will use Docker networking instead of Fargate networking.