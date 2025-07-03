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
  - API/Static file server (port 3001)
  - gRPC for inter-server communication (port 50051)
  - Raft consensus protocol (port 50052)
- Direct IP communication between containers
- SQL-based service discovery
- Static web assets served directly from the Rust server

### Database
- RDS PostgreSQL (db.t4g.micro)
- Used for:
  - Service discovery (server IPs)
  - User authentication
  - Game state persistence
  - Matchmaking

### Networking
- Network Load Balancer (NLB) for TCP traffic
  - Port 80/443: Routes to API/static file server
  - Port 8080: Routes to WebSocket server
- Containers discover their own IP via platform-specific methods
- Register IP in database on startup
- Query database to discover peer IPs
- Direct container-to-container communication for Raft/gRPC

## Deployment Scripts

### Simple ECS Deployment
The deployment process is simplified with direct ECS deployments:
- No blue/green complexity - NLB handles connection draining
- Rolling updates maintain service availability
- New tasks start before old ones are stopped
- Health checks ensure new containers are ready

To deploy manually:
```bash
# Update the ECS service with a new task definition
aws ecs update-service \
  --cluster snaketron-prod-cluster \
  --service snaketron-prod-game-service \
  --task-definition snaketron-prod-server:latest
```

Features:
- Simple rolling deployments
- Automatic connection draining via NLB
- Maintains Raft cluster quorum during updates
- No additional infrastructure complexity

### `setup-aws.sh`
Initial setup script for AWS resources:
```bash
./deploy/setup-aws.sh
```

## Cost Optimization

The deployment is optimized for low cost (~$40-50/month):
- Fargate Spot pricing (70% discount)
- Minimal container sizes (0.25 vCPU, 0.5GB RAM)
- db.t4g.micro RDS instance
- CloudWatch Logs with 1-day retention
- NLB is cheaper than ALB
- No S3/CloudFront costs

## Monitoring

- CloudWatch Logs: `/ecs/snaketron-server`
- ECS task metrics in CloudWatch
- NLB metrics and flow logs
- Target group health metrics

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

### Web Client Not Loading
- Check NLB target group health for port 3001
- Verify API server is running on port 3001
- Check browser console for connection errors
- Ensure SNAKETRON_WEB_DIR is set to /app/web

## Rolling Deployments

The deployment uses ECS rolling updates for zero-downtime deployments:

1. **Health Checks**: New containers must pass health checks before receiving traffic
2. **Connection Draining**: NLB automatically drains connections from old containers
3. **Gradual Rollout**: New tasks are started before old ones are stopped
4. **Automatic Rollback**: ECS can automatically rollback if deployment fails

### Deployment Process
1. New task definition is registered with updated container image
2. ECS starts new tasks with the new definition
3. Health checks verify new containers are ready
4. NLB gradually shifts traffic to new containers
5. Old containers are drained and stopped

## Rollback

To manually rollback to a previous version:
```bash
# List recent task definitions
aws ecs list-task-definitions --family-prefix snaketron-prod-server

# Update service with previous task definition
aws ecs update-service \
  --cluster snaketron-prod-cluster \
  --service snaketron-prod-game-service \
  --task-definition snaketron-prod-server:previous-revision
```

## Local Development

The same code works locally with Docker Compose:
```bash
docker-compose up
```

Containers will use Docker networking instead of Fargate networking.