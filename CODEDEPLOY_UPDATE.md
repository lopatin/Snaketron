# CodeDeploy Integration Update

## Overview
I've updated the SnakeTron deployment process to use AWS CodeDeploy instead of custom rolling deployment scripts. This provides a more robust, AWS-native solution with blue/green deployments, automatic rollback, and better monitoring.

## What Changed

### 1. **Terraform Infrastructure**
- Added new `codedeploy` module with:
  - CodeDeploy application and deployment group
  - Blue/green target groups for ALB
  - CloudWatch alarms for deployment monitoring
  - SNS topic for deployment notifications
  - IAM roles with proper permissions

### 2. **Load Balancer Configuration**
- Updated to use blue/green target groups
- ALB now supports traffic shifting between environments
- Health checks configured for both target groups

### 3. **GitHub Actions Workflow**
- Replaced direct ECS service update with CodeDeploy deployment
- Workflow now:
  1. Builds and pushes Docker image
  2. Registers new ECS task definition
  3. Creates CodeDeploy deployment
  4. Monitors deployment progress
  5. Automatic rollback on failure

### 4. **Deployment Scripts**
- **Removed**: `rolling-update.sh` (renamed to `.old`)
- **Added**: `codedeploy-deploy.sh` for manual deployments
- **Added**: `codedeploy-hooks/` with Lambda functions for validation

### 5. **Deployment Validation Hooks**
- **BeforeInstall**: Verifies Raft cluster health
- **AfterInstall**: Confirms new containers started
- **AfterAllowTestTraffic**: Validates with test traffic
- **BeforeAllowTraffic**: Final health check
- **AfterAllowTraffic**: Post-deployment verification

## Benefits

### Blue/Green Deployments
- Zero-downtime deployments
- Traffic gradually shifted (10% per minute)
- Old environment kept until deployment succeeds
- Instant rollback capability

### Better Monitoring
- CloudWatch alarms for response time and unhealthy hosts
- Deployment notifications via SNS
- Detailed deployment lifecycle visibility
- Automatic rollback on alarm triggers

### Raft Cluster Safety
- Maintains quorum throughout deployment
- Validates cluster health at each stage
- Never deploys if cluster is unhealthy
- Graceful handling of leader changes

## Usage

### Automatic Deployment (GitHub Actions)
Push to `release` branch triggers deployment automatically.

### Manual Deployment
```bash
# Deploy current task definition
./deploy/codedeploy-deploy.sh

# Deploy specific version
./deploy/codedeploy-deploy.sh --task-definition arn:aws:ecs:...

# With description
./deploy/codedeploy-deploy.sh --description "Bug fix for issue #123"
```

### Monitoring Deployments
1. AWS Console → CodeDeploy → Deployments
2. CloudWatch → Alarms (for health metrics)
3. SNS notifications (if email configured)

### Rollback
```bash
# Stop and rollback active deployment
aws deploy stop-deployment --deployment-id <id> --auto-rollback-enabled

# Or deploy previous version
./deploy/codedeploy-deploy.sh --task-definition <previous-arn>
```

## Configuration

### Required GitHub Secrets
```
CODEDEPLOY_APP_NAME         # From Terraform output
CODEDEPLOY_DEPLOYMENT_GROUP # From Terraform output
DB_HOST                     # RDS endpoint
DB_PORT                     # 5432
DB_USER                     # snaketron
DB_PASS                     # From Secrets Manager
DB_NAME                     # snaketron
```

### Terraform Variables
```hcl
deployment_notification_email = "ops@example.com"
enable_deployment_alarms = true
```

## Migration Notes

1. **First Deployment**: Will create green environment alongside existing blue
2. **No Downtime**: Existing service continues running during migration
3. **Rollback Ready**: Can revert to old deployment method if needed
4. **Cost Neutral**: No additional costs for CodeDeploy service

## Next Steps

1. Run `terraform plan` to review infrastructure changes
2. Run `terraform apply` to create CodeDeploy resources
3. Update GitHub secrets with CodeDeploy values
4. Test deployment on staging environment first
5. Deploy to production via release branch

The CodeDeploy integration provides enterprise-grade deployment capabilities while maintaining the simplicity and cost-efficiency of the original design.