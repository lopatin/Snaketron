# AWS Fargate Deployment Plan for SnakeTron

## Overview

This document outlines a cost-optimized deployment strategy for SnakeTron on AWS, including:
- 3-9 game server containers running as a Raft cluster (auto-scaling with Fargate Spot)
- Serverless web client using Lambda@Edge and CloudFront
- Rolling deployments one node at a time
- GitHub Actions CI/CD pipeline triggered on `release` branch
- SQL-based service discovery (platform agnostic)
- Direct IP communication between containers
- **Target monthly cost: ~$50-60** for 3 nodes + minimal Lambda costs

## Architecture

### AWS Resources

#### 1. Networking
- **VPC**: Default VPC (no additional cost)
- **Subnets**: Use existing default subnets
- **Security Groups**:
  - Game server SG: 
    - Inbound: WebSocket (8080) from ALB
    - Inbound: gRPC (50051) from game server SG (self-referencing)
    - Inbound: Raft (50052) from game server SG (self-referencing)
    - Outbound: All traffic
  - Web client SG: Allow HTTP/HTTPS (80/443) from ALB
  - ALB SG: Allow HTTP/HTTPS (80/443) from internet
  - RDS SG: Allow PostgreSQL (5432) from game servers only

#### 2. Container Infrastructure
- **ECR Repositories**:
  - `snaketron-server`: Game server images
  - `snaketron-client`: Web client images

- **ECS Cluster**: `snaketron-cluster`

- **ECS Service** (Game Servers Only):
  - `snaketron-game-service`: 
    - Initial: 3 Fargate Spot tasks (one per AZ)
    - Auto-scaling: 3-9 tasks based on CPU/memory
    - Rolling updates: 1 task at a time
    - Health check grace period: 120 seconds

- **Task Definition** (Game Server):
  - 0.25 vCPU, 0.5GB RAM per task (Fargate Spot)
  - Port mappings: 8080 (WebSocket), 50051 (gRPC), 50052 (Raft)
  - awsvpc network mode (required for Fargate)
  - Health check: HTTP GET /health

- **Auto-scaling Configuration**:
  - Target CPU: 70%
  - Target Memory: 80%
  - Scale-out cooldown: 60 seconds
  - Scale-in cooldown: 300 seconds
  - Min tasks: 3, Max tasks: 9

- **Web Client (Serverless)**:
  - S3 bucket for static assets (HTML, JS, CSS, WASM)
  - CloudFront distribution with caching
  - Lambda@Edge for dynamic routing if needed
  - Cost: ~$0.20/million requests + minimal S3 storage

#### 3. Load Balancing and CDN
- **Application Load Balancer** (for game servers only)
  - WebSocket connections to game servers
  - SSL termination
  - Health checks on `/health` endpoint
  
- **CloudFront Distribution** (for web client)
  - Origin: S3 bucket with static assets
  - Global edge caching
  - HTTPS with AWS Certificate Manager (free)
  - Compression enabled
  - Cost: Pay per request (~$0.01/10K requests)

#### 4. Database
- **RDS PostgreSQL**: Single-AZ deployment (cost-optimized)
  - Instance: db.t4g.micro (Graviton2, 1 vCPU, 1GB RAM)
  - Storage: 20GB GP3 (can auto-scale if needed)
  - Automated backups: 1 day retention
  - Option: Consider Aurora Serverless v2 with 0.5 ACU minimum for pay-per-use

#### 5. Service Discovery via SQL Database (Platform Agnostic)

##### How Containers Get IPs

**In AWS Fargate:**
- Each task gets a private IP from the VPC subnet (e.g., 10.0.1.5)
- Containers discover their own IP via ECS metadata endpoint:
  ```bash
  curl ${ECS_CONTAINER_METADATA_URI_V4}/task | jq -r '.Containers[0].Networks[0].IPv4Addresses[0]'
  ```
- All containers in the same VPC can reach each other via private IPs

**In Local Docker Development:**
- Containers get IPs from Docker's bridge network (e.g., 172.17.0.2)
- Containers discover their own IP via:
  ```bash
  hostname -i  # or
  ip addr show eth0 | grep inet | awk '{print $2}' | cut -d/ -f1
  ```
- All containers on the same Docker network can reach each other

##### SQL-Based Service Discovery
1. **On Startup**: Each server:
   - Discovers its own IP (platform-specific method)
   - Registers in PostgreSQL `servers` table with IP and ports
   - Updates heartbeat timestamp

2. **Peer Discovery**: 
   - Query database for other healthy servers
   - Connect directly using private IPs
   - No AWS-specific services required

3. **Database Schema** (already exists):
   - Servers table with columns: id, ip_address, grpc_port, raft_port, last_heartbeat
   - Used for service registration and health tracking

## CI/CD Pipeline

### GitHub Secrets Required
```yaml
AWS_ACCOUNT_ID
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
DATABASE_URL
JWT_SECRET
S3_BUCKET_NAME              # For web client assets
CLOUDFRONT_DISTRIBUTION_ID  # For cache invalidation
SLACK_WEBHOOK_URL (optional)
```

### GitHub Actions Workflows

#### 1. Build and Test (`build-test.yml`)
```yaml
on:
  pull_request:
  push:
    branches: [main, master, release]

jobs:
  test:
    - Run cargo test
    - Run cargo clippy
    - Build WASM client
    - Run client tests
```

#### 2. Deploy to Production (`deploy.yml`)
```yaml
on:
  push:
    branches: [release]  # Only deploy from release branch

jobs:
  build-and-push:
    - Build server Docker image
    - Build client Docker image
    - Push to ECR
  
  deploy-server:
    - Update ECS task definition
    - Configure rolling deployment (1 task at a time)
    - Wait for each task to be healthy before proceeding
    - Monitor Raft cluster health during deployment
  
  deploy-client:
    - Build WASM and React app
    - Upload to S3 bucket
    - Invalidate CloudFront cache
    - Zero-downtime deployment (old files remain cached)
```

## Rolling Deployment Strategy for Raft Cluster

### Phase 1: Preparation
1. Verify minimum 3 healthy nodes in cluster
2. Identify current Raft leader via database
3. Create new task definition revision
4. Set ECS service to update 1 task at a time

### Phase 2: Rolling Update (One Node at a Time)
```
For each server (starting with followers):
  1. ECS starts new task with updated image
  2. New task discovers its IP and registers in database
  3. New task queries database for peer IPs
  4. New task joins Raft cluster using peer IPs
  5. Wait for Raft cluster to report healthy (via database)
  6. Old task receives SIGTERM, drains connections
  7. Old task removes itself from database
  8. Old task shuts down gracefully
  9. Wait 60 seconds before next server
```

### Phase 3: Leader Update
1. Trigger Raft leadership transfer (if current task is leader)
2. Wait for new leader election (monitor via database)
3. Update former leader using same process

### Auto-scaling Considerations
- New nodes (4-9) join as followers only
- Scale-in removes followers first, never the leader
- Maintain minimum 3 nodes for Raft quorum

### Health Checks
- ECS health check: `/health` endpoint
- Raft health: Stored in database
- Minimum nodes: Alert if < 3 healthy nodes

## Implementation Steps

### Step 1: Build and Deploy Web Client to S3/CloudFront

#### Build Process (GitHub Actions)
```yaml
# Build WASM and React app
- name: Build WASM
  run: |
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
    cd client && wasm-pack build --target web --out-dir pkg

- name: Build React App
  run: |
    cd client/web
    npm ci
    npm run build
    
# Deploy to S3
- name: Deploy to S3
  run: |
    aws s3 sync client/web/build s3://${{ secrets.S3_BUCKET_NAME }} \
      --delete \
      --cache-control "public, max-age=31536000" \
      --exclude "index.html" \
      --exclude "*.json"
    
    # Upload index.html with no-cache
    aws s3 cp client/web/build/index.html s3://${{ secrets.S3_BUCKET_NAME }}/ \
      --cache-control "no-cache, no-store, must-revalidate"
      
# Invalidate CloudFront
- name: Invalidate CloudFront
  run: |
    aws cloudfront create-invalidation \
      --distribution-id ${{ secrets.CLOUDFRONT_DISTRIBUTION_ID }} \
      --paths "/*"
```

### Step 2: Create Terraform Infrastructure
```
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── modules/
│   ├── networking/
│   ├── ecs/
│   ├── rds/
│   └── load-balancing/
```

#### Terraform Configuration

##### Game Server Infrastructure
```hcl
# ECS service for game servers only
resource "aws_ecs_service" "game_server" {
  name            = "snaketron-game-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.game_server.arn
  desired_count   = 3  # Initial count
  
  deployment_configuration {
    maximum_percent         = 133  # Allow 1 extra during deploy
    minimum_healthy_percent = 67   # Keep 2/3 running
    deployment_circuit_breaker {
      enable   = true
      rollback = true
    }
  }
  
  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.game_server.id]
  }
  
  capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight           = 100
  }
  
  health_check_grace_period_seconds = 120
}

# Auto-scaling configuration
resource "aws_appautoscaling_target" "ecs_target" {
  max_capacity       = 9
  min_capacity       = 3
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.game_server.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

# ALB for WebSocket connections
resource "aws_lb_target_group" "game_websocket" {
  name        = "snaketron-game-ws"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = aws_vpc.default.id
  target_type = "ip"
  
  health_check {
    path                = "/health"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 30
  }
}
```

##### Web Client Infrastructure (Serverless)
```hcl
# S3 bucket for static assets
resource "aws_s3_bucket" "web_client" {
  bucket = "snaketron-web-client"
}

resource "aws_s3_bucket_public_access_block" "web_client" {
  bucket = aws_s3_bucket.web_client.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "web_client" {
  bucket = aws_s3_bucket.web_client.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowCloudFrontAccess"
        Effect    = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.web_client.arn}/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = aws_cloudfront_distribution.web_client.arn
          }
        }
      }
    ]
  })
}

# CloudFront distribution
resource "aws_cloudfront_distribution" "web_client" {
  enabled             = true
  is_ipv6_enabled     = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"  # US, Canada, Europe only (cheaper)

  origin {
    domain_name              = aws_s3_bucket.web_client.bucket_regional_domain_name
    origin_id                = "S3-${aws_s3_bucket.web_client.id}"
    origin_access_control_id = aws_cloudfront_origin_access_control.web_client.id
  }

  default_cache_behavior {
    allowed_methods  = ["GET", "HEAD"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = "S3-${aws_s3_bucket.web_client.id}"

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 86400
    max_ttl                = 31536000
    compress               = true
  }

  # Cache behavior for index.html (no cache)
  ordered_cache_behavior {
    path_pattern     = "/index.html"
    allowed_methods  = ["GET", "HEAD"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = "S3-${aws_s3_bucket.web_client.id}"

    forwarded_values {
      query_string = false
      cookies {
        forward = "none"
      }
    }

    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 0
    max_ttl                = 0
  }

  custom_error_response {
    error_code         = 404
    response_code      = 200
    response_page_path = "/index.html"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

# Origin access control for S3
resource "aws_cloudfront_origin_access_control" "web_client" {
  name                              = "snaketron-oac"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}
```

### Step 3: Environment Configuration

#### Server Environment Variables
```
DATABASE_URL=<from secrets>
JWT_SECRET=<from secrets>
RUST_LOG=info
# Server discovers its own ID and IP at runtime
# No hardcoded peer configuration needed
# Container's own address for binding
BIND_ADDR=0.0.0.0
GRPC_PORT=50051
RAFT_PORT=50052
WS_PORT=8080
# Platform detection
IS_FARGATE=true  # Set by ECS task definition
# Memory optimization for small containers
RUST_MIN_STACK=1048576  # 1MB stack
```

#### Client Environment Variables (Build Time)
```
REACT_APP_WS_URL=wss://api.snaketron.io
REACT_APP_API_URL=https://api.snaketron.io
# Production optimizations
NODE_ENV=production
GENERATE_SOURCEMAP=false
PUBLIC_URL=https://play.snaketron.io
```

### Step 4: SQL-Based Service Discovery Implementation

#### Server Startup Process
1. **Discover Own IP**:
   - In Fargate: Use ECS metadata endpoint
   - In Docker: Use hostname or ip command
   - Platform detection via IS_FARGATE environment variable

2. **Register in Database**:
   ```sql
   INSERT INTO servers (id, ip_address, grpc_port, raft_port, last_heartbeat)
   VALUES ($1, $2, 50051, 50052, NOW())
   ON CONFLICT (id) DO UPDATE SET 
       ip_address = $2,
       last_heartbeat = NOW();
   ```

3. **Discover Peers**:
   ```sql
   SELECT id, ip_address, grpc_port, raft_port 
   FROM servers 
   WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
   AND id != $1;
   ```

4. **Join Raft Cluster**:
   - Connect to discovered peers using their IPs
   - Form or join existing Raft cluster
   - Start heartbeat thread to update database

#### Network Architecture (Direct IP Communication)
```
Internet → ALB → ECS Tasks (port 8080 only)
                     
     Direct IP communication between containers
     
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Task 1    │←--→│   Task 2    │←--→│   Task 3    │
│ 10.0.1.5    │    │ 10.0.1.12   │    │ 10.0.2.8    │
└─────────────┘    └─────────────┘    └─────────────┘
   ↑                                          ↑
   └──────── Direct gRPC/Raft via IPs ────────┘
              (discovered from database)
              
When scaled up:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Tasks 4-6  │    │  Tasks 7-9  │    │ (up to 9)   │
│ 10.0.1.x    │    │ 10.0.2.x    │    │ 10.0.3.x    │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Step 5: Monitoring and Observability (Cost-Optimized)

- **CloudWatch Logs**: 
  - 1-day retention for cost savings
  - Log level: WARN and above only
  - Compressed log groups
- **CloudWatch Metrics**: 
  - Basic ECS metrics only (free tier)
  - No custom metrics initially
- **Alarms** (free tier - 10 alarms):
  - All tasks unhealthy
  - Database connection failures
  - ALB 5XX errors > 10/min

## Security Considerations

1. **Secrets Management**:
   - Use AWS Secrets Manager for runtime secrets
   - GitHub secrets for CI/CD only
   - Rotate secrets quarterly

2. **Network Security**:
   - Private subnets for game servers and RDS
   - WAF rules for web client
   - VPC flow logs enabled

3. **IAM Roles**:
   - Minimal permissions for ECS tasks
   - Separate roles for CI/CD

## Cost Optimization Strategy

### Infrastructure Costs (Estimated Monthly)
- **Fargate Spot Tasks**: ~$12-15/month
  - Game servers only: 3 × 0.25 vCPU × 0.5GB RAM × 24/7
  - 70% discount with Spot pricing
  
- **RDS Database**: ~$15/month
  - db.t4g.micro: ~$12/month
  - 20GB storage: ~$3/month
  - Alternative: Aurora Serverless v2 at 0.5 ACU: ~$30/month but scales to zero
  
- **Load Balancer**: ~$20/month
  - Single ALB for game servers
  - Data transfer costs additional
  
- **Web Client (Serverless)**: ~$1-3/month
  - S3 storage: ~$0.50/month (10GB)
  - CloudFront: ~$0.50/month (light traffic)
  - Lambda@Edge: ~$0 (free tier covers most usage)
  
- **Total**: ~$50-60/month base cost (3 nodes)
  - Scales up to ~$85-100/month at max capacity (9 nodes)

### Cost Reduction Strategies

1. **Development/Testing Environment**:
   - Use single game server instead of 3
   - Share ALB using host-based routing
   - Stop resources when not in use

2. **Production Optimizations**:
   - Use Fargate Spot for all containers (70% savings)
   - Auto-scaling adjusts costs based on load (3-9 nodes)
   - Consider EC2 t4g.nano instances (~$3/month each) for ultra-low cost
   - Use CloudFront for static as``sets (pay per request)
   - Enable S3 lifecycle policies for logs

3. **Database Options**:
   - Start with db.t4g.micro
   - Consider DynamoDB for session/game state (pay-per-request)
   - Use SQLite on EFS for development ($0.30/GB/month)

4. **Monitoring Costs**:
   - Use CloudWatch Logs with 1-day retention
   - Basic metrics only (no custom metrics initially)
   - Use free tier for X-Ray traces

### Scaling Cost Impact
- 3 nodes: ~$50-60/month (base)
- 6 nodes: ~$70-80/month
- 9 nodes: ~$85-100/month
- Web client: ~$1-3/month regardless of scale
- Scales automatically based on CPU/memory usage

## Container-to-Container Communication Details

### Direct IP Communication Benefits
- **No ALB latency**: Direct gRPC/Raft communication bypasses load balancer
- **Cost savings**: Internal traffic doesn't go through ALB
- **Platform agnostic**: Same approach works locally and in AWS
- **No vendor lock-in**: Uses standard PostgreSQL, not AWS-specific services
- **Simple debugging**: Just IP addresses, no complex service mesh

### Implementation Notes

1. **Server Code Changes (SQL-Based Discovery)**:
   - Discover own IP using platform-specific method
   - Register self in PostgreSQL database with IP and ports
   - Query database for other healthy servers
   - Connect to peers using their private IPs
   - Update heartbeat every 10 seconds
   - Clean up stale entries (> 60 seconds old)

2. **Security Group Rules** (must allow internal traffic):
   ```
   Ingress:
   - Port 50051 from source: sg-gameserver (self)
   - Port 50052 from source: sg-gameserver (self)
   - Port 8080 from source: sg-alb
   ```

3. **Cross-Platform Compatibility**:
   - Same code works in Docker Compose and AWS Fargate
   - No AWS-specific dependencies for service discovery
   - Database-driven discovery works anywhere
   - Easy to test locally with same discovery mechanism

## Rollback Strategy

1. **Automated Rollback Triggers**:
   - Health check failures
   - Raft cluster instability
   - Error rate > 5%

2. **Manual Rollback Process**:
   ```bash
   aws ecs update-service --cluster snaketron-cluster \
     --service snaketron-game-service \
     --task-definition snaketron-server:<previous-revision>
   ```

## Testing Strategy

### Pre-deployment Tests
1. Integration tests with test Raft cluster
2. Load testing with simulated players
3. Chaos testing (random node failures)

### Post-deployment Validation
1. Verify all 3 nodes in Raft cluster
2. Test game creation and joining
3. Verify WebSocket connections
4. Check monitoring dashboards

## Ultra-Low Cost Alternative: EC2-Based Deployment

For extreme cost savings (~$15-20/month total):

### Option 1: Single EC2 Instance
- **Instance**: t4g.small (2 vCPU, 2GB RAM) - ~$12/month
- Run all 3 game servers on one instance
- Use Docker Compose for orchestration
- Web client still on S3 + CloudFront
- RDS: db.t4g.micro or SQLite on instance

### Option 2: Spot Instances
- **Instances**: 3 × t4g.nano (2 vCPU, 0.5GB RAM) - ~$1/month each with Spot
- One instance per game server
- Web assets on S3 + CloudFront
- Use EFS for shared state if needed

### Full Serverless Alternative
- **Lambda**: Game servers as Lambda functions (challenging for WebSockets)
- **API Gateway WebSockets**: For real-time connections
- **DynamoDB**: For game state (pay-per-request)
- **S3 + CloudFront**: Static web hosting (already implemented)
- Cost: ~$0 base, pay only for usage
- Note: WebSocket connections on Lambda have 2-hour limit

## Implementation Timeline

### Phase 1: Minimal Viable Deployment (Week 1)
- Use AWS Free Tier where possible
- Deploy single game server initially
- Manual deployment scripts
- Basic monitoring only

### Phase 2: Add Redundancy (Week 2)
- Scale to 3 game servers
- Implement Raft cluster
- Add GitHub Actions CI/CD

### Phase 3: Production Ready (Week 3)
- Add Fargate Spot
- Optimize container sizes
- Implement cost monitoring
- Add auto-scaling policies

## Web Client Deployment Details

### Benefits of Serverless Web Client
- **Zero server management**: No containers to maintain
- **Global distribution**: CloudFront edge locations worldwide
- **Instant scaling**: Handles any traffic spike
- **Cost efficiency**: Pay only for requests and storage
- **Better performance**: Static assets cached at edge

### Deployment Process
1. GitHub Actions builds WASM and React bundle
2. Assets uploaded to S3 with proper cache headers
3. CloudFront invalidation ensures users get latest version
4. Zero downtime - old version stays cached until invalidation

### Configuration
- **S3**: Private bucket, CloudFront-only access
- **CloudFront**: HTTPS, compression, global caching
- **Cache Strategy**:
  - Static assets (JS, CSS, WASM): 1 year cache
  - index.html: No cache (always fresh)
  - 404 → 200 redirect for client-side routing

## Maintenance Procedures

### Daily
- Monitor CloudWatch dashboards
- Check Raft cluster health
- Review CloudFront analytics

### Weekly
- Review error logs
- Analyze performance metrics
- Check S3 storage usage

### Monthly
- Security patches
- Dependency updates
- Cost optimization review
- CloudFront cache analysis

### Quarterly
- Disaster recovery drill
- Secret rotation
- Architecture review