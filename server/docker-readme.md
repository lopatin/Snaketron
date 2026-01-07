# Docker Setup for SnakeTron Server

## Local Development

### Quick Start

```bash
# Build and start both database and server
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f server

# Stop services
docker-compose down
```

### Accessing the Server

- WebSocket: `ws://localhost:8080`
- gRPC: `localhost:50051`
- Database: `localhost:5432`

### Volumes

- Replays are saved to `./replays` directory (mounted as volume)

## Production Deployment (AWS Fargate)

### Build and Push to ECR

```bash
# Build production image
docker build -f server/Dockerfile -t snaketron-server .

# Tag for ECR
docker tag snaketron-server:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/snaketron-server:latest

# Push to ECR
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/snaketron-server:latest
```

### Environment Variables for Fargate

Required environment variables:
- `SNAKETRON_DB_HOST`: RDS endpoint
- `SNAKETRON_DB_PORT`: Database port (5432)
- `SNAKETRON_DB_USER`: Database username
- `SNAKETRON_DB_PASS`: Database password
- `SNAKETRON_DB_NAME`: Database name
- `SNAKETRON_REGION`: AWS region
- `SNAKETRON_WS_PORT`: WebSocket port (8080)
- `SNAKETRON_GRPC_PORT`: gRPC port (50051)

### Task Definition Configuration

- Memory: 512 MB (minimum)
- CPU: 256 units (0.25 vCPU)
- Network mode: awsvpc
- Exposed ports: 8080 (WebSocket), 50051 (gRPC)

### Health Check

The server exposes WebSocket on port 8080. For ALB health checks, you may need to implement an HTTP health endpoint.

## Troubleshooting

### Database Connection Issues

If the server can't connect to the database:
1. Check that the database container is healthy: `docker-compose ps`
2. Verify environment variables are correct
3. Ensure the database has been initialized with migrations

### Build Issues

If the build fails:
1. Ensure you have sufficient disk space
2. Try cleaning Docker cache: `docker system prune`
3. Check that all Rust dependencies are available

### Performance Tuning

For production:
- Adjust `RUST_LOG` level (default: info)
- Configure appropriate CPU/memory limits
- Use AWS RDS for database with proper instance size