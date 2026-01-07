# Port Configuration

This document clarifies the port assignments for different services in the SnakeTron application.

## Development Setup

### Docker Services (via docker-compose)
- **PostgreSQL Database**: Port 5432
- **WebSocket Server**: Port 8080
- **gRPC Server**: Port 50051

### Client Development
- **Webpack Dev Server**: Port 3000

## How It Works

1. **Backend Services** run in Docker containers:
   ```bash
   docker-compose up --build
   ```
   - The game server listens on port 8080 for WebSocket connections
   - The gRPC server listens on port 50051 for inter-server communication
   - PostgreSQL database is available on port 5432

2. **Frontend Development** runs separately:
   ```bash
   cd client/web
   npm start
   ```
   - The webpack-dev-server hosts the React app on port 3000
   - The React app connects to the WebSocket server at `ws://localhost:8080/ws`

## No Port Conflicts

- Frontend development server: **Port 3000**
- Backend WebSocket server: **Port 8080**
- No conflicts! 🎉

## Production Configuration

In production, you would typically:
- Serve the built React app from a CDN or static file server
- Connect to the WebSocket server via a load balancer or API gateway
- Use environment variables to configure the WebSocket URL