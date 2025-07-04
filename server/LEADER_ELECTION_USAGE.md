# Redis Leader Election Module Usage

This module provides simple leader election using Redis SET with NX (set if not exists) and automatic lease renewal.

## Configuration

Set the `SNAKETRON_REDIS_URL` environment variable to enable leader election:

```bash
export SNAKETRON_REDIS_URL="redis://localhost:6379"
```

## How it Works

1. **Leader Election**: Every 1 second, non-leader servers try to claim leadership using Redis SET NX with a 2-second TTL
2. **Lease Renewal**: The leader renews its lease every 300ms using an atomic Lua script
3. **Service Management**: When a server becomes leader, it starts the user-provided service. When it loses leadership, the service is stopped.

## Customizing the Service

To run your own service when elected as leader, modify the service closure in `game_server.rs`:

```rust
// Example service that runs only on the leader
let service = || Box::pin(async move {
    info!("This server is now the leader!");
    
    // Your custom logic here, for example:
    // - Run periodic maintenance tasks
    // - Coordinate distributed operations
    // - Manage singleton services
    
    // Keep the service running
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("Leader service is running...");
    }
    
    Ok::<(), anyhow::Error>(())
}) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
```

## Redis Key Structure

The leader election uses the key: `snaketron:leader:{region}`

For example:
- `snaketron:leader:us-east-1`
- `snaketron:leader:eu-west-1`

## Testing

To test leader election with multiple servers:

1. Start Redis:
   ```bash
   docker run -d -p 6379:6379 redis:latest
   ```

2. Start multiple server instances with the same region:
   ```bash
   SNAKETRON_REDIS_URL=redis://localhost:6379 SNAKETRON_REGION=test cargo run --bin server
   ```

3. Observe the logs - only one server should claim leadership at a time.

## Notes

- The module is completely optional - if `SNAKETRON_REDIS_URL` is not set, leader election is disabled
- Leader election is separate from the existing Raft consensus system
- The lease duration is set to 2 seconds with renewal every 300ms to handle network delays
- If Redis connection fails, the server loses leadership automatically