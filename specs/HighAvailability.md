# High Availability

The Snaketron server is designed for high availability. When one server is terminated, another server automatically takes over the game state, and websockets reconnect to a new server as well.

## Design

Each server is a single Rust process on its own host in the same network. The Rust process contains the following components:

- **GameManager**: Runs the authoritative game state and tick loop for games assigned to this server.
- **WebSocket Server**: A WebSocket server that clients connect to for sending commands and receiving game updates.
- **ServiceManager**: Uses Raft to manage the cluster of servers. Handles service discovery.
- **ReplicaManager**: Uses Raft to replicate game state across servers, ensuring that if one server goes down, another can take over without losing game state.
- **gRPC**: A streaming gRPC server and client that is used as the network backend for Raft.
- **Raft**: Implements the Raft consensus algorithm for leader election and state replication across servers.

## Data Flow
The WebSocket connection sends game commands to the GameManager, if the game is running on this server. 
If the game is not running on this server, the command is sent to the gRPC client, which forwards it to the appropriate server.
The GameManager runs the game loop for a single GameState by applying game commands (including system commands like Tick).
Each tick in the loop emits events, which are replicated to the other servers via Raft.
Rust channels are used for intra-server message passing, and Raft is used for inter-server coordination.

## Scenarios

### Automatic Failover

1. Servers A and B are running in the same region.
2. A is sent a SIGTERM signal to begin graceful shutdown.
3. Each connected WebSocket is sent a shutdown message. It will reconnect to another server to drain A.
4. Fails over each game in its GameManager:
    - Queries Raft for the least loaded server (B).
    - Sets the game state `host` field to B.
    - B realizes that it is now the host for the game and starts running the game loop.
