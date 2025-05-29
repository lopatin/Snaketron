# High Availability

The Snaketron server is designed for high availability. When one server is terminated, another server automatically takes over the game state, and websockets reconnect to a new server as well.

## Design

Each server is a single Rust process on its own host in the same network. The Rust process contains the following components:

- **GameManager**: Runs the authoritative game state and tick loop for games assigned to this server.
- **WebSocket Server**: A WebSocket server that clients connect to for sending commands and receiving game updates.
- **ServiceManager**: Handles service discovery. Bootstraps its knowledge of the cluster by querying the database, and then makes gRPC connections to the other servers in the cluster.
- **ReplicaManager**: Uses gRPC to replicate game state across servers. The replicas are used for game failover.
- **gRPC**: A streaming gRPC server and client that is used to replicate game state between servers.

## Data Flow
The WebSocket connection sends game commands to the GameManager, if the game is running on this server. 
If the game is not running on this server, the command is sent to the gRPC client, which forwards it to the appropriate server.
The GameManager runs the game loop for a single GameState by applying game commands (including system commands like Tick).
Each tick in the loop emits events, which are replicated to the other servers via gRPC.
Rust channels are used for intra-server message passing, and gRPC is used for inter-server coordination.

## Scenarios

### Automatic Failover

1. Servers A and B are running in the same region.
2. A is sent a SIGTERM signal to begin graceful shutdown.
3. Each connected WebSocket is sent a shutdown message. It will reconnect to another server to drain A of websocket connections.
4. Fails over each game in its GameManager:
    - Queries the service manager for the least loaded server (B).
    - Updates the database to indicate that B is now the authoritative server for the game.
    - All servers are notified of the change via gRPC.
    - B realizes it is now the authoritative server for the game and starts running the game loop.
    - B notifies all servers that it has started running the game, which means that it is now the authority for game events which get sent to clients. The messaging layer should be aware of this, as any events sent from A will now need to be dropped.
    - A can shut down the game, everyone has stopped listening to it.
