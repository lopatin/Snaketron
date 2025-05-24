# Snaketron Server Overview

The Snaketron server is a Rust application that has the following responsibilities:

- Runs the authoritative game loop
- Runs the WebSocket and REST API servers
- Clusters with other servers for high availability
- Runs matchmaking and other periodic system tasks

# Architecture
Auto-scalability and resiliency is a key design goal of the Snaketron server, which is achieved using clustering and auto fail-over of the game state. Infrastructure simplicity is also a goal. It should be a simple binary with no external dependencies other than the RDS master database.

The server will be deployed as a Rust binary inside a Docker container in an AWS auto-scaling group via Elastic Beanstalk. Every server will have the following components:

## Game Manager
The GameManager holds actively running GameState instances which are assigned to the local server.

## WebSocket Server
Clients will connect to this server to send commands and receive game update events. It will interact with the game instance in the GameManager on behalf of the user.

## Service Manager