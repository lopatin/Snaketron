use anyhow::Result;
use server::game_broker::{GameMessageBroker, DistributedBroker};
use server::games_manager::GamesManager;
use server::ws_server::{run_websocket_server, JwtVerifier};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;
use uuid::Uuid;
use std::time::Duration;

// Import test utilities
#[path = "common/mod.rs"]
mod common;
use self::common::{TestServerBuilder, TestClient};

/// Test that a client connected to Server A can play a game running on Server B
#[tokio::test]
async fn test_cross_server_game_relay() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    // Create shared test database
    let db_pool = TestServerBuilder::create_test_db().await?;
    
    // Setup Server A (WebSocket server)
    let server_a_id = Uuid::new_v4();
    let server_a_port = TestServerBuilder::get_random_port();
    let server_a_grpc_port = TestServerBuilder::get_random_port();
    
    sqlx::query(
        "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
    .bind(server_a_id)
    .bind("test-server-a")
    .bind("localhost")
    .bind(server_a_port as i32)
    .bind(server_a_grpc_port as i32)
    .bind("test")
    .execute(&db_pool)
    .await?;
    
    // Setup Server B (Game server)
    let server_b_id = Uuid::new_v4();
    let server_b_port = TestServerBuilder::get_random_port();
    let server_b_grpc_port = TestServerBuilder::get_random_port();
    
    sqlx::query(
        "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
    .bind(server_b_id)
    .bind("test-server-b")
    .bind("localhost")
    .bind(server_b_port as i32)
    .bind(server_b_grpc_port as i32)
    .bind("test")
    .execute(&db_pool)
    .await?;
    
    // Create game record
    let game_id = 12345;
    sqlx::query(
        "INSERT INTO games (id, status) VALUES ($1, $2)")
    .bind(game_id as i32)
    .bind("waiting")
    .execute(&db_pool)
    .await?;
    
    // Start Server B with the game
    let broker_b = Arc::new(DistributedBroker::new(db_pool.clone(), server_b_id.to_string()));
    let games_manager_b = Arc::new(Mutex::new(GamesManager::new_with_broker(broker_b.clone())));
    
    // Start the game on Server B
    {
        let mut gm = games_manager_b.lock().await;
        gm.start_game(game_id).await?;
        println!("Game {} started on Server B", game_id);
    }
    
    // Verify game is registered to Server B
    let game_location: Option<Uuid> = sqlx::query_scalar(
        "SELECT server_id FROM games WHERE id = $1")
    .bind(game_id as i32)
    .fetch_one(&db_pool)
    .await?;
    assert_eq!(game_location, Some(server_b_id));
    
    // Start gRPC server on Server B
    let grpc_cancellation_b = CancellationToken::new();
    let grpc_server_b = tokio::spawn({
        let broker = broker_b.clone();
        let token = grpc_cancellation_b.clone();
        async move {
            server::grpc_server::run_game_relay_server(
                &format!("0.0.0.0:{}", server_b_grpc_port),
                broker,
                token
            ).await
        }
    });
    
    // Give gRPC server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Start Server A with WebSocket
    let broker_a = Arc::new(DistributedBroker::new(db_pool.clone(), server_a_id.to_string()));
    let games_manager_a = Arc::new(Mutex::new(GamesManager::new_with_broker(broker_a.clone())));
    let jwt_verifier = Arc::new(common::mock_jwt::MockJwtVerifier::accept_any());
    let ws_cancellation_a = CancellationToken::new();
    
    let ws_server_a = tokio::spawn({
        let gm = games_manager_a.clone();
        let verifier = jwt_verifier.clone();
        let token = ws_cancellation_a.clone();
        async move {
            run_websocket_server(
                &format!("0.0.0.0:{}", server_a_port),
                gm,
                token,
                verifier as Arc<dyn JwtVerifier>
            ).await
        }
    });
    
    // Give WebSocket server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect client to Server A
    let mut client = TestClient::connect(&format!("ws://localhost:{}", server_a_port)).await?;
    
    // Authenticate
    client.authenticate(1).await?;
    
    // Join game (which is running on Server B)
    println!("Client joining game {} on remote server", game_id);
    client.send_message(server::ws_server::WSMessage::JoinGame(game_id)).await?;
    
    // Wait for game snapshot
    println!("Waiting for game snapshot...");
    let snapshot_received = tokio::time::timeout(
        Duration::from_secs(5),
        async {
            loop {
                if let Some(msg) = client.receive_game_event().await? {
                    println!("Received event: {:?}", msg.event);
                    if matches!(msg.event, ::common::GameEvent::Snapshot { .. }) {
                        return Ok::<bool, anyhow::Error>(true);
                    }
                } else {
                    println!("No event received");
                }
            }
        }
    ).await??;
    
    assert!(snapshot_received, "Should receive game snapshot");
    
    // Send a command (turn snake)
    client.send_message(server::ws_server::WSMessage::GameCommand(
        ::common::GameCommand::Turn { snake_id: 1, direction: ::common::Direction::Up }
    )).await?;
    
    // Wait for turn event
    let turn_received = tokio::time::timeout(
        Duration::from_secs(5),
        async {
            loop {
                if let Some(msg) = client.receive_game_event().await? {
                    if let ::common::GameEvent::SnakeTurned { snake_id, direction } = msg.event {
                        if snake_id == 1 && direction == ::common::Direction::Up {
                            return Ok::<bool, anyhow::Error>(true);
                        }
                    }
                }
            }
        }
    ).await??;
    
    assert!(turn_received, "Should receive snake turned event");
    
    // Cleanup
    client.disconnect().await?;
    ws_cancellation_a.cancel();
    grpc_cancellation_b.cancel();
    
    // Wait for servers to shut down
    tokio::time::timeout(Duration::from_secs(5), ws_server_a).await??;
    tokio::time::timeout(Duration::from_secs(5), grpc_server_b).await??;
    
    Ok(())
}

/// Test multiple clients on different servers playing the same game
#[tokio::test]
async fn test_multi_client_cross_server() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    // Create shared test database
    let db_pool = TestServerBuilder::create_test_db().await?;
    
    // Setup three servers
    let servers = vec![
        (Uuid::new_v4(), TestServerBuilder::get_random_port(), TestServerBuilder::get_random_port()),
        (Uuid::new_v4(), TestServerBuilder::get_random_port(), TestServerBuilder::get_random_port()),
        (Uuid::new_v4(), TestServerBuilder::get_random_port(), TestServerBuilder::get_random_port()),
    ];
    
    // Register all servers
    for (id, ws_port, grpc_port) in &servers {
        sqlx::query(
            "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
        .bind(id)
        .bind("test-server")
        .bind("localhost")
        .bind(*ws_port as i32)
        .bind(*grpc_port as i32)
        .bind("test")
        .execute(&db_pool)
        .await?;
    }
    
    // Create game
    let game_id = 54321;
    sqlx::query(
        "INSERT INTO games (id, status) VALUES ($1, $2)")
    .bind(game_id as i32)
    .bind("waiting")
    .execute(&db_pool)
    .await?;
    
    // Start all servers
    let mut server_handles = vec![];
    let mut cancellation_tokens = vec![];
    let jwt_verifier = Arc::new(common::mock_jwt::MockJwtVerifier::accept_any());
    
    for (idx, (id, ws_port, grpc_port)) in servers.iter().enumerate() {
        let broker = Arc::new(DistributedBroker::new(db_pool.clone(), id.to_string()));
        let games_manager = Arc::new(Mutex::new(GamesManager::new_with_broker(broker.clone())));
        
        // Start game on first server only
        if idx == 0 {
            let mut gm = games_manager.lock().await;
            gm.start_game(game_id).await?;
        }
        
        // Start gRPC server
        let grpc_token = CancellationToken::new();
        let grpc_handle = tokio::spawn({
            let broker = broker.clone();
            let token = grpc_token.clone();
            let port = *grpc_port;
            async move {
                server::grpc_server::run_game_relay_server(
                    &format!("0.0.0.0:{}", port),
                    broker,
                    token
                ).await
            }
        });
        
        // Start WebSocket server
        let ws_token = CancellationToken::new();
        let ws_handle = tokio::spawn({
            let gm = games_manager.clone();
            let verifier = jwt_verifier.clone();
            let token = ws_token.clone();
            let port = *ws_port;
            async move {
                run_websocket_server(
                    &format!("0.0.0.0:{}", port),
                    gm,
                    token,
                    verifier as Arc<dyn JwtVerifier>
                ).await
            }
        });
        
        cancellation_tokens.push((ws_token, grpc_token));
        server_handles.push((ws_handle, grpc_handle));
    }
    
    // Give servers time to start
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Connect clients to different servers
    let mut clients = vec![];
    for (idx, (_, ws_port, _)) in servers.iter().enumerate() {
        let mut client = TestClient::connect(&format!("ws://localhost:{}", ws_port)).await?;
        client.authenticate(idx as i32 + 1).await?;
        client.send_message(server::ws_server::WSMessage::JoinGame(game_id)).await?;
        
        // Wait for snapshot
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(msg) = client.receive_game_event().await? {
                    if matches!(msg.event, ::common::GameEvent::Snapshot { .. }) {
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }).await??;
        
        clients.push(client);
    }
    
    // Client 0 sends a command
    clients[0].send_message(server::ws_server::WSMessage::GameCommand(
        ::common::GameCommand::Turn { snake_id: 1, direction: ::common::Direction::Left }
    )).await?;
    
    // All clients should receive the turn event
    for client in &mut clients {
        let turn_received = tokio::time::timeout(
            Duration::from_secs(5),
            async {
                loop {
                    if let Some(msg) = client.receive_game_event().await? {
                        if let ::common::GameEvent::SnakeTurned { snake_id, direction } = msg.event {
                            if snake_id == 1 && direction == ::common::Direction::Left {
                                return Ok::<bool, anyhow::Error>(true);
                            }
                        }
                    }
                }
            }
        ).await??;
        
        assert!(turn_received, "All clients should receive the turn event");
    }
    
    // Cleanup
    for client in clients {
        client.disconnect().await?;
    }
    
    for (ws_token, grpc_token) in cancellation_tokens {
        ws_token.cancel();
        grpc_token.cancel();
    }
    
    for (ws_handle, grpc_handle) in server_handles {
        tokio::time::timeout(Duration::from_secs(5), ws_handle).await??;
        tokio::time::timeout(Duration::from_secs(5), grpc_handle).await??;
    }
    
    Ok(())
}

/// Test game failover when a server goes down
#[tokio::test]
async fn test_game_server_failover() -> Result<()> {
    // This test would require implementing game state persistence and failover logic
    // For now, we'll create a placeholder that tests the basic scenario
    
    let db_pool = TestServerBuilder::create_test_db().await?;
    
    // Register two servers
    let server_a_id = Uuid::new_v4();
    let server_b_id = Uuid::new_v4();
    
    for (id, port) in [(server_a_id, 8080), (server_b_id, 8081)] {
        sqlx::query(
            "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
        .bind(id)
        .bind("test-server")
        .bind("localhost")
        .bind(port)
        .bind(port + 1000)
        .bind("test")
        .execute(&db_pool)
        .await?;
    }
    
    // Create game on server A
    let game_id = 99999;
    sqlx::query(
        "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)")
    .bind(game_id as i32)
    .bind("active")
    .bind(server_a_id)
    .execute(&db_pool)
    .await?;
    
    // Create broker for server B
    let broker_b = DistributedBroker::new(db_pool.clone(), server_b_id.to_string());
    
    // Server B should see game is on server A
    assert!(!broker_b.is_game_local(game_id).await?);
    assert_eq!(broker_b.get_game_location(game_id).await?, Some(server_a_id.to_string()));
    
    // Simulate server A going down by updating its heartbeat to be old
    sqlx::query(
        "UPDATE servers SET last_heartbeat = NOW() - INTERVAL '5 minutes' WHERE id = $1")
    .bind(server_a_id)
    .execute(&db_pool)
    .await?;
    
    // In a real implementation, server B would:
    // 1. Detect server A is down (via heartbeat monitoring)
    // 2. Take over games from server A
    // 3. Restore game state from database
    // 4. Update game location in database
    // 5. Continue game execution
    
    // For now, we just verify the game is still marked as on server A
    assert_eq!(broker_b.get_game_location(game_id).await?, Some(server_a_id.to_string()));
    
    Ok(())
}