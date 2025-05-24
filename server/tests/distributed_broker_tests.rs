use anyhow::Result;
use server::game_broker::{GameMessageBroker, DistributedBroker};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

// Import test utilities
#[path = "common/mod.rs"]
mod common;
use self::common::TestServerBuilder;

#[tokio::test]
async fn test_distributed_broker_local_game() -> Result<()> {
    // Create test database pool
    let db_pool = TestServerBuilder::create_test_db().await?;
    let server_id = Uuid::new_v4().to_string();
    
    // Register server in database
    sqlx::query(
        "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())"
    )
    .bind(Uuid::parse_str(&server_id)?)
    .bind("test-server")
    .bind("localhost")
    .bind(8080)
    .bind(9090)
    .bind("test")
    .execute(&db_pool)
    .await?;
    
    // Create broker
    let broker = DistributedBroker::new(db_pool.clone(), server_id.clone());
    
    // Create a game with unique ID to avoid conflicts
    let game_id = (rand::random::<u16>() as u32) + 500000;
    sqlx::query("INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)")
        .bind(game_id as i32)
        .bind("active")
        .bind(Uuid::parse_str(&server_id)?)
        .execute(&db_pool)
        .await?;
    
    // Create game channels (this should also update the database)
    broker.create_game_channels(game_id).await?;
    
    // Verify game is local
    assert!(broker.is_game_local(game_id).await?);
    assert_eq!(broker.get_game_location(game_id).await?, Some(server_id.clone()));
    
    // Test command pub/sub
    let mut cmd_rx = broker.subscribe_commands(game_id).await?;
    let test_cmd = ::common::GameCommandMessage {
        tick: 100,
        received_order: 1,
        user_id: 1,
        command: ::common::GameCommand::Tick,
    };
    broker.publish_command(game_id, test_cmd.clone()).await?;
    
    let received = timeout(Duration::from_secs(1), cmd_rx.recv()).await??;
    assert_eq!(received, test_cmd);
    
    // Test event pub/sub
    let mut evt_rx = broker.subscribe_events(game_id).await?;
    let test_evt = ::common::GameEventMessage {
        game_id,
        tick: 101,
        user_id: Some(1),
        event: ::common::GameEvent::FoodSpawned { position: ::common::Position { x: 10, y: 20 } },
    };
    broker.publish_event(game_id, test_evt.clone()).await?;
    
    let received = timeout(Duration::from_secs(1), evt_rx.recv()).await??;
    assert_eq!(received, test_evt);
    
    Ok(())
}

#[tokio::test]
async fn test_distributed_broker_remote_game_lookup() -> Result<()> {
    let db_pool = TestServerBuilder::create_test_db().await?;
    let local_server_id = Uuid::new_v4().to_string();
    let remote_server_id = Uuid::new_v4().to_string();
    
    // Register both servers
    for (id, port) in [(local_server_id.clone(), 8080), (remote_server_id.clone(), 8081)] {
        sqlx::query(
            "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
        .bind(Uuid::parse_str(&id)?)
        .bind("test-server")
        .bind("localhost")
        .bind(port)
        .bind(port + 1000)
        .bind("test")
        .execute(&db_pool)
        .await?;
    }
    
    // Create game on remote server with unique ID
    let game_id = (rand::random::<u16>() as u32) + 600000;
    sqlx::query(
        "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)")
    .bind(game_id as i32)
    .bind("active")
    .bind(Uuid::parse_str(&remote_server_id)?)
    .execute(&db_pool)
    .await?;
    
    // Create broker for local server
    let broker = DistributedBroker::new(db_pool.clone(), local_server_id);
    
    // Game should not be local
    assert!(!broker.is_game_local(game_id).await?);
    assert_eq!(broker.get_game_location(game_id).await?, Some(remote_server_id));
    
    // Publishing to remote game should fail (not implemented yet)
    let test_cmd = ::common::GameCommandMessage {
        tick: 200,
        received_order: 1,
        user_id: 2,
        command: ::common::GameCommand::Tick,
    };
    assert!(broker.publish_command(game_id, test_cmd).await.is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_distributed_broker_caching() -> Result<()> {
    let db_pool = TestServerBuilder::create_test_db().await?;
    let server_id = Uuid::new_v4().to_string();
    
    // Register server
    sqlx::query(
        "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
    .bind(Uuid::parse_str(&server_id)?)
    .bind("test-server")
    .bind("localhost")
    .bind(8080)
    .bind(9090)
    .bind("test")
    .execute(&db_pool)
    .await?;
    
    // Create game with unique ID
    let game_id = (rand::random::<u16>() as u32) + 700000;
    sqlx::query(
        "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)")
    .bind(game_id as i32)
    .bind("active")
    .bind(Uuid::parse_str(&server_id)?)
    .execute(&db_pool)
    .await?;
    
    let broker = DistributedBroker::new(db_pool.clone(), server_id.clone());
    
    // First lookup should hit database
    let location1 = broker.get_game_location(game_id).await?;
    assert_eq!(location1, Some(server_id.clone()));
    
    // Second lookup should use cache (we can't directly test this, but it should be fast)
    let location2 = broker.get_game_location(game_id).await?;
    assert_eq!(location2, Some(server_id));
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_games_on_different_servers() -> Result<()> {
    let db_pool = TestServerBuilder::create_test_db().await?;
    let server1_id = Uuid::new_v4().to_string();
    let server2_id = Uuid::new_v4().to_string();
    
    // Register servers
    for (id, port) in [(server1_id.clone(), 8080), (server2_id.clone(), 8081)] {
        sqlx::query(
            "INSERT INTO servers (id, hostname, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())")
        .bind(Uuid::parse_str(&id)?)
        .bind("test-server")
        .bind("localhost")
        .bind(port)
        .bind(port + 1000)
        .bind("test")
        .execute(&db_pool)
        .await?;
    }
    
    // Create games on different servers with unique IDs
    let base_id = (rand::random::<u16>() as u32) * 1000 + 800000;
    let games = vec![
        (base_id + 1, server1_id.clone()),
        (base_id + 2, server1_id.clone()),
        (base_id + 3, server2_id.clone()),
        (base_id + 4, server2_id.clone()),
    ];
    
    for (game_id, server_id) in &games {
        sqlx::query(
            "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)")
        .bind(*game_id as i32)
        .bind("active")
        .bind(Uuid::parse_str(server_id)?)
        .execute(&db_pool)
        .await?;
    }
    
    // Create brokers for both servers
    let broker1 = DistributedBroker::new(db_pool.clone(), server1_id.clone());
    let broker2 = DistributedBroker::new(db_pool.clone(), server2_id.clone());
    
    // Verify game locations from broker1's perspective
    assert!(broker1.is_game_local(games[0].0).await?);
    assert!(broker1.is_game_local(games[1].0).await?);
    assert!(!broker1.is_game_local(games[2].0).await?);
    assert!(!broker1.is_game_local(games[3].0).await?);
    
    // Verify game locations from broker2's perspective  
    assert!(!broker2.is_game_local(games[0].0).await?);
    assert!(!broker2.is_game_local(games[1].0).await?);
    assert!(broker2.is_game_local(games[2].0).await?);
    assert!(broker2.is_game_local(games[3].0).await?);
    
    Ok(())
}