use anyhow::Result;
use sqlx::PgPool;
use server::game_broker::{GameMessageBroker, DistributedBroker, LocalBroker};
use common::{GameCommandMessage, GameEventMessage, GameCommand, GameEvent, Position};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

// Import test utilities
mod common;
use common::TestServerBuilder;

#[tokio::test]
async fn test_distributed_broker_local_game() -> Result<()> {
    // Create test database pool
    let db_pool = TestServerBuilder::create_test_db().await?;
    let server_id = Uuid::new_v4().to_string();
    
    // Register server in database
    sqlx::query!(
        "INSERT INTO servers (id, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, NOW(), NOW())",
        Uuid::parse_str(&server_id)?,
        "localhost",
        8080,
        9090,
        "test"
    )
    .execute(&db_pool)
    .await?;
    
    // Create broker
    let broker = DistributedBroker::new(db_pool.clone(), server_id.clone());
    
    // Create a game
    let game_id = 123;
    sqlx::query!(
        "INSERT INTO games (id, status) VALUES ($1, $2)",
        game_id as i32,
        "active"
    )
    .execute(&db_pool)
    .await?;
    
    // Create game channels (this should also update the database)
    broker.create_game_channels(game_id).await?;
    
    // Verify game is local
    assert!(broker.is_game_local(game_id).await?);
    assert_eq!(broker.get_game_location(game_id).await?, Some(server_id.clone()));
    
    // Test command pub/sub
    let mut cmd_rx = broker.subscribe_commands(game_id).await?;
    let test_cmd = GameCommandMessage {
        game_id,
        tick: 100,
        user_id: 1,
        command: GameCommand::Tick,
    };
    broker.publish_command(game_id, test_cmd.clone()).await?;
    
    let received = timeout(Duration::from_secs(1), cmd_rx.recv()).await??;
    assert_eq!(received, test_cmd);
    
    // Test event pub/sub
    let mut evt_rx = broker.subscribe_events(game_id).await?;
    let test_evt = GameEventMessage {
        game_id,
        tick: 101,
        user_id: Some(1),
        event: GameEvent::FoodSpawned { position: Position { x: 10, y: 20 } },
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
        sqlx::query!(
            "INSERT INTO servers (id, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, NOW(), NOW())",
            Uuid::parse_str(&id)?,
            "localhost",
            port,
            port + 1000,
            "test"
        )
        .execute(&db_pool)
        .await?;
    }
    
    // Create game on remote server
    let game_id = 456;
    sqlx::query!(
        "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)",
        game_id as i32,
        "active",
        Uuid::parse_str(&remote_server_id)?
    )
    .execute(&db_pool)
    .await?;
    
    // Create broker for local server
    let broker = DistributedBroker::new(db_pool.clone(), local_server_id);
    
    // Game should not be local
    assert!(!broker.is_game_local(game_id).await?);
    assert_eq!(broker.get_game_location(game_id).await?, Some(remote_server_id));
    
    // Publishing to remote game should fail (not implemented yet)
    let test_cmd = GameCommandMessage {
        game_id,
        tick: 200,
        user_id: 2,
        command: GameCommand::Tick,
    };
    assert!(broker.publish_command(game_id, test_cmd).await.is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_distributed_broker_caching() -> Result<()> {
    let db_pool = TestServerBuilder::create_test_db().await?;
    let server_id = Uuid::new_v4().to_string();
    
    // Register server
    sqlx::query!(
        "INSERT INTO servers (id, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
         VALUES ($1, $2, $3, $4, $5, NOW(), NOW())",
        Uuid::parse_str(&server_id)?,
        "localhost",
        8080,
        9090,
        "test"
    )
    .execute(&db_pool)
    .await?;
    
    // Create game
    let game_id = 789;
    sqlx::query!(
        "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)",
        game_id as i32,
        "active",
        Uuid::parse_str(&server_id)?
    )
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
        sqlx::query!(
            "INSERT INTO servers (id, host, ws_port, grpc_port, region, created_at, last_heartbeat) 
             VALUES ($1, $2, $3, $4, $5, NOW(), NOW())",
            Uuid::parse_str(&id)?,
            "localhost",
            port,
            port + 1000,
            "test"
        )
        .execute(&db_pool)
        .await?;
    }
    
    // Create games on different servers
    let games = vec![
        (101, server1_id.clone()),
        (102, server1_id.clone()),
        (201, server2_id.clone()),
        (202, server2_id.clone()),
    ];
    
    for (game_id, server_id) in &games {
        sqlx::query!(
            "INSERT INTO games (id, status, server_id) VALUES ($1, $2, $3)",
            *game_id as i32,
            "active",
            Uuid::parse_str(server_id)?
        )
        .execute(&db_pool)
        .await?;
    }
    
    // Create brokers for both servers
    let broker1 = DistributedBroker::new(db_pool.clone(), server1_id.clone());
    let broker2 = DistributedBroker::new(db_pool.clone(), server2_id.clone());
    
    // Verify game locations from broker1's perspective
    assert!(broker1.is_game_local(101).await?);
    assert!(broker1.is_game_local(102).await?);
    assert!(!broker1.is_game_local(201).await?);
    assert!(!broker1.is_game_local(202).await?);
    
    // Verify game locations from broker2's perspective  
    assert!(!broker2.is_game_local(101).await?);
    assert!(!broker2.is_game_local(102).await?);
    assert!(broker2.is_game_local(201).await?);
    assert!(broker2.is_game_local(202).await?);
    
    Ok(())
}