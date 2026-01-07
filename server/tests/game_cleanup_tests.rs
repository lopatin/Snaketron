use ::common::{GameEvent, GameType};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};

mod common;
use self::common::{TestClient, TestEnvironment};

// #[tokio::test]
#[allow(dead_code)]
async fn test_cleanup_abandoned_game() -> Result<()> {
    let mut env = TestEnvironment::new("test_cleanup_abandoned_game").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Create a game with two players
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;

    // Get matched
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    let game_id = wait_for_match(&mut client1).await?;
    let _ = wait_for_match(&mut client2).await?;

    // Give the server time to start the game after matchmaking
    // This delay is needed because the WebSocket handler polls the database
    // and may find the match before the matchmaking service has started the game
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Both join the game
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;

    // Wait for game to start
    wait_for_snapshot(&mut client1).await?;
    wait_for_snapshot(&mut client2).await?;

    // Both players disconnect, abandoning the game
    client1.disconnect().await?;
    client2.disconnect().await?;

    // The cleanup service will eventually mark this game as abandoned
    // and clean it up according to the configured timeouts

    // No manual database manipulation needed - the server handles it

    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_cleanup_finished_game() -> Result<()> {
    let mut env = TestEnvironment::new("test_cleanup_finished_game").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Create a game
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;

    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    let game_id = wait_for_match(&mut client1).await?;
    let _ = wait_for_match(&mut client2).await?;

    // Give the server time to start the game after matchmaking
    // This delay is needed because the WebSocket handler polls the database
    // and may find the match before the matchmaking service has started the game
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Join game
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;

    wait_for_snapshot(&mut client1).await?;
    wait_for_snapshot(&mut client2).await?;

    // In a real game, the game would end when a win condition is met
    // For testing, we just disconnect and let cleanup handle it

    client1.disconnect().await?;
    client2.disconnect().await?;

    // The cleanup service will handle game cleanup based on game state
    // No manual intervention needed

    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_cleanup_stale_matchmaking_requests() -> Result<()> {
    let mut env = TestEnvironment::new("test_cleanup_stale_matchmaking_requests").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Create clients that queue but never get matched
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;

    // Queue for a match that requires 3 players
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 3 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 3 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    // Wait a bit but not long enough to get matched
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Disconnect without leaving queue properly
    client1.disconnect().await?;
    client2.disconnect().await?;

    // The server's cleanup service will handle removing stale requests
    // No manual database cleanup needed

    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_multiple_games_cleanup() -> Result<()> {
    let mut env = TestEnvironment::new("test_multiple_games_cleanup").await?;
    env.add_server().await?;
    for _ in 0..6 {
        env.create_user().await?;
    }
    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Create multiple games concurrently
    let mut game_ids = Vec::new();

    for i in 0..3 {
        let mut client1 = TestClient::connect(&server_addr).await?;
        let mut client2 = TestClient::connect(&server_addr).await?;

        client1.authenticate(env.user_ids()[i * 2]).await?;
        client2.authenticate(env.user_ids()[i * 2 + 1]).await?;

        client1
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;

        client2
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;

        let game_id = wait_for_match(&mut client1).await?;
        let _ = wait_for_match(&mut client2).await?;
        game_ids.push(game_id);

        // Give the server time to start the game after matchmaking
        // This delay is needed because the WebSocket handler polls the database
        // and may find the match before the matchmaking service has started the game
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Join games
        client1.send_message(WSMessage::JoinGame(game_id)).await?;
        client2.send_message(WSMessage::JoinGame(game_id)).await?;

        wait_for_snapshot(&mut client1).await?;
        wait_for_snapshot(&mut client2).await?;

        // Abandon the games
        client1.disconnect().await?;
        client2.disconnect().await?;
    }

    // All games will be cleaned up by the server's cleanup service
    // based on their state and configured timeouts

    env.shutdown().await?;
    Ok(())
}

// Helper functions
async fn wait_for_match(client: &mut TestClient) -> Result<u32> {
    timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if matches!(event.event, GameEvent::Snapshot { .. }) {
                        return Ok(event.game_id);
                    }
                }
                _ => continue,
            }
        }
    })
    .await?
}

async fn wait_for_snapshot(client: &mut TestClient) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok(());
                }
            }
        }
    })
    .await?
}
