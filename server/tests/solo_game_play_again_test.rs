mod common;

use crate::common::{TestClient, TestEnvironment};
use ::common::{GameEvent, GameStatus};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};

// #[tokio::test]
#[allow(dead_code)]
async fn test_solo_game_play_again() -> Result<()> {
    timeout(Duration::from_secs(45), async {
        // Initialize tracing
        let _ = tracing_subscriber::fmt::try_init();

        // Create environment
        let mut env = TestEnvironment::new("test_solo_game_play_again").await?;
        let (_, _server_id) = env.add_server().await?;
        env.create_user().await?;

        let server_addr = env.ws_addr(0).expect("Server should exist");

        // Connect client
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[0]).await?;

        println!("Client authenticated");

        // ==== FIRST GAME ====
        println!("=== Starting First Solo Game ===");
        let first_game_result = play_single_solo_game(&mut client, env.user_ids()[0]).await;

        match first_game_result {
            Ok(result) => {
                println!(
                    "✓ First game completed successfully with result: {:?}",
                    result
                );
            }
            Err(e) => {
                println!("✗ First game failed: {}", e);
                panic!("First solo game should work but failed: {}", e);
            }
        }

        // Drain any remaining messages before starting the second game
        drain_remaining_messages(&mut client).await?;

        // ==== SECOND GAME (PLAY AGAIN) - This is what we're testing ====
        println!("=== Starting Second Solo Game (Play Again) ===");
        let second_game_result = play_single_solo_game(&mut client, env.user_ids()[0]).await;

        match second_game_result {
            Ok(result) => {
                println!(
                    "✓ Second game completed successfully with result: {:?}",
                    result
                );
                println!("✓ PLAY AGAIN FUNCTIONALITY WORKS!");
            }
            Err(e) => {
                println!("✗ Second game failed: {}", e);
                println!("✗ PLAY AGAIN FUNCTIONALITY BROKEN!");

                // This is the bug we're testing for - the second game creation fails
                // Even though the test fails, we've successfully identified the issue
                panic!(
                    "Play-again functionality is broken: Second solo game creation failed with: {}",
                    e
                );
            }
        }

        client.disconnect().await?;
        env.shutdown().await?;
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out"))?
}

#[derive(Debug)]
struct GameCompletionResult {
    winning_snake_id: Option<u32>,
    score: u32,
    duration: u32,
}

async fn play_single_solo_game(
    _client: &mut TestClient,
    _user_id: i32,
) -> Result<GameCompletionResult> {
    // TODO: Update this test to use the new lobby-based solo game creation
    // The old WSMessage::CreateSoloGame has been removed in favor of lobby-based matchmaking
    unimplemented!("This test needs to be updated for the new lobby system")
}

async fn drain_remaining_messages(client: &mut TestClient) -> Result<()> {
    println!("Draining any remaining messages...");
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < Duration::from_millis(1000) {
        match timeout(Duration::from_millis(100), client.receive_message()).await {
            Ok(Ok(msg)) => {
                println!("Drained message: {:?}", msg);
            }
            Ok(Err(_)) => {
                println!("Error while draining, continuing...");
            }
            Err(_) => {
                // Timeout - no more messages to drain
                break;
            }
        }
    }

    println!("Message draining complete");
    Ok(())
}
