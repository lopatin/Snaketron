mod common;

use crate::common::{TestClient, TestEnvironment};
use ::common::{GameEvent, GameState, GameType, QueueMode};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};

// TestEnvironment sets process-global env vars, so tests in this binary must
// not run concurrently.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn wait_for_join_game(client: &mut TestClient, label: &str) -> Result<u32> {
    // Matchmaking runs on a 2s interval and the lobby has to settle first, so
    // this is generous on purpose: a tight bound here turns ordinary scheduling
    // jitter into a flaky failure.
    timeout(Duration::from_secs(25), async {
        loop {
            match client.receive_message().await? {
                WSMessage::JoinGame(id) => break Ok::<u32, anyhow::Error>(id),
                other => println!("{label}: ignoring while waiting for JoinGame: {other:?}"),
            }
        }
    })
    .await?
}

/// Drain messages until a snapshot satisfying `accept` arrives.
async fn wait_for_snapshot_where(
    client: &mut TestClient,
    label: &str,
    seconds: u64,
    accept: impl Fn(&GameState) -> bool,
) -> Result<GameState> {
    timeout(Duration::from_secs(seconds), async {
        loop {
            let message = client.receive_message().await?;
            if let WSMessage::GameEvent(event) = &message
                && let GameEvent::Snapshot { game_state } = &event.event
                && accept(game_state)
            {
                break Ok::<GameState, anyhow::Error>(game_state.clone());
            }
            println!("{label}: ignoring while waiting for a matching snapshot");
        }
    })
    .await?
}

/// Wait for a specific readiness confirmation to reach this client.
async fn wait_for_player_ready(
    client: &mut TestClient,
    label: &str,
    user_id: u32,
    seconds: u64,
) -> Result<()> {
    timeout(Duration::from_secs(seconds), async {
        loop {
            let message = client.receive_message().await?;
            if let WSMessage::GameEvent(event) = &message
                && let GameEvent::PlayerReady { user_id: ready } = &event.event
                && *ready == user_id
            {
                break Ok::<(), anyhow::Error>(());
            }
            println!("{label}: ignoring while waiting for PlayerReady({user_id})");
        }
    })
    .await?
}

/// Watch the event stream for `seconds` and report the highest tick observed.
/// A gated match must produce none.
async fn observe_highest_tick(client: &mut TestClient, seconds: u64) -> Result<u32> {
    let mut highest = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline {
        match timeout(Duration::from_millis(400), client.receive_message()).await {
            Ok(Ok(WSMessage::GameEvent(event))) => {
                highest = highest.max(event.tick);
                if let GameEvent::Snapshot { game_state } = &event.event {
                    highest = highest.max(game_state.tick);
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {}
        }
    }
    Ok(highest)
}

struct MatchedDuel {
    env: TestEnvironment,
    client1: TestClient,
    client2: TestClient,
    game_id: u32,
    user1: u32,
}

async fn matched_duel(test_name: &str) -> Result<MatchedDuel> {
    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://localhost:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new(test_name).await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

    for client in [&mut client1, &mut client2] {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::TeamMatch { per_team: 1 },
                queue_mode: QueueMode::Quickmatch,
            })
            .await?;
    }

    let game_id = wait_for_join_game(&mut client1, "client1").await?;
    let game_id2 = wait_for_join_game(&mut client2, "client2").await?;
    assert_eq!(game_id, game_id2, "both clients should join the same game");

    // The gate is what these tests exercise, so readiness is sent explicitly
    // rather than folded into the join helper.
    client1.join_game_without_readiness(game_id).await?;
    client2.join_game_without_readiness(game_id).await?;

    let user1 = env.user_ids()[0] as u32;
    Ok(MatchedDuel {
        env,
        client1,
        client2,
        game_id,
        user1,
    })
}

/// The core promise of the feature: a fresh match is held, and it is released
/// only once every player has confirmed. The wall-clock countdown alone must
/// not start it.
#[tokio::test]
async fn a_match_does_not_start_until_every_player_is_ready() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let MatchedDuel {
        env,
        mut client1,
        mut client2,
        game_id,
        user1,
        ..
    } = matched_duel("readiness_gate_holds_the_match").await?;

    let first = wait_for_snapshot_where(&mut client1, "client1", 20, |_| true).await?;
    assert!(
        first.readiness.is_some(),
        "a freshly matched game must arrive with the readiness gate armed"
    );
    assert!(
        first.simulation_start_ms().is_none(),
        "a gated match has no simulation epoch yet"
    );
    assert_eq!(first.players_pending_ready().len(), 2);
    let _ = wait_for_snapshot_where(&mut client2, "client2", 20, |_| true).await?;

    // Everything from here until the second confirmation has to fit inside the
    // readiness deadline, or the gate resolves on its own and the assertions
    // below stop meaning anything. The ungated countdown (start_ms + 3s) has
    // already elapsed by the time the first snapshot arrives, so four seconds
    // of silence is enough to prove the gate — not the clock — is holding it.
    let highest_tick = observe_highest_tick(&mut client1, 4).await?;
    assert_eq!(
        highest_tick, 0,
        "a match held by the readiness gate must not simulate a single tick"
    );

    // One player readying is not enough. Readiness is broadcast as an event,
    // not as a fresh snapshot, so this waits for the event itself.
    client1
        .send_message(WSMessage::PlayerReady { game_id })
        .await?;
    wait_for_player_ready(&mut client1, "client1", user1, 10).await?;

    let still_held = observe_highest_tick(&mut client1, 2).await?;
    assert_eq!(
        still_held, 0,
        "one player confirming must not release the gate"
    );

    // Both ready: the gate resolves and the match begins.
    client2
        .send_message(WSMessage::PlayerReady { game_id })
        .await?;
    let released = wait_for_snapshot_where(&mut client2, "client2", 15, |state| {
        state.readiness.is_none()
    })
    .await?;
    assert!(
        released.simulation_start_ms().is_some(),
        "resolving the gate must stamp a simulation epoch"
    );
    assert_eq!(
        released.start_ms, first.start_ms,
        "start_ms is the durable runtime game identity and must never move"
    );

    let ticked = observe_highest_tick(&mut client2, 8).await?;
    assert!(
        ticked > 0,
        "the match must simulate once everyone has confirmed"
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

/// A player who never answers must not be able to hold the match hostage.
#[tokio::test]
async fn the_readiness_deadline_starts_the_match_without_a_silent_player() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let MatchedDuel {
        env,
        mut client1,
        mut client2,
        game_id,
        ..
    } = matched_duel("readiness_deadline_releases_the_match").await?;

    let gated = wait_for_snapshot_where(&mut client1, "client1", 20, |_| true).await?;
    assert!(gated.readiness.is_some());
    let _ = wait_for_snapshot_where(&mut client2, "client2", 20, |_| true).await?;

    // Only client1 ever answers. client2 stays silent for the whole window.
    client1
        .send_message(WSMessage::PlayerReady { game_id })
        .await?;

    // MATCH_READY_WINDOW_MS is 15s; allow the countdown plus slack on top.
    let released = wait_for_snapshot_where(&mut client1, "client1", 30, |state| {
        state.readiness.is_none()
    })
    .await?;
    assert!(
        released.simulation_start_ms().is_some(),
        "the deadline must stamp an epoch even with a player still missing"
    );

    let ticked = observe_highest_tick(&mut client1, 10).await?;
    assert!(
        ticked > 0,
        "the match must start once the readiness deadline lapses"
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

/// Readiness travels over the at-least-once command stream, so a resend is
/// routine. It must be a no-op rather than corrupting the gate.
#[tokio::test]
async fn repeated_readiness_confirmations_are_idempotent() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let MatchedDuel {
        env,
        mut client1,
        mut client2,
        game_id,
        user1,
        ..
    } = matched_duel("readiness_confirmations_are_idempotent").await?;

    let _ = wait_for_snapshot_where(&mut client1, "client1", 20, |_| true).await?;
    let _ = wait_for_snapshot_where(&mut client2, "client2", 20, |_| true).await?;

    for _ in 0..5 {
        client1
            .send_message(WSMessage::PlayerReady { game_id })
            .await?;
    }
    wait_for_player_ready(&mut client1, "client1", user1, 10).await?;

    let still_held = observe_highest_tick(&mut client1, 3).await?;
    assert_eq!(
        still_held, 0,
        "five confirmations from one player must not stand in for the other"
    );

    client2
        .send_message(WSMessage::PlayerReady { game_id })
        .await?;
    let released = wait_for_snapshot_where(&mut client2, "client2", 15, |state| {
        state.readiness.is_none()
    })
    .await?;
    assert!(released.simulation_start_ms().is_some());

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}
