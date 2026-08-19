//! Lobby leadership: who may change shared lobby state, and who inherits that
//! authority when the leader leaves.
//!
//! Preferences and matchmaking are lobby-wide — one member's action decides
//! what every member queues for — so both are gated on leadership. The gate is
//! only safe because leadership always lands somewhere: these tests pin the
//! denial and the succession together, since a gate without succession would
//! strand a lobby whose host walked away.

use ::common::{GameType, QueueMode};
use anyhow::{Context, Result};
use server::{
    lobby_manager::LobbyPreferences, matchmaking_pool::MatchmakingPool, ws_server::WSMessage,
};
use tokio::time::{Duration, timeout};

mod common;
use self::common::{TestClient, TestEnvironment};

// See lobby_matchmaking_tests.rs: TestEnvironment::new sets process-wide env
// vars and flushes the shared Redis database.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_redis_url() -> String {
    std::env::var("SNAKETRON_REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

async fn flush_test_redis() -> Result<()> {
    let client = redis::Client::open(test_redis_url())?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut connection)
        .await?;
    Ok(())
}

/// Connect `user_ids[0]` as host, then join everyone else into their lobby.
async fn connect_lobby(
    env: &TestEnvironment,
    user_ids: &[i32],
) -> Result<(String, Vec<TestClient>)> {
    let server_addr = env.ws_addr(0).expect("server should exist");

    let mut clients = Vec::new();
    for &user_id in user_ids {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(user_id).await?;
        clients.push(client);
    }

    let lobby_code = clients[0].create_lobby().await?;

    for client in clients.iter_mut().skip(1) {
        client
            .send_message(WSMessage::JoinLobby {
                lobby_code: lobby_code.clone(),
                preferences: None,
            })
            .await?;
        timeout(Duration::from_secs(5), async {
            loop {
                if let WSMessage::JoinedLobby { .. } = client.receive_message().await? {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        })
        .await
        .context("member did not receive JoinedLobby")??;
    }

    Ok((lobby_code, clients))
}

/// Await the next `AccessDenied`, ignoring the lobby traffic that flows
/// continuously on these sockets.
async fn expect_access_denied(client: &mut TestClient) -> Result<String> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::AccessDenied { reason } = client.receive_message().await? {
                return Ok::<String, anyhow::Error>(reason);
            }
        }
    })
    .await
    .context("expected an AccessDenied response")?
}

#[tokio::test]
async fn non_leader_cannot_change_lobby_preferences() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    flush_test_redis().await?;

    let mut env = TestEnvironment::new("non_leader_cannot_change_preferences").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let user_ids = env.user_ids().to_vec();
    let (lobby_code, mut clients) = connect_lobby(&env, &user_ids).await?;

    let lobby_manager = env
        .server(0)
        .expect("server should exist")
        .lobby_manager()
        .clone();
    let before = lobby_manager.get_lobby_preferences(&lobby_code).await?;

    // The joiner asks for a mode nobody else chose.
    clients[1]
        .send_message(WSMessage::UpdateLobbyPreferences {
            selected_modes: vec!["ffa".to_string()],
            competitive: true,
        })
        .await?;

    let reason = expect_access_denied(&mut clients[1]).await?;
    assert!(
        reason.contains("lobby leader"),
        "denial should say why: {reason}"
    );

    let after = lobby_manager.get_lobby_preferences(&lobby_code).await?;
    assert_eq!(
        before, after,
        "a non-leader's request must not alter lobby preferences"
    );

    // The same request from the leader is honored, proving the gate is about
    // leadership and not about the request itself.
    clients[0]
        .send_message(WSMessage::UpdateLobbyPreferences {
            selected_modes: vec!["ffa".to_string()],
            competitive: true,
        })
        .await?;

    let expected = LobbyPreferences {
        selected_modes: vec!["ffa".to_string()],
        competitive: true,
    };
    timeout(Duration::from_secs(5), async {
        loop {
            if lobby_manager.get_lobby_preferences(&lobby_code).await? == expected {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("leader's preference change was not applied")??;

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn non_leader_cannot_start_matchmaking() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    flush_test_redis().await?;

    let mut env = TestEnvironment::new("non_leader_cannot_start_matchmaking").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let user_ids = env.user_ids().to_vec();
    let (lobby_code, mut clients) = connect_lobby(&env, &user_ids).await?;

    clients[1]
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::Solo,
            queue_mode: QueueMode::Quickmatch,
        })
        .await?;

    let reason = expect_access_denied(&mut clients[1]).await?;
    assert!(
        reason.contains("lobby leader"),
        "denial should say why: {reason}"
    );

    let lobby_manager = env
        .server(0)
        .expect("server should exist")
        .lobby_manager()
        .clone();
    let lobby = lobby_manager.get_lobby(&lobby_code).await?;
    assert_eq!(
        lobby.state, "waiting",
        "a denied queue request must leave the lobby unqueued"
    );

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn leadership_passes_to_a_remaining_member_when_the_host_leaves() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    flush_test_redis().await?;

    let mut env = TestEnvironment::new("leadership_passes_on_host_leave").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let user_ids = env.user_ids().to_vec();
    let (lobby_code, mut clients) = connect_lobby(&env, &user_ids).await?;

    let lobby_manager = env
        .server(0)
        .expect("server should exist")
        .lobby_manager()
        .clone();
    assert_eq!(
        lobby_manager.get_lobby(&lobby_code).await?.host_user_id,
        user_ids[0],
        "the creator starts as host"
    );

    let host = clients.remove(0);
    host.disconnect().await?;

    // Succession resolves on read, so the surviving member's own view is what
    // has to converge.
    let successor = timeout(Duration::from_secs(10), async {
        loop {
            let lobby = lobby_manager.get_lobby(&lobby_code).await?;
            if lobby.host_user_id != user_ids[0] {
                return Ok::<i32, anyhow::Error>(lobby.host_user_id);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("leadership never migrated away from the departed host")??;

    assert_eq!(
        successor, user_ids[1],
        "the only remaining member should inherit the lobby"
    );

    // And the inheritance is real authority, not just a displayed field.
    clients[0]
        .send_message(WSMessage::UpdateLobbyPreferences {
            selected_modes: vec!["2v2".to_string()],
            competitive: false,
        })
        .await?;

    timeout(Duration::from_secs(5), async {
        loop {
            let preferences = lobby_manager.get_lobby_preferences(&lobby_code).await?;
            if preferences.selected_modes == vec!["2v2".to_string()] {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("the promoted member could not change preferences")??;

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn lobby_presence_resolves_a_user_to_their_lobby_and_clears_on_leave() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    flush_test_redis().await?;

    let mut env = TestEnvironment::new("lobby_presence_round_trip").await?;
    env.add_server().await?;
    env.create_user().await?;

    let user_ids = env.user_ids().to_vec();
    let (lobby_code, mut clients) = connect_lobby(&env, &user_ids).await?;

    let lobby_manager = env
        .server(0)
        .expect("server should exist")
        .lobby_manager()
        .clone();
    let user_id = u32::try_from(user_ids[0])?;

    // Presence is written as part of admitting the member, so it is readable
    // as soon as the join has been acknowledged.
    let resolved = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(code) = lobby_manager.get_user_lobby_code(user_id).await? {
                return Ok::<String, anyhow::Error>(code);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("presence was never published for a lobby member")??;

    assert_eq!(
        resolved, lobby_code,
        "presence must point at the lobby the user actually joined"
    );

    clients[0].send_message(WSMessage::LeaveLobby).await?;
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LeftLobby = clients[0].receive_message().await? {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .context("client did not receive LeftLobby")??;

    // Leaving is explicit intent: the record goes now rather than at lease
    // expiry, so an invite link cannot point at a lobby the user just left.
    let cleared = timeout(Duration::from_secs(5), async {
        loop {
            if lobby_manager.get_user_lobby_code(user_id).await?.is_none() {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        cleared.is_ok(),
        "presence should be retracted when a member leaves"
    );

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

/// A solo player is their own leader: gating must not make the ordinary
/// one-person case unplayable.
#[tokio::test]
async fn a_lone_member_leads_their_own_lobby() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    flush_test_redis().await?;

    let mut env = TestEnvironment::new("lone_member_leads_own_lobby").await?;
    env.add_server().await?;
    env.create_user().await?;

    let user_ids = env.user_ids().to_vec();
    let (lobby_code, clients) = connect_lobby(&env, &user_ids).await?;

    let lobby_manager = env
        .server(0)
        .expect("server should exist")
        .lobby_manager()
        .clone();
    assert!(
        lobby_manager
            .is_lobby_host(&lobby_code, user_ids[0])
            .await?,
        "the sole member of a lobby must be its leader"
    );
    assert_eq!(
        lobby_manager
            .get_lobby_metadata(&lobby_code)
            .await?
            .expect("lobby metadata should exist")
            .matchmaking_pool,
        MatchmakingPool::Public
    );

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}
