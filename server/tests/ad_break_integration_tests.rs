mod common;

use crate::common::{TestClient, TestEnvironment, is_unsolicited_push};
use ::common::{GameType, QueueMode};
use anyhow::{Context, Result};
use server::ads::{
    AdBreakResolution, AdsConfig, BannerAdsConfig, ClientAdsConfig, ClientDistribution,
    LobbyAdBreakView, VideoAdsConfig,
};
use server::db::models::{
    RuntimeAdsConfig, RuntimeAdsDistributionsConfig, RuntimeConfig, RuntimeConfigActor,
    RuntimeDistributionAdsConfig,
};
use server::lifecycle::WS_PROTOCOL_VERSION;
use server::ws_server::WSMessage;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::timeout;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn wait_for_lobby_state(
    client: &mut TestClient,
    lobby_code: &str,
    expected_state: &str,
) -> Result<Option<LobbyAdBreakView>> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LobbyUpdate {
                lobby_code: update_code,
                state,
                ad_break,
                ..
            } = client.receive_message().await?
                && update_code == lobby_code
                && state == expected_state
            {
                return Ok::<Option<LobbyAdBreakView>, anyhow::Error>(ad_break);
            }
        }
    })
    .await
    .with_context(|| format!("Timed out waiting for lobby state {expected_state}"))?
}

fn video_policy(provider: &str) -> ClientAdsConfig {
    ClientAdsConfig {
        enabled: true,
        provider: provider.to_owned(),
        banners: BannerAdsConfig {
            bottom: true,
            sides: true,
        },
        video: VideoAdsConfig { pre_match: true },
    }
}

async fn enable_runtime_ads(
    env: &TestEnvironment,
    distributions: &[ClientDistribution],
    minimum_games_played: u32,
    minimum_interval_minutes: u16,
) -> Result<()> {
    let enabled = |distribution| RuntimeDistributionAdsConfig {
        enabled: distributions.contains(&distribution),
    };
    let config = RuntimeConfig {
        ads: RuntimeAdsConfig {
            enabled: true,
            minimum_games_played,
            minimum_interval_minutes,
            distributions: RuntimeAdsDistributionsConfig {
                web: enabled(ClientDistribution::Web),
                crazygames: enabled(ClientDistribution::CrazyGames),
                itch: enabled(ClientDistribution::Itch),
            },
        },
        ..RuntimeConfig::default()
    };
    env.db()
        .update_runtime_config(
            0,
            &config,
            &RuntimeConfigActor {
                user_id: 1,
                username: "test-operator".to_owned(),
            },
        )
        .await?;
    Ok(())
}

async fn wait_for_queue_without_ad(client: &mut TestClient, lobby_code: &str) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LobbyUpdate {
                lobby_code: update_code,
                state,
                ..
            } = client.receive_message().await?
                && update_code == lobby_code
            {
                if state == "ad_break" {
                    return Err(anyhow::anyhow!("lobby unexpectedly entered an ad break"));
                }
                if state == "queued" {
                    return Ok(());
                }
            }
        }
    })
    .await
    .context("Timed out waiting for direct queue admission")?
}

async fn join_lobby(client: &mut TestClient, lobby_code: &str) -> Result<()> {
    client
        .send_message(WSMessage::JoinLobby {
            lobby_code: lobby_code.to_owned(),
            preferences: None,
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        loop {
            match client.receive_message().await? {
                WSMessage::JoinedLobby {
                    lobby_code: joined_code,
                } if joined_code == lobby_code => return Ok::<(), anyhow::Error>(()),
                WSMessage::AccessDenied { reason } => {
                    return Err(anyhow::anyhow!("Joining lobby was denied: {reason}"));
                }
                _ => {}
            }
        }
    })
    .await
    .context("Timed out waiting to join lobby")?
}

#[tokio::test]
async fn shared_server_routes_policy_by_authenticated_distribution() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("shared_server_routes_ad_policy").await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([
            (ClientDistribution::Web, video_policy("website-h5")),
            (ClientDistribution::CrazyGames, video_policy("crazygames")),
            (ClientDistribution::Itch, ClientAdsConfig::default()),
        ]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;

    let web_user = env.create_user().await?;
    let crazygames_user = env.create_user().await?;
    let itch_user = env.create_user().await?;
    let server_addr = env.ws_addr(0).context("Server should exist")?;

    let mut web = TestClient::connect(&server_addr).await?;
    web.authenticate_for_distribution(web_user, ClientDistribution::Web)
        .await?;
    let mut crazygames = TestClient::connect(&server_addr).await?;
    crazygames
        .authenticate_for_distribution(crazygames_user, ClientDistribution::CrazyGames)
        .await?;
    let mut itch = TestClient::connect(&server_addr).await?;
    itch.authenticate_for_distribution(itch_user, ClientDistribution::Itch)
        .await?;

    assert_eq!(
        web.ad_configuration
            .as_ref()
            .map(|config| config.provider.as_str()),
        Some("website-h5")
    );
    assert_eq!(
        crazygames
            .ad_configuration
            .as_ref()
            .map(|config| config.provider.as_str()),
        Some("crazygames")
    );
    assert_eq!(itch.ad_configuration, Some(ClientAdsConfig::default()));

    web.disconnect().await?;
    crazygames.disconnect().await?;
    itch.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn mixed_distribution_lobby_waits_for_ad_and_no_ad_members() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("mixed_distribution_ad_break").await?;
    enable_runtime_ads(&env, &[ClientDistribution::Web], 0, 10).await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([
            (ClientDistribution::Web, video_policy("website-h5")),
            (ClientDistribution::Itch, video_policy("itch-test-provider")),
        ]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;

    let web_user = env.create_user().await?;
    let itch_user = env.create_user().await?;
    let server_addr = env.ws_addr(0).context("Server should exist")?;
    let mut web = TestClient::connect(&server_addr).await?;
    web.authenticate_for_distribution(web_user, ClientDistribution::Web)
        .await?;
    let mut itch = TestClient::connect(&server_addr).await?;
    itch.authenticate_for_distribution(itch_user, ClientDistribution::Itch)
        .await?;

    let lobby_code = web.create_lobby().await?;
    join_lobby(&mut itch, &lobby_code).await?;
    web.send_message(WSMessage::QueueForMatch {
        game_type: GameType::TeamMatch { per_team: 1 },
        queue_mode: QueueMode::Quickmatch,
    })
    .await?;

    let web_break = wait_for_lobby_state(&mut web, &lobby_code, "ad_break")
        .await?
        .context("web member did not receive the mixed-lobby break")?;
    let itch_break = wait_for_lobby_state(&mut itch, &lobby_code, "ad_break")
        .await?
        .context("no-ad member did not receive the mixed-lobby barrier")?;
    assert_eq!(web_break.id, itch_break.id);
    assert_eq!(web_break.participant_count, 2);
    assert!(
        itch.ad_configuration
            .as_ref()
            .is_some_and(|config| config.enabled)
    );
    assert_eq!(web_break.ad_user_ids, vec![u32::try_from(web_user)?]);
    assert_eq!(itch_break.ad_user_ids, web_break.ad_user_ids);

    web.send_message(WSMessage::AdBreakResolved {
        break_id: web_break.id.clone(),
        resolution: AdBreakResolution::Completed,
    })
    .await?;
    itch.send_message(WSMessage::AdBreakResolved {
        break_id: itch_break.id,
        resolution: AdBreakResolution::Unavailable,
    })
    .await?;
    assert!(
        wait_for_lobby_state(&mut web, &lobby_code, "queued")
            .await?
            .is_none()
    );

    web.disconnect().await?;
    itch.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn eligible_lobby_resolves_ad_break_before_queue_admission() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("eligible_lobby_resolves_ad_break").await?;
    enable_runtime_ads(&env, &[ClientDistribution::Web], 0, 10).await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([(
            ClientDistribution::Web,
            ClientAdsConfig {
                enabled: true,
                provider: "test-provider".to_owned(),
                banners: BannerAdsConfig {
                    bottom: false,
                    sides: false,
                },
                video: VideoAdsConfig { pre_match: true },
            },
        )]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;
    let user_id = env.create_user().await?;

    let server_addr = env.ws_addr(0).context("Server should exist")?;
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;
    let client_policy = client
        .ad_configuration
        .as_ref()
        .context("v9 authentication omitted runtime ad configuration")?;
    assert!(client_policy.enabled);
    assert!(client_policy.video.pre_match);
    assert_eq!(client_policy.provider, "test-provider");

    let lobby_code = client.create_lobby().await?;
    client
        .send_message(WSMessage::QueueForMatch {
            // A one-player duel lobby remains queued long enough to observe the
            // post-barrier state instead of immediately becoming a Solo match.
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Quickmatch,
        })
        .await?;

    let ad_break = wait_for_lobby_state(&mut client, &lobby_code, "ad_break")
        .await?
        .context("ad-break state omitted its durable break view")?;
    assert_eq!(ad_break.participant_count, 1);
    assert_eq!(ad_break.resolved_count, 0);
    assert_eq!(ad_break.ad_user_ids, vec![u32::try_from(user_id)?]);

    client
        .send_message(WSMessage::AdBreakResolved {
            break_id: ad_break.id,
            resolution: AdBreakResolution::Completed,
        })
        .await?;

    let queued_break = wait_for_lobby_state(&mut client, &lobby_code, "queued").await?;
    assert!(
        queued_break.is_none(),
        "atomic queue admission must clear the completed ad-break payload"
    );

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stale_protocol_is_denied_before_any_ad_frames() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("stale_protocol_denied_before_ads").await?;
    enable_runtime_ads(&env, &[ClientDistribution::Web], 0, 10).await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([(
            ClientDistribution::Web,
            ClientAdsConfig {
                enabled: true,
                provider: "test-provider".to_owned(),
                banners: BannerAdsConfig {
                    bottom: true,
                    sides: true,
                },
                video: VideoAdsConfig { pre_match: true },
            },
        )]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;
    let user_id = env.create_user().await?;

    let server_addr = env.ws_addr(0).context("Server should exist")?;
    let mut client = TestClient::connect(&server_addr).await?;
    client
        .send_message(WSMessage::Authenticate {
            token: user_id.to_string(),
            protocol_version: WS_PROTOCOL_VERSION - 1,
            distribution: Some(ClientDistribution::Web),
        })
        .await?;
    let denial_reason = timeout(Duration::from_secs(5), async {
        loop {
            match client.receive_message().await? {
                WSMessage::AccessDenied { reason } => return Ok::<_, anyhow::Error>(reason),
                other if is_unsolicited_push(&other) => {}
                WSMessage::AdConfiguration(configuration) => {
                    return Err(anyhow::anyhow!(
                        "stale peer received ad configuration before protocol denial: {configuration:?}"
                    ));
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "stale peer received {other:?} before protocol denial"
                    ));
                }
            }
        }
    })
    .await
    .context("Timed out waiting for stale-protocol denial")??;
    assert!(denial_reason.contains("Gameplay update required"));
    assert!(denial_reason.contains(&format!("server protocol {WS_PROTOCOL_VERSION}")));

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn durable_interval_skips_a_second_break_for_the_same_user() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("durable_ad_interval").await?;
    enable_runtime_ads(&env, &[ClientDistribution::Web], 0, 10).await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([(ClientDistribution::Web, video_policy("test-provider"))]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;
    let user_id = env.create_user().await?;
    let server_addr = env.ws_addr(0).context("Server should exist")?;
    let mut client = TestClient::connect(&server_addr).await?;
    client
        .authenticate_for_distribution(user_id, ClientDistribution::Web)
        .await?;
    let lobby_code = client.create_lobby().await?;

    client
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Quickmatch,
        })
        .await?;
    let ad_break = wait_for_lobby_state(&mut client, &lobby_code, "ad_break")
        .await?
        .context("first queue request did not create an ad break")?;
    client
        .send_message(WSMessage::AdBreakResolved {
            break_id: ad_break.id,
            resolution: AdBreakResolution::Blocked,
        })
        .await?;
    wait_for_lobby_state(&mut client, &lobby_code, "queued").await?;

    client.send_message(WSMessage::LeaveQueue).await?;
    wait_for_lobby_state(&mut client, &lobby_code, "waiting").await?;
    client
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Quickmatch,
        })
        .await?;
    wait_for_queue_without_ad(&mut client, &lobby_code).await?;

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn minimum_games_policy_skips_newcomer_lobby() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("minimum_games_skips_newcomer").await?;
    enable_runtime_ads(&env, &[ClientDistribution::Web], 1, 10).await?;
    env.add_server_with_ads_config(AdsConfig {
        enabled: true,
        distributions: BTreeMap::from([(ClientDistribution::Web, video_policy("test-provider"))]),
        ad_break_timeout: Duration::from_secs(5),
    })
    .await?;
    let user_id = env.create_user().await?;
    let server_addr = env.ws_addr(0).context("Server should exist")?;
    let mut client = TestClient::connect(&server_addr).await?;
    client
        .authenticate_for_distribution(user_id, ClientDistribution::Web)
        .await?;
    let lobby_code = client.create_lobby().await?;
    client
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Quickmatch,
        })
        .await?;
    wait_for_queue_without_ad(&mut client, &lobby_code).await?;

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}
