use common::{GameType, QueueMode};
use server::ads::{AdBreakResolution, AdsConfig, ClientDistribution, LobbyAdBreak};
use server::matchmaking_pool::MatchmakingPool;
use std::collections::BTreeMap;

#[test]
fn public_client_policy_is_disabled_and_provider_neutral_by_default() {
    let value =
        serde_json::to_value(AdsConfig::default().client_config(Some(ClientDistribution::Web)))
            .unwrap();

    assert_eq!(
        value,
        serde_json::json!({
            "enabled": false,
            "provider": "none",
            "banners": {
                "bottom": false,
                "sides": false
            },
            "video": {
                "pre_match": false
            }
        })
    );
}

#[test]
fn durable_ad_break_round_trips_numeric_participants_and_terminal_outcomes() {
    let ad_break = LobbyAdBreak {
        id: "break-1".to_owned(),
        expires_at_ms: 1_750_000_000_000,
        participant_user_ids: vec![7, 9],
        ad_user_ids: vec![7],
        resolutions: BTreeMap::from([(7, AdBreakResolution::Blocked)]),
        game_types: vec![GameType::Solo],
        queue_mode: QueueMode::Quickmatch,
        requesting_user_id: 7,
        matchmaking_pool: MatchmakingPool::Public,
    };

    let json = serde_json::to_string(&ad_break).unwrap();
    assert!(json.contains(r#""resolutions":{"7":"blocked"}"#));

    let restored: LobbyAdBreak = serde_json::from_str(&json).unwrap();
    assert_eq!(restored, ad_break);
    assert!(!restored.is_resolved());
    assert_eq!(restored.view().resolved_user_ids, vec![7]);
}
