//! Server-owned advertisement policy and provider-neutral wire types.
//!
//! The server decides whether any advertisement surface is active. Browser
//! builds only resolve the advertised provider name to an adapter; a missing
//! SDK, ad blocker, or no-fill response is a normal terminal outcome.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::matchmaking_pool::MatchmakingPool;

pub const ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_ENABLED";
/// Removed because one server process can serve several distributions at the
/// same time. Kept as a named constant so startup can reject the ambiguous
/// legacy setting with an actionable error.
pub const ADS_PROVIDER_ENV: &str = "SNAKETRON_ADS_PROVIDER";
pub const BOTTOM_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_BOTTOM_BANNER_ENABLED";
pub const SIDE_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_SIDE_BANNERS_ENABLED";
pub const PRE_MATCH_VIDEO_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_PRE_MATCH_VIDEO_ENABLED";
pub const WEB_ADS_PROVIDER_ENV: &str = "SNAKETRON_ADS_WEB_PROVIDER";
pub const WEB_BOTTOM_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_WEB_BOTTOM_BANNER_ENABLED";
pub const WEB_SIDE_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_WEB_SIDE_BANNERS_ENABLED";
pub const WEB_PRE_MATCH_VIDEO_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_WEB_PRE_MATCH_VIDEO_ENABLED";
pub const CRAZYGAMES_ADS_PROVIDER_ENV: &str = "SNAKETRON_ADS_CRAZYGAMES_PROVIDER";
pub const CRAZYGAMES_BOTTOM_BANNER_ADS_ENABLED_ENV: &str =
    "SNAKETRON_ADS_CRAZYGAMES_BOTTOM_BANNER_ENABLED";
pub const CRAZYGAMES_SIDE_BANNER_ADS_ENABLED_ENV: &str =
    "SNAKETRON_ADS_CRAZYGAMES_SIDE_BANNERS_ENABLED";
pub const CRAZYGAMES_PRE_MATCH_VIDEO_ADS_ENABLED_ENV: &str =
    "SNAKETRON_ADS_CRAZYGAMES_PRE_MATCH_VIDEO_ENABLED";
pub const ITCH_ADS_PROVIDER_ENV: &str = "SNAKETRON_ADS_ITCH_PROVIDER";
pub const ITCH_BOTTOM_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_ITCH_BOTTOM_BANNER_ENABLED";
pub const ITCH_SIDE_BANNER_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_ITCH_SIDE_BANNERS_ENABLED";
pub const ITCH_PRE_MATCH_VIDEO_ADS_ENABLED_ENV: &str = "SNAKETRON_ADS_ITCH_PRE_MATCH_VIDEO_ENABLED";
/// Replaced by the versioned runtime policy managed through the admin API.
pub const ADS_MIN_GAMES_PLAYED_ENV: &str = "SNAKETRON_ADS_MIN_GAMES_PLAYED";
pub const AD_BREAK_TIMEOUT_SECONDS_ENV: &str = "SNAKETRON_AD_BREAK_TIMEOUT_SECONDS";

// Long enough for provider initialization plus a normal interstitial lifecycle.
// Clients with a larger provider-declared budget fail closed before submission.
const DEFAULT_AD_BREAK_TIMEOUT_SECONDS: u64 = 120;
const MAX_AD_BREAK_TIMEOUT_SECONDS: u64 = 300;
const MAX_AD_BREAK_ID_LENGTH: usize = 128;
const MAX_AD_PROVIDER_ID_LENGTH: usize = 64;
pub const MAX_AD_BREAK_PARTICIPANTS: usize = 4;

/// The build/distribution through which the current browser session was
/// launched. This is session metadata, not an account property: the same user
/// can legitimately play through several distributions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ClientDistribution {
    Web,
    CrazyGames,
    Itch,
}

impl ClientDistribution {
    pub const ALL: [Self; 3] = [Self::Web, Self::CrazyGames, Self::Itch];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::CrazyGames => "crazygames",
            Self::Itch => "itch",
        }
    }

    const fn environment(self) -> DistributionEnvironment {
        match self {
            Self::Web => DistributionEnvironment {
                provider: WEB_ADS_PROVIDER_ENV,
                bottom_banner: WEB_BOTTOM_BANNER_ADS_ENABLED_ENV,
                side_banners: WEB_SIDE_BANNER_ADS_ENABLED_ENV,
                pre_match_video: WEB_PRE_MATCH_VIDEO_ADS_ENABLED_ENV,
            },
            Self::CrazyGames => DistributionEnvironment {
                provider: CRAZYGAMES_ADS_PROVIDER_ENV,
                bottom_banner: CRAZYGAMES_BOTTOM_BANNER_ADS_ENABLED_ENV,
                side_banners: CRAZYGAMES_SIDE_BANNER_ADS_ENABLED_ENV,
                pre_match_video: CRAZYGAMES_PRE_MATCH_VIDEO_ADS_ENABLED_ENV,
            },
            Self::Itch => DistributionEnvironment {
                provider: ITCH_ADS_PROVIDER_ENV,
                bottom_banner: ITCH_BOTTOM_BANNER_ADS_ENABLED_ENV,
                side_banners: ITCH_SIDE_BANNER_ADS_ENABLED_ENV,
                pre_match_video: ITCH_PRE_MATCH_VIDEO_ADS_ENABLED_ENV,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DistributionEnvironment {
    provider: &'static str,
    bottom_banner: &'static str,
    side_banners: &'static str,
    pre_match_video: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BannerAdsConfig {
    pub bottom: bool,
    pub sides: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct VideoAdsConfig {
    pub pre_match: bool,
}

/// The complete advertisement capability advertised to a browser session.
/// There are intentionally no placement IDs or SDK-specific fields here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ClientAdsConfig {
    pub enabled: bool,
    pub provider: String,
    pub banners: BannerAdsConfig,
    pub video: VideoAdsConfig,
}

impl Default for ClientAdsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: "none".to_owned(),
            banners: BannerAdsConfig {
                bottom: false,
                sides: false,
            },
            video: VideoAdsConfig { pre_match: false },
        }
    }
}

/// Process-level policy. A distribution-specific client policy is resolved at
/// authentication time. Eligibility and frequency are live runtime policy;
/// only the provider capability ceiling and safety deadline live here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdsConfig {
    /// Process-wide kill switch. Distribution policies cannot override it.
    pub enabled: bool,
    pub distributions: BTreeMap<ClientDistribution, ClientAdsConfig>,
    pub ad_break_timeout: Duration,
}

impl Default for AdsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            distributions: ClientDistribution::ALL
                .into_iter()
                .map(|distribution| (distribution, ClientAdsConfig::default()))
                .collect(),
            ad_break_timeout: Duration::from_secs(DEFAULT_AD_BREAK_TIMEOUT_SECONDS),
        }
    }
}

impl AdsConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        for legacy_name in [
            ADS_PROVIDER_ENV,
            BOTTOM_BANNER_ADS_ENABLED_ENV,
            SIDE_BANNER_ADS_ENABLED_ENV,
            PRE_MATCH_VIDEO_ADS_ENABLED_ENV,
        ] {
            if lookup(legacy_name).is_some() {
                return Err(anyhow!(
                    "{legacy_name} is no longer supported because one server can serve multiple distributions; configure SNAKETRON_ADS_WEB_*, SNAKETRON_ADS_CRAZYGAMES_*, and SNAKETRON_ADS_ITCH_* instead"
                ));
            }
        }
        if lookup(ADS_MIN_GAMES_PLAYED_ENV).is_some() {
            return Err(anyhow!(
                "{ADS_MIN_GAMES_PLAYED_ENV} is no longer supported; configure the minimum-games policy through the versioned server runtime configuration"
            ));
        }

        let enabled =
            parse_optional_bool(ADS_ENABLED_ENV, lookup(ADS_ENABLED_ENV))?.unwrap_or(false);

        let mut distributions = BTreeMap::new();
        for distribution in ClientDistribution::ALL {
            let environment = distribution.environment();
            let provider = lookup(environment.provider)
                .map(|value| value.trim().to_lowercase())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "none".to_owned());
            validate_provider(environment.provider, &provider)?;

            // A provider selects the adapter for this distribution. The
            // process-wide switch remains the sole master control.
            let distribution_enabled = enabled && provider != "none";
            let bottom =
                parse_optional_bool(environment.bottom_banner, lookup(environment.bottom_banner))?
                    .unwrap_or(distribution_enabled);
            let sides =
                parse_optional_bool(environment.side_banners, lookup(environment.side_banners))?
                    .unwrap_or(distribution_enabled);
            let pre_match = parse_optional_bool(
                environment.pre_match_video,
                lookup(environment.pre_match_video),
            )?
            .unwrap_or(distribution_enabled);

            distributions.insert(
                distribution,
                ClientAdsConfig {
                    enabled: distribution_enabled,
                    provider: if distribution_enabled {
                        provider
                    } else {
                        "none".to_owned()
                    },
                    banners: BannerAdsConfig {
                        bottom: distribution_enabled && bottom,
                        sides: distribution_enabled && sides,
                    },
                    video: VideoAdsConfig {
                        pre_match: distribution_enabled && pre_match,
                    },
                },
            );
        }

        let timeout_seconds = parse_optional_number::<u64>(
            AD_BREAK_TIMEOUT_SECONDS_ENV,
            lookup(AD_BREAK_TIMEOUT_SECONDS_ENV),
        )?
        .unwrap_or(DEFAULT_AD_BREAK_TIMEOUT_SECONDS);
        if !(5..=MAX_AD_BREAK_TIMEOUT_SECONDS).contains(&timeout_seconds) {
            return Err(anyhow!(
                "{AD_BREAK_TIMEOUT_SECONDS_ENV} must be between 5 and {MAX_AD_BREAK_TIMEOUT_SECONDS}"
            ));
        }

        Ok(Self {
            enabled,
            distributions,
            ad_break_timeout: Duration::from_secs(timeout_seconds),
        })
    }

    /// Resolve the wire policy for one authenticated build. An absent
    /// distribution (legacy v8 or `Token` authentication) is deliberately
    /// disabled because it cannot safely identify the SDK it embeds.
    pub fn client_config(&self, distribution: Option<ClientDistribution>) -> ClientAdsConfig {
        if !self.enabled {
            return ClientAdsConfig::default();
        }
        distribution
            .and_then(|distribution| self.distributions.get(&distribution))
            .cloned()
            .unwrap_or_default()
    }

    pub fn any_pre_match_video_enabled(&self) -> bool {
        self.enabled
            && self
                .distributions
                .values()
                .any(|client| client.enabled && client.video.pre_match)
    }
}

fn validate_provider(name: &str, provider: &str) -> Result<()> {
    if provider.len() > MAX_AD_PROVIDER_ID_LENGTH
        || !provider
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(anyhow!(
            "{name} must be a provider identifier of at most {MAX_AD_PROVIDER_ID_LENGTH} characters"
        ));
    }
    Ok(())
}

fn parse_optional_bool(name: &str, value: Option<String>) -> Result<Option<bool>> {
    value
        .map(|value| match value.trim().to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("{name} must be true or false, got '{value}'")),
        })
        .transpose()
}

fn parse_optional_number<T>(name: &str, value: Option<String>) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value
        .map(|value| {
            value
                .trim()
                .parse::<T>()
                .with_context(|| format!("{name} must be a non-negative integer"))
        })
        .transpose()
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum AdBreakResolution {
    Completed,
    Blocked,
    Unavailable,
    Error,
    TimedOut,
}

impl AdBreakResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Unavailable => "unavailable",
            Self::Error => "error",
            Self::TimedOut => "timed_out",
        }
    }
}

/// Durable, provider-neutral state stored with lobby metadata in Redis.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LobbyAdBreak {
    pub id: String,
    pub expires_at_ms: i64,
    pub participant_user_ids: Vec<u32>,
    /// Participants authorized by the server's snapshotted runtime policy to
    /// submit a provider request. Other modern members resolve the barrier as
    /// unavailable without displaying an ad.
    pub ad_user_ids: Vec<u32>,
    #[serde(default)]
    pub resolutions: BTreeMap<u32, AdBreakResolution>,
    pub game_types: Vec<common::GameType>,
    pub queue_mode: common::QueueMode,
    pub requesting_user_id: u32,
    pub matchmaking_pool: MatchmakingPool,
}

impl LobbyAdBreak {
    /// Validate invariants relied on by the Redis transition scripts and the
    /// matchmaking roster fence. Participant IDs are canonicalized by callers
    /// before persistence so equality remains deterministic across servers.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty()
            || self.id.len() > MAX_AD_BREAK_ID_LENGTH
            || !self.id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(anyhow!(
                "ad-break id must contain 1 to {MAX_AD_BREAK_ID_LENGTH} URL-safe identifier bytes"
            ));
        }
        if self.expires_at_ms <= 0 {
            return Err(anyhow!("ad-break expiry must be a positive Unix timestamp"));
        }
        if self.participant_user_ids.is_empty() {
            return Err(anyhow!("ad break must contain at least one participant"));
        }
        if self.participant_user_ids.len() > MAX_AD_BREAK_PARTICIPANTS {
            return Err(anyhow!(
                "ad break may contain at most {MAX_AD_BREAK_PARTICIPANTS} participants"
            ));
        }
        if !self
            .participant_user_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(anyhow!(
                "ad-break participant IDs must be sorted and unique"
            ));
        }
        if self.ad_user_ids.is_empty()
            || !self.ad_user_ids.windows(2).all(|pair| pair[0] < pair[1])
            || self
                .ad_user_ids
                .iter()
                .any(|user_id| self.participant_user_ids.binary_search(user_id).is_err())
        {
            return Err(anyhow!(
                "ad-break target IDs must be a non-empty sorted subset of participants"
            ));
        }
        if self
            .participant_user_ids
            .binary_search(&self.requesting_user_id)
            .is_err()
        {
            return Err(anyhow!(
                "ad-break requester must belong to the participant roster"
            ));
        }
        if self
            .resolutions
            .keys()
            .any(|user_id| self.participant_user_ids.binary_search(user_id).is_err())
        {
            return Err(anyhow!(
                "ad-break resolutions may only reference participants"
            ));
        }
        if self.game_types.is_empty() {
            return Err(anyhow!("ad break must contain at least one game type"));
        }
        if self.game_types.len() > crate::matchmaking_manager::MATCHMAKING_GAME_TYPES.len()
            || self.game_types.iter().any(|game_type| {
                !crate::matchmaking_manager::MATCHMAKING_GAME_TYPES.contains(game_type)
            })
        {
            return Err(anyhow!(
                "ad break may only target supported matchmaking game types"
            ));
        }
        if self
            .game_types
            .iter()
            .enumerate()
            .any(|(index, game_type)| self.game_types[..index].contains(game_type))
        {
            return Err(anyhow!("ad-break game types must be unique"));
        }
        Ok(())
    }

    pub fn validate_new(&self, now_ms: i64) -> Result<()> {
        self.validate()?;
        if self.expires_at_ms <= now_ms {
            return Err(anyhow!("ad-break expiry must be in the future"));
        }
        let maximum_expiry_ms = now_ms.saturating_add(
            i64::try_from(Duration::from_secs(MAX_AD_BREAK_TIMEOUT_SECONDS).as_millis())
                .unwrap_or(i64::MAX),
        );
        if self.expires_at_ms > maximum_expiry_ms {
            return Err(anyhow!(
                "ad-break expiry may be at most {MAX_AD_BREAK_TIMEOUT_SECONDS} seconds in the future"
            ));
        }
        Ok(())
    }

    pub fn view(&self) -> LobbyAdBreakView {
        LobbyAdBreakView {
            id: self.id.clone(),
            expires_at_ms: self.expires_at_ms,
            participant_count: u32::try_from(self.participant_user_ids.len()).unwrap_or(u32::MAX),
            resolved_count: u32::try_from(self.resolutions.len()).unwrap_or(u32::MAX),
            resolved_user_ids: self.resolutions.keys().copied().collect(),
            ad_user_ids: self.ad_user_ids.clone(),
        }
    }

    pub fn is_resolved(&self) -> bool {
        self.participant_user_ids
            .iter()
            .all(|user_id| self.resolutions.contains_key(user_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LobbyAdBreakView {
    pub id: String,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub expires_at_ms: i64,
    pub participant_count: u32,
    pub resolved_count: u32,
    pub resolved_user_ids: Vec<u32>,
    pub ad_user_ids: Vec<u32>,
}

pub fn lobby_meets_game_threshold<'a>(
    games_played: impl IntoIterator<Item = &'a i32>,
    minimum: u32,
) -> bool {
    let minimum = i64::from(minimum);
    let mut found_member = false;
    for played in games_played {
        found_member = true;
        if i64::from(*played) < minimum {
            return false;
        }
    }
    found_member
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(values: &[(&str, &str)]) -> Result<AdsConfig> {
        let values: HashMap<&str, &str> = values.iter().copied().collect();
        AdsConfig::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
    }

    #[test]
    fn ads_are_disabled_by_default() {
        assert_eq!(config(&[]).unwrap(), AdsConfig::default());
    }

    #[test]
    fn global_switch_controls_every_distribution_surface() {
        let disabled = config(&[
            (WEB_ADS_PROVIDER_ENV, "adsense_h5"),
            (WEB_BOTTOM_BANNER_ADS_ENABLED_ENV, "true"),
            (WEB_SIDE_BANNER_ADS_ENABLED_ENV, "true"),
            (WEB_PRE_MATCH_VIDEO_ADS_ENABLED_ENV, "true"),
        ])
        .unwrap();
        assert_eq!(
            disabled.client_config(Some(ClientDistribution::Web)),
            ClientAdsConfig::default()
        );

        let enabled = config(&[
            (ADS_ENABLED_ENV, "true"),
            (WEB_ADS_PROVIDER_ENV, "Adsense_H5"),
            (WEB_SIDE_BANNER_ADS_ENABLED_ENV, "false"),
            (CRAZYGAMES_ADS_PROVIDER_ENV, "CrazyGames"),
            (ITCH_ADS_PROVIDER_ENV, "none"),
        ])
        .unwrap();
        let web = enabled.client_config(Some(ClientDistribution::Web));
        assert!(web.enabled);
        assert!(web.banners.bottom);
        assert!(!web.banners.sides);
        assert!(web.video.pre_match);
        assert_eq!(web.provider, "adsense_h5");

        let crazygames = enabled.client_config(Some(ClientDistribution::CrazyGames));
        assert!(crazygames.enabled);
        assert_eq!(crazygames.provider, "crazygames");
        assert_eq!(
            enabled.client_config(Some(ClientDistribution::Itch)),
            ClientAdsConfig::default()
        );
        assert_eq!(enabled.client_config(None), ClientAdsConfig::default());
        assert!(enabled.any_pre_match_video_enabled());
    }

    #[test]
    fn config_rejects_invalid_values() {
        assert!(config(&[(ADS_ENABLED_ENV, "sometimes")]).is_err());
        assert!(config(&[(ADS_PROVIDER_ENV, "invalid provider")]).is_err());
        assert!(config(&[(BOTTOM_BANNER_ADS_ENABLED_ENV, "true")]).is_err());
        assert!(config(&[(WEB_ADS_PROVIDER_ENV, "invalid provider")]).is_err());
        assert!(config(&[(WEB_PRE_MATCH_VIDEO_ADS_ENABLED_ENV, "sometimes")]).is_err());
        assert!(config(&[(ADS_MIN_GAMES_PLAYED_ENV, "1")]).is_err());
        assert!(config(&[(AD_BREAK_TIMEOUT_SECONDS_ENV, "4")]).is_err());
        assert!(config(&[(AD_BREAK_TIMEOUT_SECONDS_ENV, "301")]).is_err());
    }

    #[test]
    fn distribution_wire_names_are_stable() {
        for (distribution, expected) in [
            (ClientDistribution::Web, "web"),
            (ClientDistribution::CrazyGames, "crazygames"),
            (ClientDistribution::Itch, "itch"),
        ] {
            assert_eq!(distribution.as_str(), expected);
            assert_eq!(
                serde_json::to_string(&distribution).unwrap(),
                format!(r#""{expected}""#)
            );
        }
    }

    #[test]
    fn every_lobby_member_must_meet_the_threshold() {
        let veteran = 5;
        let newcomer = 0;
        assert!(lobby_meets_game_threshold([&veteran], 1));
        assert!(!lobby_meets_game_threshold([&veteran, &newcomer], 1));
        assert!(lobby_meets_game_threshold([&newcomer], 0));
        assert!(!lobby_meets_game_threshold(std::iter::empty(), 0));
    }

    fn sample_ad_break() -> LobbyAdBreak {
        LobbyAdBreak {
            id: "break-1".to_owned(),
            expires_at_ms: 100_000,
            participant_user_ids: vec![10, 20],
            ad_user_ids: vec![10],
            resolutions: BTreeMap::new(),
            game_types: vec![common::GameType::TeamMatch { per_team: 1 }],
            queue_mode: common::QueueMode::Quickmatch,
            requesting_user_id: 10,
            matchmaking_pool: MatchmakingPool::Public,
        }
    }

    #[test]
    fn ad_break_validation_rejects_ambiguous_or_unsafe_payloads() {
        let valid = sample_ad_break();
        assert!(valid.validate_new(10_000).is_ok());

        let mut duplicate_participant = valid.clone();
        duplicate_participant.participant_user_ids = vec![10, 10];
        assert!(duplicate_participant.validate().is_err());

        let mut missing_requester = valid.clone();
        missing_requester.requesting_user_id = 30;
        assert!(missing_requester.validate().is_err());

        let mut foreign_target = valid.clone();
        foreign_target.ad_user_ids = vec![30];
        assert!(foreign_target.validate().is_err());

        let mut no_target = valid.clone();
        no_target.ad_user_ids.clear();
        assert!(no_target.validate().is_err());

        let mut foreign_resolution = valid.clone();
        foreign_resolution
            .resolutions
            .insert(30, AdBreakResolution::Completed);
        assert!(foreign_resolution.validate().is_err());

        let mut unsupported_game_type = valid.clone();
        unsupported_game_type.game_types = vec![common::GameType::FreeForAll { max_players: 3 }];
        assert!(unsupported_game_type.validate().is_err());

        let mut duplicate_game_type = valid.clone();
        duplicate_game_type.game_types = vec![common::GameType::Solo, common::GameType::Solo];
        assert!(duplicate_game_type.validate().is_err());

        let mut expired = valid.clone();
        expired.expires_at_ms = 10_000;
        assert!(expired.validate_new(10_000).is_err());
    }

    #[test]
    fn ad_break_json_round_trip_preserves_numeric_resolution_keys() {
        let mut ad_break = sample_ad_break();
        ad_break.resolutions.insert(10, AdBreakResolution::Blocked);

        let json = serde_json::to_string(&ad_break).unwrap();
        assert!(json.contains(r#""resolutions":{"10":"blocked"}"#));
        let decoded: LobbyAdBreak = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ad_break);
        assert_eq!(decoded.view().resolved_user_ids, vec![10]);
    }

    #[test]
    fn lua_resolution_names_match_serde_and_missing_map_defaults_empty() {
        for (resolution, expected) in [
            (AdBreakResolution::Completed, "completed"),
            (AdBreakResolution::Blocked, "blocked"),
            (AdBreakResolution::Unavailable, "unavailable"),
            (AdBreakResolution::Error, "error"),
            (AdBreakResolution::TimedOut, "timed_out"),
        ] {
            assert_eq!(resolution.as_str(), expected);
            assert_eq!(
                serde_json::to_string(&resolution).unwrap(),
                format!(r#""{expected}""#)
            );
        }

        let mut value = serde_json::to_value(sample_ad_break()).unwrap();
        value.as_object_mut().unwrap().remove("resolutions");
        let decoded: LobbyAdBreak = serde_json::from_value(value).unwrap();
        assert!(decoded.resolutions.is_empty());
        assert!(decoded.validate().is_ok());
    }
}
