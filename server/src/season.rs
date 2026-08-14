use std::env::VarError;
use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

pub type Season = u32;

pub const CURRENT_SEASON_ENV: &str = "SNAKETRON_CURRENT_SEASON";
pub const DEFAULT_CURRENT_SEASON: Season = 0;
/// DynamoDB model readers use signed 32-bit numeric fields throughout the
/// existing leaderboard schema, so configuration must stay in that range.
pub const MAX_CURRENT_SEASON: Season = i32::MAX as Season;

static CURRENT_SEASON: OnceLock<Season> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentSeasonConfigError {
    Empty,
    Invalid(String),
    OutOfRange(String),
    NotUnicode,
    AlreadyInitialized {
        initialized: Season,
        configured: Season,
    },
}

impl fmt::Display for CurrentSeasonConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(
                formatter,
                "{CURRENT_SEASON_ENV} must not be empty when it is set"
            ),
            Self::Invalid(value) => write!(
                formatter,
                "{CURRENT_SEASON_ENV} must be an unsigned base-10 integer, got {value:?}"
            ),
            Self::OutOfRange(value) => write!(
                formatter,
                "{CURRENT_SEASON_ENV} is outside the supported season range, got {value:?}"
            ),
            Self::NotUnicode => write!(formatter, "{CURRENT_SEASON_ENV} must be valid UTF-8"),
            Self::AlreadyInitialized {
                initialized,
                configured,
            } => write!(
                formatter,
                "current season is already initialized to {initialized}, but the environment now resolves to {configured}"
            ),
        }
    }
}

impl Error for CurrentSeasonConfigError {}

/// Resolve and cache the season used by this server process.
///
/// Call this during startup, after loading any `.env` file and before starting
/// database-backed services. An unset value defaults to Season 0 for local
/// development; an explicitly configured value must be valid.
pub fn initialize_current_season() -> Result<Season, CurrentSeasonConfigError> {
    let configured = current_season_from_env()?;

    if let Some(&initialized) = CURRENT_SEASON.get() {
        return if initialized == configured {
            Ok(initialized)
        } else {
            Err(CurrentSeasonConfigError::AlreadyInitialized {
                initialized,
                configured,
            })
        };
    }

    // Another thread could initialize between the check and the set. Reading
    // the stored value afterward keeps this correct without a mutable global.
    let _ = CURRENT_SEASON.set(configured);
    let initialized = *CURRENT_SEASON
        .get()
        .expect("current season must be set by this point");

    if initialized == configured {
        Ok(initialized)
    } else {
        Err(CurrentSeasonConfigError::AlreadyInitialized {
            initialized,
            configured,
        })
    }
}

/// Return the immutable current season for this process.
///
/// The main server initializes this value explicitly so invalid deployment
/// configuration fails startup. Lazy initialization remains as a safeguard for
/// library consumers and local tools that do not run the server binary.
pub fn get_current_season() -> Season {
    *CURRENT_SEASON.get_or_init(|| {
        current_season_from_env().unwrap_or_else(|error| {
            panic!("invalid current-season configuration: {error}");
        })
    })
}

fn current_season_from_env() -> Result<Season, CurrentSeasonConfigError> {
    match std::env::var(CURRENT_SEASON_ENV) {
        Ok(value) => parse_current_season(Some(&value)),
        Err(VarError::NotPresent) => parse_current_season(None),
        Err(VarError::NotUnicode(_)) => Err(CurrentSeasonConfigError::NotUnicode),
    }
}

fn parse_current_season(value: Option<&str>) -> Result<Season, CurrentSeasonConfigError> {
    let Some(value) = value else {
        return Ok(DEFAULT_CURRENT_SEASON);
    };

    if value.is_empty() {
        return Err(CurrentSeasonConfigError::Empty);
    }

    if !value.bytes().all(|character| character.is_ascii_digit()) {
        return Err(CurrentSeasonConfigError::Invalid(value.to_string()));
    }

    value
        .parse::<Season>()
        .ok()
        .filter(|season| *season <= MAX_CURRENT_SEASON)
        .ok_or_else(|| CurrentSeasonConfigError::OutOfRange(value.to_string()))
}

fn resolve_storage_region(
    snaketron_aws_region: Option<String>,
    aws_region: Option<String>,
    legacy_region: Option<String>,
    snaketron_region_fallback: Option<String>,
) -> String {
    [
        snaketron_aws_region,
        aws_region,
        legacy_region,
        snaketron_region_fallback,
    ]
    .into_iter()
    .flatten()
    .find_map(|region| {
        let region = region.trim();
        (!region.is_empty()).then(|| region.to_string())
    })
    .unwrap_or_else(|| "us-east-1".to_string())
}

/// Get the physical region used by the existing ranking/high-score keyspace.
pub fn get_region() -> String {
    resolve_storage_region(
        std::env::var("SNAKETRON_AWS_REGION").ok(),
        std::env::var("AWS_REGION").ok(),
        std::env::var("REGION").ok(),
        std::env::var("SNAKETRON_REGION").ok(),
    )
}

fn resolve_ranking_region(
    requested_region: Option<&str>,
    logical_server_region: Option<&str>,
    storage_region: &str,
) -> String {
    let Some(requested) = requested_region
        .map(str::trim)
        .filter(|region| !region.is_empty())
    else {
        return storage_region.to_string();
    };

    if logical_server_region
        .map(str::trim)
        .is_some_and(|logical| requested.eq_ignore_ascii_case(logical))
    {
        return storage_region.to_string();
    }

    match requested.to_ascii_lowercase().as_str() {
        "use1" => "us-east-1".to_string(),
        "euw1" => "eu-west-1".to_string(),
        "usw2" => "us-west-2".to_string(),
        "aps2" => "ap-southeast-2".to_string(),
        "ane1" => "ap-northeast-1".to_string(),
        _ => requested.to_string(),
    }
}

/// Translate the logical matchmaking region into the physical ranking
/// keyspace without invalidating existing regional leaderboard rows.
pub fn get_ranking_region(requested_region: Option<&str>) -> String {
    let storage_region = get_region();
    let logical_server_region = std::env::var("SNAKETRON_REGION").ok();
    resolve_ranking_region(
        requested_region,
        logical_server_region.as_deref(),
        &storage_region,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_current_season_defaults_to_season_zero() {
        assert_eq!(parse_current_season(None), Ok(DEFAULT_CURRENT_SEASON));
    }

    #[test]
    fn parses_supported_current_seasons() {
        assert_eq!(parse_current_season(Some("0")), Ok(0));
        assert_eq!(parse_current_season(Some("12")), Ok(12));
        assert_eq!(
            parse_current_season(Some(&MAX_CURRENT_SEASON.to_string())),
            Ok(MAX_CURRENT_SEASON)
        );
    }

    #[test]
    fn rejects_empty_or_non_decimal_current_seasons() {
        assert_eq!(
            parse_current_season(Some("")),
            Err(CurrentSeasonConfigError::Empty)
        );

        for value in ["-1", "+1", " 1", "1 ", "1.5", "season-1"] {
            assert_eq!(
                parse_current_season(Some(value)),
                Err(CurrentSeasonConfigError::Invalid(value.to_string()))
            );
        }
    }

    #[test]
    fn rejects_current_season_larger_than_storage_type() {
        let value = (u64::from(MAX_CURRENT_SEASON) + 1).to_string();
        assert_eq!(
            parse_current_season(Some(&value)),
            Err(CurrentSeasonConfigError::OutOfRange(value))
        );
    }

    #[test]
    fn test_region_default() {
        // Test that we can get a region (might be from env or default)
        let region = get_region();
        assert!(!region.is_empty());
    }

    #[test]
    fn dedicated_aws_region_wins_for_ranking_storage() {
        assert_eq!(
            resolve_storage_region(
                Some("eu-west-1".to_string()),
                Some("us-east-1".to_string()),
                None,
                Some("euw1".to_string()),
            ),
            "eu-west-1"
        );
    }

    #[test]
    fn storage_region_keeps_legacy_fallbacks() {
        assert_eq!(
            resolve_storage_region(None, Some("eu-west-1".to_string()), None, None),
            "eu-west-1"
        );
        assert_eq!(
            resolve_storage_region(None, None, Some("legacy".to_string()), None),
            "legacy"
        );
        assert_eq!(
            resolve_storage_region(Some("  ".to_string()), None, None, None),
            "us-east-1"
        );
    }

    #[test]
    fn logical_matchmaking_regions_resolve_to_existing_ranking_regions() {
        assert_eq!(
            resolve_ranking_region(Some("us"), Some("us"), "us-east-1"),
            "us-east-1"
        );
        assert_eq!(
            resolve_ranking_region(Some("use1"), Some("euw1"), "eu-west-1"),
            "us-east-1"
        );
        assert_eq!(
            resolve_ranking_region(Some("eu-west-1"), Some("use1"), "us-east-1"),
            "eu-west-1"
        );
        assert_eq!(
            resolve_ranking_region(None, Some("use1"), "us-east-1"),
            "us-east-1"
        );
    }
}
