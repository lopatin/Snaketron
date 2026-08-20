use chrono::{DateTime, Datelike, Utc};

pub type Season = u32;

/// Snaketron seasons follow the UTC calendar quarters used by the original
/// leaderboard implementation. Existing numeric Season 0 data remains in the
/// launch season (Q3 2026); the first automatic rollover is Q4 2026.
const SEASON_ZERO_YEAR: i32 = 2026;
const SEASON_ZERO_QUARTER: i64 = 2; // Zero-based: Q3.
const QUARTERS_PER_YEAR: i64 = 4;

/// Resolve the season containing one authoritative UTC timestamp.
///
/// All timestamps before the numeric-season launch are deliberately folded
/// into Season 0 so existing leaderboard and high-score rows remain readable.
/// From 2026-10-01T00:00:00Z onward the value increments at every UTC calendar
/// quarter boundary without configuration, deployment, or process restart.
pub fn get_season_at(timestamp: DateTime<Utc>) -> Season {
    let quarter = i64::from(timestamp.month0() / 3);
    let quarter_index = i64::from(timestamp.year()) * QUARTERS_PER_YEAR + quarter;
    let season_zero_index = i64::from(SEASON_ZERO_YEAR) * QUARTERS_PER_YEAR + SEASON_ZERO_QUARTER;
    let elapsed_quarters = quarter_index.saturating_sub(season_zero_index).max(0);

    Season::try_from(elapsed_quarters)
        .expect("chrono's supported year range fits in the numeric season storage type")
}

/// Resolve the season active at the instant this function is called.
pub fn get_current_season() -> Season {
    get_season_at(Utc::now())
}

/// Return every numeric season through the supplied timestamp, newest first.
/// This keeps Season 0 selectable after later rollovers instead of orphaning
/// the leaderboard data created during launch.
pub fn seasons_at(timestamp: DateTime<Utc>) -> Vec<Season> {
    let current = get_season_at(timestamp);
    (0..=current).rev().collect()
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
    use chrono::TimeZone;

    fn utc(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .unwrap()
    }

    #[test]
    fn season_zero_absorbs_history_and_the_launch_quarter() {
        assert_eq!(get_season_at(utc(2020, 1, 1, 0, 0, 0)), 0);
        assert_eq!(get_season_at(utc(2026, 7, 1, 0, 0, 0)), 0);
        assert_eq!(get_season_at(utc(2026, 9, 30, 23, 59, 59)), 0);
    }

    #[test]
    fn seasons_roll_exactly_on_utc_quarter_boundaries() {
        assert_eq!(get_season_at(utc(2026, 10, 1, 0, 0, 0)), 1);
        assert_eq!(get_season_at(utc(2026, 12, 31, 23, 59, 59)), 1);
        assert_eq!(get_season_at(utc(2027, 1, 1, 0, 0, 0)), 2);
        assert_eq!(get_season_at(utc(2027, 4, 1, 0, 0, 0)), 3);
        assert_eq!(get_season_at(utc(2027, 7, 1, 0, 0, 0)), 4);
        assert_eq!(get_season_at(utc(2027, 10, 1, 0, 0, 0)), 5);
    }

    #[test]
    fn seasons_are_newest_first_and_preserve_season_zero() {
        assert_eq!(seasons_at(utc(2026, 8, 14, 12, 0, 0)), vec![0]);
        assert_eq!(seasons_at(utc(2027, 7, 1, 0, 0, 0)), vec![4, 3, 2, 1, 0]);
    }

    #[test]
    fn test_region_default() {
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
