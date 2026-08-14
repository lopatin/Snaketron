pub type Season = u32;

/// Get the current season identifier.
/// Placeholder until a season schedule/roller exists.
pub fn get_current_season() -> Season {
    // TODO: replace with real season scheduler/roller
    0
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
    fn test_season_non_negative() {
        assert_eq!(get_current_season(), 0);
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
