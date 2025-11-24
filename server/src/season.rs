pub type Season = u32;

/// Get the current season identifier.
/// Placeholder until a season schedule/roller exists.
pub fn get_current_season() -> Season {
    // TODO: replace with real season scheduler/roller
    0
}

/// Get the region from environment or default
pub fn get_region() -> String {
    std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
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
}
