use chrono::{Datelike, Utc};

/// Get the current season identifier
/// Format: YYYY-SN where N is the season number (1-4, one per quarter)
pub fn get_current_season() -> String {
    // Check environment variable first
    if let Ok(season) = std::env::var("CURRENT_SEASON") {
        return season;
    }

    // Auto-generate based on current date
    let now = Utc::now();
    let year = now.year();
    let month = now.month();

    // Determine season based on quarter
    let season_num = match month {
        1..=3 => 1,   // Q1: Jan-Mar
        4..=6 => 2,   // Q2: Apr-Jun
        7..=9 => 3,   // Q3: Jul-Sep
        10..=12 => 4, // Q4: Oct-Dec
        _ => 1,
    };

    format!("{}-S{}", year, season_num)
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
    fn test_season_format() {
        let season = get_current_season();
        // Should match pattern YYYY-SN
        assert!(season.contains("-S"));
        assert!(season.len() >= 7); // e.g., "2025-S1"
    }

    #[test]
    fn test_region_default() {
        // Test that we can get a region (might be from env or default)
        let region = get_region();
        assert!(!region.is_empty());
    }
}
