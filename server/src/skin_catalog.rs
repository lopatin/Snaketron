//! The server's view of which skins exist.
//!
//! Clients tell the server what they want to wear, and that choice ends up in
//! game state where it reaches every other player's renderer. So the server
//! decides what is real: an id nobody recognises becomes the classic look here,
//! before it can travel, rather than being trusted and then quietly ignored by
//! whichever clients happen to be new enough to notice.
//!
//! The catalogue is deliberately a small allowlist rather than anything
//! open-ended. Player-authored skins will be content-addressed and validated by
//! the shared `skin-schema` crate when they arrive; until then, an unknown id
//! is simply not a skin.

/// The look every client can render, and the answer to any question this
/// module cannot resolve.
pub const DEFAULT_SKIN_REF: &str = "classic@1";

/// Every selectable skin, in catalogue order.
///
/// Kept in step with the client's registry by
/// `client/src/skin/registry.rs`. The two lists are checked against each other
/// by `skin_catalog_matches_the_client_registry` in the client crate's tests.
pub const CATALOG: &[&str] = &[
    "classic@1",
    "aurora@1",
    "ember@1",
    "tidewave@1",
    "voltage@1",
    "lantern@1",
    "gambit@1",
    "harlequin@1",
    "pitlane@1",
];

/// Longest id worth considering. Sized for a future `sha256:<64 hex>` ref so
/// this limit does not have to move when player-authored skins land.
const MAX_SKIN_REF_LENGTH: usize = 96;

/// Resolve a client's requested skin to something safe to publish.
///
/// Never fails and never rejects a player: an unusable request becomes the
/// classic look. Cosmetics are not worth refusing a join over.
pub fn resolve_skin_ref(requested: Option<&str>) -> &'static str {
    let Some(requested) = requested else {
        return DEFAULT_SKIN_REF;
    };
    let trimmed = requested.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_SKIN_REF_LENGTH {
        return DEFAULT_SKIN_REF;
    }
    CATALOG
        .iter()
        .find(|known| **known == trimmed)
        .copied()
        .unwrap_or(DEFAULT_SKIN_REF)
}

/// Whether an id names a skin this build knows.
pub fn is_known(skin_ref: &str) -> bool {
    CATALOG.contains(&skin_ref)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalogue_entry_resolves_to_itself() {
        for entry in CATALOG {
            assert_eq!(resolve_skin_ref(Some(entry)), *entry);
            assert!(is_known(entry));
        }
    }

    /// The property that matters: whatever a client sends, what gets published
    /// is always something every other client can draw.
    #[test]
    fn anything_unrecognised_becomes_the_classic_look() {
        for requested in [
            None,
            Some(""),
            Some("   "),
            Some("classic"),
            Some("CLASSIC@1"),
            Some("aurora@2"),
            Some("../../etc/passwd"),
            Some("<script>alert(1)</script>"),
            Some("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
        ] {
            let resolved = resolve_skin_ref(requested);
            assert_eq!(
                resolved, DEFAULT_SKIN_REF,
                "{requested:?} should have fallen back to classic"
            );
            assert!(is_known(resolved));
        }
    }

    #[test]
    fn an_absurdly_long_id_is_refused_before_it_is_compared() {
        let long = "a".repeat(MAX_SKIN_REF_LENGTH + 1);
        assert_eq!(resolve_skin_ref(Some(&long)), DEFAULT_SKIN_REF);
    }

    #[test]
    fn surrounding_whitespace_does_not_hide_a_real_skin() {
        assert_eq!(resolve_skin_ref(Some("  aurora@1  ")), "aurora@1");
    }

    #[test]
    fn the_catalogue_has_no_duplicates_and_starts_with_the_default() {
        let mut seen = std::collections::HashSet::new();
        for entry in CATALOG {
            assert!(seen.insert(*entry), "{entry} appears twice");
        }
        assert_eq!(
            CATALOG.first().copied(),
            Some(DEFAULT_SKIN_REF),
            "the fallback has to be in the catalogue, and first is the tidiest place"
        );
    }
}
