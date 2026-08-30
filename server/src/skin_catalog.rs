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

use serde::Serialize;

/// The look every client can render, and the answer to any question this
/// module cannot resolve.
pub const DEFAULT_SKIN_REF: &str = "classic@1";

/// What a skin dresses. Snake skins paint one player's body and travel to
/// every other player; base skins theme the arena the wearer is looking at and
/// never leave that client (`specs/skins-prd.md` section 5.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum SkinKind {
    Snake,
    Base,
}

/// One catalogue entry, as the browse API presents it.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CatalogEntry {
    /// The reference that travels in game state and persists on the user item.
    pub reference: &'static str,
    /// What a player sees in the picker.
    pub name: &'static str,
    pub kind: SkinKind,
    /// Built-ins cost nothing and everyone already has them. The field exists
    /// so the browse shape does not change when priced skins arrive.
    pub price_bux: u32,
}

/// Every selectable skin, in catalogue order.
///
/// Kept in step with the client's registry by
/// `client/src/skin/registry.rs`. The two lists are checked against each other
/// by `the_client_catalogue_matches_the_servers` in the client crate's tests.
pub const CATALOG: &[CatalogEntry] = &[
    entry("classic@1", "Classic"),
    entry("aurora@1", "Aurora"),
    entry("ember@1", "Ember"),
    entry("tidewave@1", "Tidewave"),
    entry("voltage@1", "Voltage"),
    entry("lantern@1", "Lantern"),
    entry("breaker@1", "Breaker"),
    entry("afterburn@1", "Afterburn"),
    entry("cinder@1", "Cinder"),
    entry("floe@1", "Floe"),
    entry("woodblock@1", "Woodblock"),
    entry("bloom@1", "Bloom"),
    entry("slipstream@1", "Slipstream"),
    entry("harrier@1", "Harrier"),
    entry("argyle@1", "Argyle"),
    entry("serpentine@1", "Serpentine"),
    entry("voltcore@1", "Voltcore"),
    entry("marquee@1", "Marquee"),
    entry("basalt@1", "Basalt"),
    entry("ripple@1", "Ripple"),
    entry("moth@1", "Moth"),
    entry("prism@1", "Prism"),
    entry("houndstooth@1", "Houndstooth"),
    entry("circuit@1", "Circuit"),
    entry("pinstripe@1", "Pinstripe"),
    entry("static@1", "Static"),
    entry("mosaic@1", "Mosaic"),
    entry("herringbone@1", "Herringbone"),
    entry("carbon@1", "Carbon"),
    entry("tartan@1", "Tartan"),
    entry("coral@1", "Coral"),
    entry("amber@1", "Amber"),
    entry("geode@1", "Geode"),
    entry("timber@1", "Timber"),
    entry("peacock@1", "Peacock"),
    entry("monarch@1", "Monarch"),
    entry("chrome@1", "Chrome"),
    entry("neon@1", "Neon"),
    entry("camo@1", "Camo"),
    entry("loom@1", "Loom"),
    entry("delft@1", "Delft"),
    entry("rosette@1", "Rosette"),
    entry("gambit@1", "Gambit"),
    entry("harlequin@1", "Harlequin"),
    entry("pitlane@1", "Pitlane"),
];

/// Every snake skin also supplies base dressing, so the base picker offers the
/// same looks addressed at the other slot. These are not separate registry
/// entries — `base:<snake ref>` names "the base theme belonging to that skin",
/// which is exactly what the renderer already resolves from a skin's
/// `base_theme()`.
pub const BASE_REF_PREFIX: &str = "base:";

const fn entry(reference: &'static str, name: &'static str) -> CatalogEntry {
    CatalogEntry {
        reference,
        name,
        kind: SkinKind::Snake,
        price_bux: 0,
    }
}

/// Longest id worth considering. Sized for a future `sha256:<64 hex>` ref so
/// this limit does not have to move when player-authored skins land.
pub const MAX_SKIN_REF_LENGTH: usize = 96;

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
        .find(|known| known.reference == trimmed)
        .map(|known| known.reference)
        .unwrap_or(DEFAULT_SKIN_REF)
}

/// Whether an id names a skin this build knows.
pub fn is_known(skin_ref: &str) -> bool {
    CATALOG.iter().any(|entry| entry.reference == skin_ref)
}

/// Whether an id names a base a player may equip.
///
/// A base reference is a snake reference wearing the `base:` prefix, so the
/// base slot inherits the catalogue without a second list to keep in step.
pub fn is_known_base(base_ref: &str) -> bool {
    base_ref.strip_prefix(BASE_REF_PREFIX).is_some_and(is_known)
}

/// The catalogue as base entries, for the base picker.
pub fn base_catalog() -> Vec<CatalogEntry> {
    CATALOG
        .iter()
        .map(|entry| CatalogEntry {
            reference: entry.reference,
            name: entry.name,
            kind: SkinKind::Base,
            price_bux: entry.price_bux,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_catalogue_entry_resolves_to_itself() {
        for entry in CATALOG {
            assert_eq!(resolve_skin_ref(Some(entry.reference)), entry.reference);
            assert!(is_known(entry.reference));
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
            assert!(
                seen.insert(entry.reference),
                "{} appears twice",
                entry.reference
            );
        }
        assert_eq!(
            CATALOG.first().map(|entry| entry.reference),
            Some(DEFAULT_SKIN_REF),
            "the fallback has to be in the catalogue, and first is the tidiest place"
        );
    }

    /// A base reference is only ever a prefixed snake reference, so the two
    /// slots can never drift into naming different sets of looks.
    #[test]
    fn base_references_are_prefixed_catalogue_entries() {
        assert!(is_known_base("base:aurora@1"));
        assert!(is_known_base("base:classic@1"));
        assert!(!is_known_base("aurora@1"), "a bare snake ref is not a base");
        assert!(!is_known_base("base:nonesuch@9"));
        assert!(!is_known_base("base:"));
        assert_eq!(base_catalog().len(), CATALOG.len());
        assert!(
            base_catalog()
                .iter()
                .all(|entry| entry.kind == SkinKind::Base)
        );
    }

    #[test]
    fn every_entry_has_a_display_name() {
        for entry in CATALOG {
            assert!(!entry.name.is_empty(), "{} has no name", entry.reference);
            assert_eq!(entry.kind, SkinKind::Snake);
            assert_eq!(entry.price_bux, 0, "built-ins are free");
        }
    }
}
