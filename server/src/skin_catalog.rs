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

/// What a skin dresses.
///
/// A snake skin paints one player's body. A base skin paints one team's
/// endzone, and paints it for everybody: it is chosen by the team that owns
/// that end of the arena and published in `GameState.team_bases`, so unlike
/// the per-snake base *themes* it replaced it does not stop at the wearer's
/// own screen (`specs/base-skins-prd.md`).
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
    entry("gambit@1", "Gambit"),
    entry("harlequin@1", "Harlequin"),
    entry("pitlane@1", "Pitlane"),
    entry("zebra@1", "Zebra"),
    entry("zebra-print@1", "Zebra Print"),
    entry("tiger@1", "Tiger"),
    entry("tiger-print@1", "Tiger Print"),
    entry("jaguar@1", "Jaguar"),
    entry("jaguar-print@1", "Jaguar Print"),
    entry("zebra-live@1", "Living Zebra"),
    entry("tiger-live@1", "Living Tiger"),
    entry("stars-and-stripes@1", "Stars and Stripes"),
    entry("race-livery@1", "Race Livery"),
];

/// The base slot's reference prefix.
///
/// One prefix, two kinds of thing behind it, and the difference is the whole
/// of how base skins were added without breaking what players already had
/// equipped:
///
/// - `base:<base skin id>` names a **base skin** — a picture and a lettering
///   colour ([`BASE_SKINS`]). It belongs to the team that owns the endzone, so
///   it travels into game state and every player sees it.
/// - `base:<snake ref>` names the **base theme** carried by that snake skin —
///   six colours resolved from the client's own `base_theme()`. That is what
///   the base slot meant before base skins existed, it is what every value
///   stored on an account today looks like, and it is still viewer-local: it
///   dresses the arena you are looking at and nobody else's.
///
/// The two can never collide because a base skin id is not a snake skin id,
/// and [`the_two_base_namespaces_do_not_overlap`] holds that true.
pub const BASE_REF_PREFIX: &str = "base:";

/// One base skin, as the browse API presents it.
///
/// The client owns the picture and the lettering colour
/// (`client/src/skin/base_skin.rs`); the server owns only the question of
/// which ids are real, because that is the answer that has to be the same for
/// everybody. `the_client_base_catalogue_matches_the_servers` in the client
/// crate keeps the two lists in step.
pub const BASE_SKINS: &[CatalogEntry] = &[
    base_entry("invaders@1", "Invaders"),
    base_entry("lightcycle@1", "Lightcycle Grid"),
    base_entry("python@1", "Python"),
    base_entry("dragon@1", "Dragon"),
    base_entry("sharkbite@1", "Shark Bite"),
    base_entry("aquarium@1", "Aquarium"),
    base_entry("surf@1", "Surf"),
    base_entry("fairway@1", "Fairway"),
    base_entry("destroyer@1", "Destroyer"),
    base_entry("blockcraft@1", "Blockcraft"),
    base_entry("anime@1", "Anime"),
    base_entry("kittens@1", "Kittens"),
    base_entry("bears@1", "Dancing Bears"),
    base_entry("barbershop@1", "Barbershop"),
    base_entry("wizardry@1", "Wizardry"),
    base_entry("harvest@1", "Harvest"),
    base_entry("yuletide@1", "Yuletide"),
];

const fn entry(reference: &'static str, name: &'static str) -> CatalogEntry {
    CatalogEntry {
        reference,
        name,
        kind: SkinKind::Snake,
        price_bux: 0,
    }
}

const fn base_entry(reference: &'static str, name: &'static str) -> CatalogEntry {
    CatalogEntry {
        reference,
        name,
        kind: SkinKind::Base,
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
/// Either kind, because both are equippable. See [`BASE_REF_PREFIX`] for what
/// the two are and why they share one prefix.
pub fn is_known_base(base_ref: &str) -> bool {
    let Some(inner) = base_ref.strip_prefix(BASE_REF_PREFIX) else {
        return false;
    };
    is_known_base_skin(inner) || is_known(inner)
}

/// Whether a bare id names a base skin — a picture, as opposed to a snake
/// skin's colour theme.
pub fn is_known_base_skin(base_skin_id: &str) -> bool {
    BASE_SKINS
        .iter()
        .any(|entry| entry.reference == base_skin_id)
}

/// What a player's stored base choice contributes to their **team's** endzone.
///
/// Only a base skin travels. A `base:<snake ref>` theme resolves to `None`
/// here on purpose: it is viewer-local dressing, and promoting it to the team
/// would change what a player's existing choice means — silently showing
/// opponents a look they picked for their own screen. So an account still
/// holding one keeps seeing it exactly as before and contributes nothing to
/// the team's base.
///
/// Never fails. Anything unrecognised — an id from a newer build, a
/// hand-edited value, an unprefixed one — is simply not a base skin.
pub fn team_base_skin_ref(stored: Option<&str>) -> Option<&'static str> {
    let trimmed = stored?.trim();
    if trimmed.len() > MAX_SKIN_REF_LENGTH {
        return None;
    }
    let inner = trimmed.strip_prefix(BASE_REF_PREFIX)?;
    BASE_SKINS
        .iter()
        .find(|entry| entry.reference == inner)
        .map(|entry| entry.reference)
}

/// What the base picker offers.
///
/// Base skins only. The nineteen per-snake colour themes are still *accepted*
/// — an account that equipped one before base skins existed keeps it, and
/// keeps seeing it — but they are no longer *offered*, because listing them
/// beside base skins would put thirty-six rows under one heading meaning two
/// different things: one lot dresses your team's endzone for everybody, the
/// other tints your own screen and nobody else's.
pub fn base_catalog() -> Vec<CatalogEntry> {
    BASE_SKINS.to_vec()
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

    /// The base slot is always prefixed, whichever kind of thing it names.
    #[test]
    fn base_references_are_prefixed_catalogue_entries() {
        assert!(is_known_base("base:aurora@1"));
        assert!(is_known_base("base:classic@1"));
        assert!(is_known_base("base:invaders@1"));
        assert!(!is_known_base("aurora@1"), "a bare snake ref is not a base");
        assert!(
            !is_known_base("invaders@1"),
            "a bare base skin id is not a base"
        );
        assert!(!is_known_base("base:nonesuch@9"));
        assert!(!is_known_base("base:"));
        assert_eq!(
            base_catalog().len(),
            BASE_SKINS.len(),
            "the picker offers base skins and nothing else"
        );
        assert!(
            base_catalog()
                .iter()
                .all(|entry| entry.kind == SkinKind::Base)
        );
    }

    /// One prefix, two namespaces. An overlap would make a stored value
    /// ambiguous — the same string meaning a travelling picture to one code
    /// path and a viewer-local theme to another.
    #[test]
    fn the_two_base_namespaces_do_not_overlap() {
        for base in BASE_SKINS {
            assert!(
                !is_known(base.reference),
                "{} is both a base skin and a snake skin",
                base.reference
            );
            assert!(is_known_base_skin(base.reference));
        }
        for snake in CATALOG {
            assert!(!is_known_base_skin(snake.reference));
        }
    }

    /// Only a base skin reaches the team. A theme is dressing for the screen
    /// of whoever chose it, and always was.
    #[test]
    fn only_a_base_skin_travels_to_the_team() {
        assert_eq!(
            team_base_skin_ref(Some("base:invaders@1")),
            Some("invaders@1")
        );
        assert_eq!(
            team_base_skin_ref(Some("  base:dragon@1  ")),
            Some("dragon@1"),
            "stored whitespace should not cost a player their base"
        );

        for viewer_local_or_absurd in [
            None,
            Some(""),
            Some("base:"),
            Some("base:aurora@1"),
            Some("base:classic@1"),
            Some("invaders@1"),
            Some("base:INVADERS@1"),
            Some("base:from-the-future@9"),
            Some("../../etc/passwd"),
        ] {
            assert_eq!(
                team_base_skin_ref(viewer_local_or_absurd),
                None,
                "{viewer_local_or_absurd:?} must not become a team's base"
            );
        }

        let long = format!("base:{}", "a".repeat(MAX_SKIN_REF_LENGTH));
        assert_eq!(team_base_skin_ref(Some(&long)), None);
    }

    #[test]
    fn every_base_skin_has_a_display_name_and_is_free() {
        let mut seen = std::collections::HashSet::new();
        for entry in BASE_SKINS {
            assert!(!entry.name.is_empty(), "{} has no name", entry.reference);
            assert_eq!(entry.kind, SkinKind::Base);
            assert_eq!(entry.price_bux, 0, "built-ins are free");
            assert!(
                seen.insert(entry.reference),
                "{} appears twice",
                entry.reference
            );
        }
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
