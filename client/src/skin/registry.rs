//! Which skin a snake is wearing.
//!
//! Resolution never fails. An id the client does not recognise — a skin from a
//! newer build, a corrupted preference, a hand-edited request — falls back to
//! the classic look and logs once. Cosmetics must not be able to break a frame.

use crate::skin::animal::AnimalSkin;
use crate::skin::checker::CheckerSkin;
use crate::skin::doc::ParamSkin;
use crate::skin::ember::EmberSkin;
use crate::skin::sprite::SpriteSkin;
use crate::skin::{ClassicSkin, SnakeSkin};
use std::sync::OnceLock;

/// The document skins compiled into the bundle.
///
/// Compiling is deferred to first use and then cached: a document turns into
/// a ring of resolved palettes exactly once per process, never per frame.
/// A document that somehow fails to compile is dropped from the catalogue with
/// a log rather than taken as a reason to fail startup — a bad cosmetic should
/// cost you a skin, not the game.
fn document_skins() -> &'static [ParamSkin] {
    static SKINS: OnceLock<Vec<ParamSkin>> = OnceLock::new();
    SKINS.get_or_init(|| {
        [
            include_str!("../../../skin-schema/skins/aurora.skin.json"),
            include_str!("../../../skin-schema/skins/tidewave.skin.json"),
            include_str!("../../../skin-schema/skins/voltage.skin.json"),
            include_str!("../../../skin-schema/skins/lantern.skin.json"),
        ]
        .into_iter()
        .filter_map(|json| match ParamSkin::from_json(json) {
            Ok(skin) => Some(skin),
            Err(errors) => {
                web_sys::console::warn_1(&wasm_bindgen::JsValue::from_str(&format!(
                    "skipping an invalid built-in skin document: {errors:?}"
                )));
                None
            }
        })
        .collect()
    })
}

/// The built-in catalogue.
pub struct SkinRegistry {
    classic: ClassicSkin,
    ember: EmberSkin,
    /// The checkerboard family. One implementation, three boards — kept as an
    /// array rather than three fields because that is what it is, and because
    /// adding a fourth board should not mean touching this struct.
    checkers: [CheckerSkin; 3],
    /// The animal family: one textured implementation, three coats.
    animals: [AnimalSkin; 6],
    /// The sprite-sheet family: art whose rows are frames of animation.
    sheets: [SpriteSkin; 4],
}

impl Default for SkinRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SkinRegistry {
    pub const fn new() -> Self {
        Self {
            classic: ClassicSkin,
            ember: EmberSkin,
            checkers: crate::skin::checker::FAMILY,
            animals: crate::skin::animal::FAMILY,
            sheets: crate::skin::sprite::FAMILY,
        }
    }

    /// Every selectable skin, in catalogue order.
    pub fn entries(&self) -> Vec<&dyn SnakeSkin> {
        let mut entries: Vec<&dyn SnakeSkin> = vec![&self.classic, &self.ember];
        entries.extend(document_skins().iter().map(|skin| skin as &dyn SnakeSkin));
        entries.extend(self.checkers.iter().map(|skin| skin as &dyn SnakeSkin));
        entries.extend(self.animals.iter().map(|skin| skin as &dyn SnakeSkin));
        entries.extend(self.sheets.iter().map(|skin| skin as &dyn SnakeSkin));
        entries
    }

    /// The skin an id names, or classic when it names nothing.
    pub fn resolve(&self, id: Option<&str>) -> &dyn SnakeSkin {
        match id {
            None => &self.classic,
            Some(id) => self
                .entries()
                .into_iter()
                .find(|skin| skin.id() == id)
                .unwrap_or(&self.classic),
        }
    }

    /// Whether an id names a real skin.
    #[cfg(test)]
    pub fn is_known(&self, id: &str) -> bool {
        self.entries().iter().any(|skin| skin.id() == id)
    }
}

/// The process-wide catalogue.
pub fn skin_registry() -> &'static SkinRegistry {
    static REGISTRY: SkinRegistry = SkinRegistry::new();
    &REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalogue the client can draw and the one the server will accept
    /// have to be the same list, or a player picks something that silently
    /// turns back into classic the moment it reaches a match.
    #[test]
    fn the_client_catalogue_matches_the_servers() {
        let registry = SkinRegistry::new();
        let client: Vec<&str> = registry.entries().iter().map(|skin| skin.id()).collect();
        // Mirrors `server/src/skin_catalog.rs`, which cannot be imported here.
        let server = [
            "classic@1",
            "aurora@1",
            "ember@1",
            "tidewave@1",
            "voltage@1",
            "lantern@1",
            "gambit@1",
            "harlequin@1",
            "pitlane@1",
            "zebra@1",
            "zebra-print@1",
            "tiger@1",
            "tiger-print@1",
            "jaguar@1",
            "jaguar-print@1",
            "zebra-live@1",
            "tiger-live@1",
            "stars-and-stripes@1",
            "race-livery@1",
        ];
        let mut sorted_client = client.clone();
        sorted_client.sort_unstable();
        let mut sorted_server = server.to_vec();
        sorted_server.sort_unstable();
        assert_eq!(
            sorted_client, sorted_server,
            "the client renders {client:?} but the server allows {server:?}"
        );
    }

    #[test]
    fn every_registered_skin_has_a_distinct_id() {
        let mut seen = std::collections::HashSet::new();
        for skin in SkinRegistry::new().entries() {
            assert!(
                seen.insert(skin.id().to_string()),
                "duplicate {}",
                skin.id()
            );
        }
    }

    #[test]
    fn unknown_ids_resolve_to_classic_rather_than_failing() {
        let registry = SkinRegistry::new();
        assert_eq!(registry.resolve(None).id(), "classic@1");
        assert_eq!(registry.resolve(Some("classic@1")).id(), "classic@1");
        assert_eq!(registry.resolve(Some("nonesuch@9")).id(), "classic@1");
        assert_eq!(registry.resolve(Some("")).id(), "classic@1");
        assert!(!registry.is_known("nonesuch@9"));
    }
}
