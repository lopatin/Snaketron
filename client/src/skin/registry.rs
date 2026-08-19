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
    ///
    /// Content-addressed ids are looked up first and separately: they are the
    /// common case in a match with player-authored skins, and they can never
    /// collide with a catalogue id because the two are different shapes.
    pub fn resolve(&self, id: Option<&str>) -> &dyn SnakeSkin {
        match id {
            None => &self.classic,
            Some(id) => {
                if let Some(authored) = resolve_authored(id) {
                    return authored;
                }
                self.entries()
                    .into_iter()
                    .find(|skin| skin.id() == id)
                    .unwrap_or(&self.classic)
            }
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

/// How many player-authored skins one session will hold.
///
/// A match has at most eight players and a browse page shows a screenful, so
/// this is generous. It is a ceiling rather than a cache because compiled skins
/// are leaked (see [`register_authored_skin`]): unbounded registration in a
/// long session would be an unbounded leak.
pub const MAX_AUTHORED_SKINS: usize = 64;

/// Player-authored skins compiled during this session, keyed by content
/// reference.
///
/// A `Mutex` rather than a `RefCell` so the type is `Sync` and can live in a
/// `static`; in wasm there is only ever one thread, so it is never contended.
fn authored_skins() -> &'static std::sync::Mutex<Vec<(String, &'static ParamSkin)>> {
    static AUTHORED: OnceLock<std::sync::Mutex<Vec<(String, &'static ParamSkin)>>> =
        OnceLock::new();
    AUTHORED.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn resolve_authored(id: &str) -> Option<&'static dyn SnakeSkin> {
    if !skin_schema::content::is_content_ref(id) {
        return None;
    }
    let registered = authored_skins().lock().ok()?;
    registered
        .iter()
        .find(|(reference, _)| reference == id)
        .map(|(_, skin)| *skin as &'static dyn SnakeSkin)
}

/// Compile one player-authored document and make it resolvable.
///
/// Registration is idempotent by content reference: the same bytes register
/// once, so a skin worn by four players in one match compiles once.
///
/// The compiled skin is deliberately leaked. `resolve` hands out `&dyn
/// SnakeSkin` borrowed from a `&'static` registry, and the render loop interns
/// what it resolves, so a skin that could be freed while a frame still points
/// at it would be a use-after-free rather than a saving. The bound above is
/// what keeps that honest.
pub fn register_authored_skin(content_ref: &str, document_json: &str) -> Result<(), String> {
    if !skin_schema::content::is_content_ref(content_ref) {
        return Err(format!("{content_ref} is not a content reference"));
    }

    let mut registered = authored_skins()
        .lock()
        .map_err(|_| "the authored-skin registry is poisoned".to_string())?;
    if registered
        .iter()
        .any(|(reference, _)| reference == content_ref)
    {
        return Ok(());
    }
    if registered.len() >= MAX_AUTHORED_SKINS {
        return Err(format!(
            "this session already holds {MAX_AUTHORED_SKINS} authored skins"
        ));
    }

    // The bytes must hash to the reference they arrived under. Without this the
    // reference would be a label rather than a name, and a proxy or a cache
    // could substitute one skin for another without anything noticing.
    let actual = skin_schema::content::reference_for_bytes(document_json.as_bytes());
    if actual != content_ref {
        return Err(format!(
            "document does not match its reference: expected {content_ref}, got {actual}"
        ));
    }

    let skin = ParamSkin::from_json(document_json).map_err(|errors| format!("{errors:?}"))?;
    registered.push((content_ref.to_string(), Box::leak(Box::new(skin))));
    Ok(())
}

/// Whether a content reference has already been compiled this session.
pub fn authored_skin_is_registered(content_ref: &str) -> bool {
    authored_skins()
        .lock()
        .map(|registered| {
            registered
                .iter()
                .any(|(reference, _)| reference == content_ref)
        })
        .unwrap_or(false)
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

    /// A document registers under the hash of its own bytes and nothing else.
    /// Without this the reference would be a label a caller could put on any
    /// document, and a cache or a proxy could swap one skin for another.
    #[test]
    fn an_authored_document_must_hash_to_the_reference_it_arrives_under() {
        let document = include_str!("../../../skin-schema/skins/aurora.skin.json");
        let doc: skin_schema::SkinDoc =
            serde_json::from_str(document).expect("the shipped document parses");
        let canonical = String::from_utf8(
            skin_schema::content::canonical_bytes(&doc).expect("canonical bytes"),
        )
        .expect("canonical bytes are UTF-8");
        let reference = skin_schema::content::reference_for_bytes(canonical.as_bytes());

        // The right bytes under the wrong name are refused.
        let wrong = format!("sha256:{}", "0".repeat(64));
        assert!(register_authored_skin(&wrong, &canonical).is_err());
        assert!(!authored_skin_is_registered(&wrong));

        // A catalogue id is not a content reference and cannot be used as one.
        assert!(register_authored_skin("aurora@1", &canonical).is_err());

        assert!(register_authored_skin(&reference, &canonical).is_ok());
        assert!(authored_skin_is_registered(&reference));

        // Registration is idempotent: the same bytes twice is one skin.
        assert!(register_authored_skin(&reference, &canonical).is_ok());

        // And it is resolvable, which is the whole point.
        let registry = SkinRegistry::new();
        assert_eq!(registry.resolve(Some(&reference)).id(), doc.id);
    }

    /// An unregistered content reference is not an error anywhere — it is the
    /// standing fallback, and the client fetching the document is what turns it
    /// into the real skin a moment later.
    #[test]
    fn an_unregistered_content_reference_renders_as_classic() {
        let registry = SkinRegistry::new();
        let unknown = format!("sha256:{}", "1".repeat(64));
        assert_eq!(registry.resolve(Some(&unknown)).id(), "classic@1");
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
