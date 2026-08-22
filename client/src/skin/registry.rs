//! Which skin a snake is wearing.
//!
//! Resolution never fails. An id the client does not recognise — a skin from a
//! newer build, a corrupted preference, a hand-edited request — falls back to
//! the classic look and logs once. Cosmetics must not be able to break a frame.

use crate::skin::animal::AnimalSkin;
use crate::skin::checker::CheckerSkin;
use crate::skin::doc::ParamSkin;
use crate::skin::docv2::LayerSkin;
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
                if let Some(draft) = resolve_draft(id) {
                    return draft;
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
type SkinTable = std::sync::Mutex<Vec<(String, &'static dyn SnakeSkin)>>;

fn authored_skins() -> &'static SkinTable {
    static AUTHORED: OnceLock<SkinTable> = OnceLock::new();
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
        .map(|(_, skin)| *skin)
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

    registered.push((content_ref.to_string(), compile_document(document_json)?));
    Ok(())
}

/// Compile a document of either schema version.
///
/// The version is read from the document rather than from the caller, so a
/// client that understands both never has to be told which it is looking at,
/// and one that understands neither falls back to classic — the behaviour a
/// document from a newer build has always had.
///
/// The compiled skin is leaked for the reason described on
/// [`register_authored_skin`]: the render loop interns what it resolves, so a
/// skin that could be freed while a frame still points at it would be a
/// use-after-free rather than a saving.
fn compile_document(json: &str) -> Result<&'static dyn SnakeSkin, String> {
    Ok(Box::leak(compile_document_owned(json)?))
}

/// The one schema-dispatch boundary shared by runtime registration and the
/// native pre-register validator.
fn compile_document_owned(json: &str) -> Result<Box<dyn SnakeSkin>, String> {
    match skin_schema::v2::load_any(json).map_err(readable)? {
        skin_schema::v2::AnySkinDoc::V1(doc) => ParamSkin::compile(&doc)
            .map(|skin| Box::new(skin) as Box<dyn SnakeSkin>)
            .map_err(readable),
        skin_schema::v2::AnySkinDoc::V2(doc) => LayerSkin::compile(&doc)
            .map(|skin| Box::new(skin) as Box<dyn SnakeSkin>)
            .map_err(readable),
    }
}

/// Compile without registering or leaking the result.
///
/// This is the side-effect-free half of runtime registration, exposed to the
/// native pre-register gate through the crate root. It deliberately reaches
/// the same `ParamSkin` / `LayerSkin` compilers as [`compile_document`].
pub(crate) fn validate_document_for_renderer(json: &str) -> Result<(), String> {
    compile_document_owned(json).map(drop)
}

/// Turn validator complaints into something a person can act on.
///
/// The debug formatting of a `Vec<SkinDocError>` is fine in a test failure and
/// useless in an editor: an author who has just moved a colour picker should be
/// told which colour and why, not handed `SkinDocError { field: ... }`.
fn readable(errors: Vec<skin_schema::SkinDocError>) -> String {
    errors
        .iter()
        .map(|error| format!("{} — {}", pretty_field(&error.field), error.problem))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A field path as the editor labels it, so the message names the control the
/// author is looking at rather than the path the schema uses.
fn pretty_field(field: &str) -> String {
    let labelled = field
        .replace("palette.friendly[0]", "Friendly (light)")
        .replace("palette.friendly[1]", "Friendly (dark)")
        .replace("palette.enemy[0]", "Enemy (light)")
        .replace("palette.enemy[1]", "Enemy (dark)")
        .replace("palette.free_for_all[", "Free-for-all slot [")
        .replace("head.core_ratio", "Head core size")
        .replace("head.core_color", "Head core colour")
        .replace("head.gradient", "Head glow")
        .replace("outline.extra_px", "Outline width")
        .replace("animation.period_ms", "Animation cycle length")
        .replace('_', " ");
    // `Friendly (light).fill` is a path with a label glued on; the reader wants
    // a phrase.
    let labelled = labelled.replace('.', " ");
    if labelled == field {
        field.to_string()
    } else {
        labelled
    }
}

/// A document being edited, compiled under a scratch handle.
///
/// Unlike an authored skin, a draft has no content reference: it has not been
/// stored, so it has no name derived from its bytes. It is registered under a
/// caller-chosen handle instead, and re-registering replaces it — which is what
/// makes a live preview show the edit rather than the last thing saved.
///
/// Handles are namespaced so they can never be mistaken for, or collide with, a
/// catalogue id or a content reference.
pub const DRAFT_HANDLE_PREFIX: &str = "draft:";

fn draft_skins() -> &'static SkinTable {
    static DRAFTS: OnceLock<SkinTable> = OnceLock::new();
    DRAFTS.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

fn resolve_draft(id: &str) -> Option<&'static dyn SnakeSkin> {
    if !id.starts_with(DRAFT_HANDLE_PREFIX) {
        return None;
    }
    let registered = draft_skins().lock().ok()?;
    registered
        .iter()
        .find(|(handle, _)| handle == id)
        .map(|(_, skin)| *skin)
}

/// Compile a draft under `handle`, replacing any previous compilation of it.
///
/// Bounded by the same ceiling as authored skins for the same reason: compiled
/// skins are leaked, so an editor session that recompiled without limit would
/// leak without limit. Replacing in place means one editing session costs one
/// slot no matter how many edits it makes.
pub fn register_draft_skin(handle: &str, document_json: &str) -> Result<(), String> {
    if !handle.starts_with(DRAFT_HANDLE_PREFIX) {
        return Err(format!(
            "a draft handle must start with {DRAFT_HANDLE_PREFIX}"
        ));
    }

    // The same door authored skins come through, so the editor previews a
    // draft by exactly the path that will render it once it is saved. A
    // version-specific path here would mean the Builder could show something
    // the game then could not.
    let compiled = compile_document(document_json)?;

    let mut registered = draft_skins()
        .lock()
        .map_err(|_| "the draft registry is poisoned".to_string())?;
    match registered
        .iter_mut()
        .find(|(existing, _)| existing == handle)
    {
        Some(slot) => slot.1 = compiled,
        None => {
            if registered.len() >= MAX_AUTHORED_SKINS {
                return Err("too many drafts in this session".to_string());
            }
            registered.push((handle.to_string(), compiled));
        }
    }
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

    /// An author who has just moved a colour picker is told which colour and
    /// why, not handed the debug formatting of an error vector.
    #[test]
    fn validator_complaints_are_written_for_the_person_who_caused_them() {
        let message = readable(vec![
            skin_schema::SkinDocError {
                field: "palette.friendly[0].fill".to_string(),
                problem: "a teammate must read blue-family".to_string(),
            },
            skin_schema::SkinDocError {
                field: "outline.extra_px".to_string(),
                problem: "must be between 0 and 4".to_string(),
            },
        ]);

        assert!(message.contains("Friendly (light) fill"), "got: {message}");
        assert!(message.contains("Outline width"), "got: {message}");
        assert!(
            !message.contains("SkinDocError") && !message.contains('{'),
            "no debug formatting may reach an author: {message}"
        );
        assert_eq!(message.lines().count(), 2, "one line per complaint");
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

    /// A v2 layer document travels the same road as a v1 one: same content
    /// reference, same registration, same resolution. Nothing outside this
    /// function needs to know which schema a skin was written in, which is
    /// what lets both versions live indefinitely.
    #[test]
    fn a_v2_layer_document_registers_and_resolves_like_any_other() {
        let v1: skin_schema::SkinDoc =
            serde_json::from_str(include_str!("../../../skin-schema/skins/classic.skin.json"))
                .expect("the shipped classic document parses");
        let mut v2 = skin_schema::v2::upgrade(&v1);
        v2.id = "layered@1".to_string();
        // A new id does not inherit the shipped document's recorded label
        // exemption, so the steel free-for-all slot has to earn its contrast
        // like any other authored skin's would.
        v2.palette.free_for_all[2].fill = "#93a3b5".to_string();
        v2.palette.free_for_all[2].outline = "#5d6e81".to_string();

        let canonical = String::from_utf8(
            skin_schema::content::canonical_bytes(&serde_json::to_value(&v2).expect("serializes"))
                .expect("canonicalises"),
        )
        .expect("canonical bytes are utf-8");
        let reference = skin_schema::content::reference_for_bytes(canonical.as_bytes());

        assert!(register_authored_skin(&reference, &canonical).is_ok());
        let registry = SkinRegistry::new();
        let resolved = registry.resolve(Some(&reference));
        assert_eq!(resolved.id(), "layered@1");
        // ...and it answers the questions every other skin answers, so the
        // roster and the arena need no v2-specific path.
        assert!(resolved.metrics(false).overhang_px > 0.0);
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
