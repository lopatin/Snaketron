//! Player-authored skins as durable entities.
//!
//! A skin is a stable numeric id with an append-only chain of immutable
//! revisions. Ownership, browsing, and equipping all name the *skin*; match
//! snapshots, spectators, and replays all name a *revision*, by the hash of its
//! bytes. That split is what makes the guarantees hold: a creator can keep
//! editing, and a game that has already been played keeps rendering exactly
//! what its players saw.
//!
//! Publication and review are deliberately separate dimensions rather than one
//! status enum. A published skin whose newest edit is awaiting review is an
//! ordinary state, and collapsing the two would make rejecting that edit
//! indistinguishable from withdrawing the skin — silently unpublishing
//! something for everyone who already had it.

use serde::{Deserialize, Serialize};

/// What a skin dresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum SkinKind {
    Snake,
    Base,
}

impl SkinKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Snake => "snake",
            Self::Base => "base",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "snake" => Some(Self::Snake),
            "base" => Some(Self::Base),
            _ => None,
        }
    }
}

/// Which lifecycle a stored skin belongs to.
///
/// Evaluation entries exist so the real renderer can exercise optimizer and
/// technique candidates through the same immutable-revision path as a
/// production skin. They are nevertheless a separate namespace: no review or
/// administrator action may turn one into a public catalogue skin.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum SkinNamespace {
    #[default]
    Production,
    Evaluation,
}

impl SkinNamespace {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Evaluation => "evaluation",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "production" => Some(Self::Production),
            "evaluation" => Some(Self::Evaluation),
            _ => None,
        }
    }

    pub fn is_publishable(self) -> bool {
        matches!(self, Self::Production)
    }
}

/// What the world can see of a skin.
///
/// Orthogonal to review: `pending_revision` on the skin says what an admin has
/// been asked to look at, and a decision about it never moves this field except
/// on the first approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum Publication {
    /// Never published. Visible to its creator and to admins, nobody else.
    Private,
    /// Browsable, purchasable, and equippable by anyone holding a grant.
    Published,
    /// Withdrawn from browse and purchase. Everyone who already holds a grant
    /// keeps equipping it — taking a skin back off someone who has it is not a
    /// thing this system does.
    Unpublished,
    /// The moderation kill switch. Resolves to classic everywhere, including
    /// for its creator and including in replays. Grants survive so that
    /// re-enabling restores everyone at once.
    Disabled,
}

impl Publication {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Published => "published",
            Self::Unpublished => "unpublished",
            Self::Disabled => "disabled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "private" => Some(Self::Private),
            "published" => Some(Self::Published),
            "unpublished" => Some(Self::Unpublished),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    /// Whether a revision of this skin may still be resolved for rendering.
    ///
    /// Everything except `disabled` may: an unpublished skin is still worn by
    /// whoever had it, and a private draft is still worn by its creator.
    pub fn is_renderable(self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Whether this skin belongs in the public browse listing.
    pub fn is_browsable(self) -> bool {
        matches!(self, Self::Published)
    }
}

/// A skin, without its documents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Skin {
    pub skin_id: i32,
    pub kind: SkinKind,
    /// Production catalogue entry or isolated, permanently non-publishable
    /// factory evaluation fixture.
    #[serde(default)]
    pub namespace: SkinNamespace,
    /// Who made it. Distinct from who owns it: ownership is a grant many
    /// players can hold, and only the creator (or an admin) may edit.
    pub creator_user_id: i32,
    pub creator_username: Option<String>,
    pub name: String,
    pub publication: Publication,
    /// The revision an admin has been asked to review, if any.
    pub pending_revision: Option<u32>,
    pub price_bux: u32,
    pub head_revision: u32,
    pub published_revision: Option<u32>,
    /// Denormalised so match preparation resolves a wearer in one read instead
    /// of a read for the skin plus a read for its revision.
    pub head_content_ref: String,
    pub published_content_ref: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub published_at_ms: Option<i64>,
    /// How many players hold a grant on this skin.
    ///
    /// A counter on the skin rather than a count of grant rows: grants are
    /// keyed by their owner, so counting them per skin would be a scan. It is
    /// incremented in the same transaction that writes the grant, which is what
    /// keeps it from drifting away from the rows it counts.
    pub owner_count: u32,
    /// How many players currently have this skin equipped. Adjusted as
    /// equipment changes, so it lags a change by nothing and is never a scan.
    pub wearer_count: u32,
}

impl Skin {
    /// The revision this viewer should render, as a content reference.
    ///
    /// A creator wears their own work in progress — that is what "you can equip
    /// your unpublished skins" means. Everyone else wears the last revision an
    /// admin approved, so an edit in flight cannot change what other players
    /// see. A disabled skin resolves to nothing and the caller falls back to
    /// classic.
    pub fn content_ref_for(&self, viewer_user_id: Option<i32>) -> Option<&str> {
        if !self.publication.is_renderable() {
            return None;
        }
        if viewer_user_id == Some(self.creator_user_id) {
            return Some(&self.head_content_ref);
        }
        if !self.namespace.is_publishable() {
            return None;
        }
        self.published_content_ref.as_deref()
    }

    /// Whether this user may edit the skin. Admins may edit anything, which is
    /// the moderation path for fixing rather than only removing.
    pub fn may_edit(&self, user_id: i32, is_admin: bool) -> bool {
        is_admin || user_id == self.creator_user_id
    }

    /// Whether this user may see the skin at all.
    ///
    /// Anything else must be answered with the same 404 a nonexistent skin
    /// gets: ids are sequential, so a distinguishable "exists but forbidden"
    /// would let anyone enumerate how many private drafts exist and when.
    pub fn may_view(&self, user_id: Option<i32>, is_admin: bool) -> bool {
        if is_admin {
            return true;
        }
        if !self.namespace.is_publishable() {
            return user_id == Some(self.creator_user_id);
        }
        if self.publication.is_browsable() {
            return true;
        }
        match self.publication {
            // Unpublished stays visible to anyone holding a grant; the caller
            // supplies that fact, so this only answers the creator case.
            Publication::Unpublished | Publication::Private | Publication::Disabled => {
                user_id == Some(self.creator_user_id)
            }
            Publication::Published => true,
        }
    }
}

/// Whether a validated v2 layer tree can paint author-controlled words.
pub fn layers_contain_authored_text(layers: &[skin_schema::v2::LayerV2]) -> bool {
    layers.iter().any(|layer| match &layer.body {
        skin_schema::v2::LayerBodyV2::Group { layers } => layers_contain_authored_text(layers),
        skin_schema::v2::LayerBodyV2::Span {
            source: skin_schema::v2::SourceV2::Text { .. },
            ..
        } => true,
        _ => false,
    })
}

/// Recover the text gate for rows written before `containsText` was persisted.
/// Malformed legacy bytes fail closed: they need exact human approval before
/// any by-reference or matchmaking path may expose them.
pub fn document_contains_authored_text(document: &str) -> bool {
    match skin_schema::v2::load_any(document) {
        Ok(skin_schema::v2::AnySkinDoc::V1(_)) => false,
        Ok(skin_schema::v2::AnySkinDoc::V2(doc)) => layers_contain_authored_text(&doc.layers),
        Err(_) => true,
    }
}

/// One immutable revision of a skin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkinRevision {
    pub skin_id: i32,
    pub revision: u32,
    /// `sha256:<hex>` of the canonical document bytes.
    pub content_ref: String,
    /// The document, as canonical JSON.
    pub document: String,
    /// Content references of every texture the document names.
    pub texture_refs: Vec<String>,
    /// Which schema version the validator that accepted this passed it under.
    /// Stored so match preparation can check cheaply rather than re-validating
    /// in the hot path.
    pub validated_schema: u32,
    /// When this revision first entered a match snapshot, if it ever did.
    ///
    /// The moment a revision is worn in a match its reference is public by
    /// necessity — opponents have to fetch it to render it — so exposure is
    /// recorded rather than inferred, and never un-recorded.
    pub exposed_at_ms: Option<i64>,
    /// Set when an admin approves this revision. Gates whether text layers
    /// render for anyone but the creator.
    pub review_approved: bool,
    /// Whether this document contains an authored text source.
    ///
    /// Text is the one source whose pixels can communicate arbitrary words.
    /// Keeping the bit beside the revision lets match preparation fail closed
    /// without reparsing JSON on every game start: an unreviewed text revision
    /// may be previewed privately by its author, but may not enter a match or
    /// be fetched by an opponent.
    #[serde(default)]
    pub contains_text: bool,
    pub created_at_ms: i64,
}

/// How a player came to own a skin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GrantSource {
    Purchase,
    OwnCreation,
    /// Free acquisition, or an admin grant.
    Grant,
}

impl GrantSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Purchase => "purchase",
            Self::OwnCreation => "ownCreation",
            Self::Grant => "grant",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "purchase" => Some(Self::Purchase),
            "ownCreation" => Some(Self::OwnCreation),
            "grant" => Some(Self::Grant),
            _ => None,
        }
    }
}

/// A player's permanent claim on a skin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SkinGrant {
    pub skin_id: i32,
    pub acquired_at_ms: i64,
    pub price_paid_bux: u32,
    pub source: GrantSource,
}

/// Everything needed to create a skin.
///
/// Grouped into a struct rather than passed as eight positional arguments
/// because half of them are strings and a transposed pair would store a
/// document under a name and a name as a document.
#[derive(Debug, Clone)]
pub struct NewSkin<'a> {
    pub creator_user_id: i32,
    pub creator_username: Option<&'a str>,
    pub kind: SkinKind,
    pub namespace: SkinNamespace,
    pub name: &'a str,
    pub revision: NewRevision<'a>,
    /// Stable per-creator request identity. Factory callers always provide it;
    /// interactive legacy callers may omit it until their client is upgraded.
    pub idempotency_key: Option<&'a str>,
    /// Hash of the create payload, used to refuse reuse of a key for different
    /// content rather than silently returning the first skin.
    pub request_hash: Option<&'a str>,
}

/// Everything needed to append a revision.
#[derive(Debug, Clone)]
pub struct NewRevision<'a> {
    /// Canonical document bytes, as produced by `skin_schema::content`.
    pub document: &'a str,
    pub content_ref: &'a str,
    pub texture_refs: &'a [String],
    pub validated_schema: u32,
    pub contains_text: bool,
}

/// A storage conflict with a safe, caller-actionable meaning.
///
/// Database methods still return `anyhow::Error` because that is the service's
/// shared persistence interface. These values are downcast by the HTTP layer
/// so an optimistic-write loss is a 409 rather than a misleading 500.
#[derive(Debug, thiserror::Error)]
pub enum SkinWriteError {
    #[error("skin head changed: expected revision {expected}, current revision is {actual}")]
    HeadChanged { expected: u32, actual: u32 },
    #[error("the reviewed revision or content hash no longer matches the request")]
    ReviewTargetChanged,
    #[error("the skin-create idempotency key was already used for different content")]
    IdempotencyKeyReused,
    #[error("evaluation-only skins cannot enter the production catalogue")]
    EvaluationOnly,
}

/// A page of skins, and where to resume.
#[derive(Debug, Clone, Default)]
pub struct SkinPage {
    pub skins: Vec<Skin>,
    pub cursor: Option<String>,
}

/// What a player is wearing, in the stored representation.
///
/// Built-ins keep their catalogue id (`aurora@1`); first-class skins are named
/// by `skin:<id>` so the two can share one string field without ambiguity, and
/// so match preparation can tell which lookup it needs from the prefix alone.
pub const SKIN_ID_PREFIX: &str = "skin:";

/// Parse an equipped value into a skin id, if it names one.
pub fn equipped_skin_id(value: &str) -> Option<i32> {
    value.strip_prefix(SKIN_ID_PREFIX)?.parse().ok()
}

/// The stored form of a first-class skin reference.
pub fn skin_id_reference(skin_id: i32) -> String {
    format!("{SKIN_ID_PREFIX}{skin_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skin(publication: Publication, published: Option<u32>) -> Skin {
        Skin {
            skin_id: 7,
            kind: SkinKind::Snake,
            namespace: SkinNamespace::Production,
            creator_user_id: 42,
            creator_username: Some("author".to_string()),
            name: "Test".to_string(),
            publication,
            pending_revision: None,
            price_bux: 0,
            head_revision: 3,
            published_revision: published,
            head_content_ref: "sha256:head".to_string(),
            published_content_ref: published.map(|_| "sha256:published".to_string()),
            created_at_ms: 0,
            updated_at_ms: 0,
            published_at_ms: None,
            owner_count: 0,
            wearer_count: 0,
        }
    }

    /// The creator sees their work in progress; everyone else sees the last
    /// approved revision. This is the whole reason a skin has two content
    /// references rather than one.
    #[test]
    fn a_creator_wears_their_draft_and_everyone_else_wears_the_approved_revision() {
        let published = skin(Publication::Published, Some(2));
        assert_eq!(published.content_ref_for(Some(42)), Some("sha256:head"));
        assert_eq!(published.content_ref_for(Some(9)), Some("sha256:published"));
        assert_eq!(published.content_ref_for(None), Some("sha256:published"));
    }

    /// An unpublished skin keeps rendering for the people who hold it. Taking
    /// it off them is what `disabled` is for, and only an admin can do that.
    #[test]
    fn unpublishing_does_not_stop_anyone_already_wearing_it() {
        let withdrawn = skin(Publication::Unpublished, Some(2));
        assert_eq!(withdrawn.content_ref_for(Some(9)), Some("sha256:published"));
        assert!(!withdrawn.publication.is_browsable());
        assert!(withdrawn.publication.is_renderable());
    }

    /// Disable reaches everyone, the creator included. "Yours forever" cannot
    /// mean "abusive content renders forever".
    #[test]
    fn disabling_stops_rendering_for_everyone_including_the_creator() {
        let killed = skin(Publication::Disabled, Some(2));
        assert_eq!(killed.content_ref_for(Some(42)), None);
        assert_eq!(killed.content_ref_for(Some(9)), None);
        assert!(!killed.publication.is_renderable());
    }

    /// A private draft that has never been published has nothing for anyone
    /// else to wear, but its creator still wears it.
    #[test]
    fn a_private_draft_resolves_only_for_its_creator() {
        let draft = skin(Publication::Private, None);
        assert_eq!(draft.content_ref_for(Some(42)), Some("sha256:head"));
        assert_eq!(draft.content_ref_for(Some(9)), None);
    }

    #[test]
    fn editing_is_the_creators_right_and_an_admins_power() {
        let draft = skin(Publication::Private, None);
        assert!(draft.may_edit(42, false), "the creator may edit");
        assert!(!draft.may_edit(9, false), "a stranger may not");
        assert!(draft.may_edit(9, true), "an admin may edit anything");
    }

    /// Sequential ids mean a distinguishable refusal is an enumeration oracle,
    /// so anything the caller cannot see must be indistinguishable from absent.
    #[test]
    fn a_private_skin_is_invisible_to_strangers() {
        let draft = skin(Publication::Private, None);
        assert!(draft.may_view(Some(42), false));
        assert!(
            draft.may_view(Some(9), true),
            "admins review what is private"
        );
        assert!(!draft.may_view(Some(9), false));
        assert!(!draft.may_view(None, false));

        let published = skin(Publication::Published, Some(1));
        assert!(published.may_view(None, false), "browsing needs no account");
    }

    #[test]
    fn equipped_values_distinguish_built_ins_from_first_class_skins() {
        assert_eq!(equipped_skin_id("skin:12"), Some(12));
        assert_eq!(equipped_skin_id(&skin_id_reference(99)), Some(99));
        assert_eq!(equipped_skin_id("aurora@1"), None);
        assert_eq!(equipped_skin_id("skin:"), None);
        assert_eq!(equipped_skin_id("skin:not-a-number"), None);
        assert_eq!(
            equipped_skin_id("skin:-4"),
            Some(-4),
            "parsing is not validation"
        );
    }

    #[test]
    fn publication_and_kind_round_trip_through_their_stored_strings() {
        for publication in [
            Publication::Private,
            Publication::Published,
            Publication::Unpublished,
            Publication::Disabled,
        ] {
            assert_eq!(Publication::parse(publication.as_str()), Some(publication));
        }
        assert_eq!(Publication::parse("nonsense"), None);

        for kind in [SkinKind::Snake, SkinKind::Base] {
            assert_eq!(SkinKind::parse(kind.as_str()), Some(kind));
        }
        for namespace in [SkinNamespace::Production, SkinNamespace::Evaluation] {
            assert_eq!(SkinNamespace::parse(namespace.as_str()), Some(namespace));
        }
        for source in [
            GrantSource::Purchase,
            GrantSource::OwnCreation,
            GrantSource::Grant,
        ] {
            assert_eq!(GrantSource::parse(source.as_str()), Some(source));
        }
    }
}
