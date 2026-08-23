//! Browsing the catalogue, and recording what a player is wearing.
//!
//! Until this module existed, a player's skin choice lived only in their own
//! browser: `User.selected_skin` was read at match preparation and written
//! nowhere, so every remote player rendered as classic no matter what its owner
//! had picked. These two routes close that gap — the catalogue is browsable
//! without an account, and equipping writes the choice somewhere the match
//! preparation path can actually find it.
//!
//! Equipping validates against the catalogue rather than trusting the client,
//! for the reason `crate::skin_catalog` exists: a reference travels into other
//! players' renderers, so the server decides what is real before it can travel.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::skin_catalog::{self, BASE_REF_PREFIX, CatalogEntry, MAX_SKIN_REF_LENGTH, SkinKind};
use crate::skin_store::{
    NewRevision, NewSkin, Publication, Skin, SkinNamespace, SkinReviewDecision, SkinRevision,
    SkinWriteError, skin_id_reference,
};

/// How long a browser may reuse the catalogue. Built-ins change only when the
/// server is redeployed, so this is generous without being able to strand a
/// player on a stale list for long.
const CATALOG_CACHE_SECONDS: u32 = 300;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowseQuery {
    /// Which slot to list. Absent means snake skins, the main column.
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BrowseResponse {
    pub skins: Vec<CatalogEntry>,
}

/// What a player is wearing, as the client needs to see it.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Equipment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_skin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_base: Option<String>,
}

/// An equip request.
///
/// Each slot is three-valued and the encoding says so: an absent field leaves
/// that slot alone, an explicit `null` clears it back to the default look, and
/// a string equips. Collapsing absent and null would make "equip a skin" and
/// "clear my base" the same request.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EquipRequest {
    #[serde(default, deserialize_with = "double_option")]
    pub selected_skin: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub selected_base: Option<Option<String>>,
}

fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug)]
pub enum SkinsApiError {
    /// The caller may not see this, or it does not exist. Deliberately one
    /// answer for both: skin ids are sequential, so a distinguishable refusal
    /// would let anyone count the private drafts on the service.
    NotFound,
    /// The skin exists but has been taken down. Distinct from not-found on
    /// purpose — a client holding a cached copy needs to be told to drop it.
    Gone,
    AuthRequired,
    GuestNotAllowed,
    /// The document did not pass the shared validator.
    Invalid(Vec<String>),
    /// An optimistic write or exact review target lost a race.
    Conflict(String),
    /// The reference does not name anything this build can draw. Unlike match
    /// preparation, which quietly falls back to classic rather than refusing a
    /// join, an explicit equip is worth an error: the player is looking at the
    /// result and deserves to know it did not take.
    UnknownSkin(String),
    /// A real skin the caller has not acquired. Distinct from unknown: the
    /// client can act on this one, by offering to get it.
    NotOwned,
    Internal(anyhow::Error),
}

impl IntoResponse for SkinsApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "No such skin".to_string()),
            Self::NotOwned => (
                StatusCode::FORBIDDEN,
                "Get this skin before wearing it".to_string(),
            ),
            Self::Gone => (StatusCode::GONE, "This skin has been removed".to_string()),
            Self::AuthRequired => (
                StatusCode::UNAUTHORIZED,
                "Sign in to see your own skins".to_string(),
            ),
            Self::GuestNotAllowed => (
                StatusCode::FORBIDDEN,
                "Creating a skin needs a registered account".to_string(),
            ),
            Self::Invalid(problems) => (StatusCode::BAD_REQUEST, problems.join("; ")),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::UnknownSkin(reference) => (
                StatusCode::BAD_REQUEST,
                format!("{reference} is not a skin this server knows"),
            ),
            Self::Internal(error) => {
                error!(?error, "skins API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        let mut response = (status, Json(serde_json::json!({ "error": message }))).into_response();
        no_store(&mut response);
        response
    }
}

/// The catalogue, for anyone — browsing needs no account.
pub async fn browse(Query(query): Query<BrowseQuery>) -> Response {
    let skins = match query.kind.as_deref() {
        Some("base") => skin_catalog::base_catalog(),
        _ => skin_catalog::CATALOG.to_vec(),
    };

    let mut response = Json(BrowseResponse { skins }).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&format!("public, max-age={CATALOG_CACHE_SECONDS}"))
            .expect("a formatted cache-control header is always valid"),
    );
    response
}

/// Record what the authenticated player is wearing.
pub async fn set_equipment(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<EquipRequest>,
) -> Result<Response, SkinsApiError> {
    let snake = resolve_slot(
        &state,
        auth_user.user_id,
        request.selected_skin.as_ref(),
        SkinKind::Snake,
    )
    .await?;
    let base = resolve_slot(
        &state,
        auth_user.user_id,
        request.selected_base.as_ref(),
        SkinKind::Base,
    )
    .await?;

    // What they were wearing, read before the write, so the wearer counts can
    // be moved off the old skin and onto the new one.
    let worn_before = state
        .db
        .get_user_by_id(auth_user.user_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .and_then(|user| user.selected_skin);

    state
        .db
        .set_user_equipment(
            auth_user.user_id,
            snake.as_ref().map(|slot| slot.as_deref()),
            base.as_ref().map(|slot| slot.as_deref()),
        )
        .await
        .map_err(SkinsApiError::Internal)?;

    // Only the snake slot is counted, and only when it actually moved. These
    // are display numbers on a skin's page, so a failure here is swallowed:
    // an equip that worked must not report failure because a counter did not.
    if let Some(requested) = snake.as_ref() {
        let before = worn_before
            .as_deref()
            .and_then(crate::skin_store::equipped_skin_id);
        let after = requested
            .as_deref()
            .and_then(crate::skin_store::equipped_skin_id);
        if before != after {
            if let Some(skin_id) = before {
                let _ = state.db.adjust_skin_wearers(skin_id, -1).await;
            }
            if let Some(skin_id) = after {
                let _ = state.db.adjust_skin_wearers(skin_id, 1).await;
            }
        }
    }

    // Report the whole slot set back rather than echoing the request, so a
    // client that only sent one slot still learns the state it is now in.
    let user = state
        .db
        .get_user_by_id(auth_user.user_id)
        .await
        .map_err(SkinsApiError::Internal)?;
    let equipment = user
        .map(|user| Equipment {
            selected_skin: user.selected_skin,
            selected_base: user.selected_base,
        })
        .unwrap_or_default();

    let mut response = Json(equipment).into_response();
    no_store(&mut response);
    Ok(response)
}

/// What one slot of an equip request is asking for.
///
/// Separated from resolving it so the parsing — three-valued, trimmed, length
/// capped, base prefix stripped — can be read and tested without a database.
#[derive(Debug, PartialEq, Eq)]
enum SlotRequest<'a> {
    /// The field was absent: leave the slot as it is.
    Untouched,
    /// The field was null or blank: go back to the default look.
    Cleared,
    /// A reference, with any `base:` prefix already removed.
    Named(&'a str),
}

/// Read one slot of the request without deciding whether it may be worn.
fn read_slot<'a>(
    requested: Option<&'a Option<String>>,
    kind: SkinKind,
) -> Result<SlotRequest<'a>, SkinsApiError> {
    let Some(slot) = requested else {
        return Ok(SlotRequest::Untouched);
    };
    let Some(reference) = slot else {
        return Ok(SlotRequest::Cleared);
    };

    let trimmed = reference.trim();
    if trimmed.is_empty() {
        return Ok(SlotRequest::Cleared);
    }
    if trimmed.len() > MAX_SKIN_REF_LENGTH {
        return Err(SkinsApiError::UnknownSkin(format!(
            "a {}-character reference",
            trimmed.len()
        )));
    }

    // A base is a snake reference wearing a prefix, so the prefix comes off
    // here and goes back on whatever the reference resolves to.
    match kind {
        SkinKind::Snake => Ok(SlotRequest::Named(trimmed)),
        SkinKind::Base => trimmed
            .strip_prefix(BASE_REF_PREFIX)
            .filter(|inner| !inner.is_empty())
            .map(SlotRequest::Named)
            .ok_or_else(|| SkinsApiError::UnknownSkin(trimmed.to_string())),
    }
}

/// Put a resolved inner reference back into the form the slot stores.
fn stored_form(inner: &str, kind: SkinKind) -> String {
    match kind {
        SkinKind::Snake => inner.to_string(),
        SkinKind::Base => format!("{BASE_REF_PREFIX}{inner}"),
    }
}

/// Resolve a built-in against the compiled catalogue.
///
/// Returns the catalogue's own string rather than the caller's bytes, so what
/// lands in the database is always a reference this build compiled and never
/// merely one that compared equal to it.
fn catalogue_reference(inner: &str, kind: SkinKind) -> Option<String> {
    skin_catalog::CATALOG
        .iter()
        .find(|entry| entry.reference == inner)
        .map(|entry| stored_form(entry.reference, kind))
}

/// Whether this viewer may wear this stored skin in this slot.
///
/// The wearability question is `Skin::content_ref_for` — the same one match
/// preparation asks when it turns a stored reference back into something to
/// draw — so a skin can never be equippable here and unrenderable there. It
/// carries the three rules that matter: a creator wears their own private
/// draft, everyone else needs an approved revision, and a disabled skin is
/// refused to both.
fn wearable_reference(skin: &Skin, viewer: i32, kind: SkinKind, owned: bool) -> Option<String> {
    if !skin.namespace.is_publishable() {
        return None;
    }
    let wanted = match kind {
        SkinKind::Snake => crate::skin_store::SkinKind::Snake,
        SkinKind::Base => crate::skin_store::SkinKind::Base,
    };
    // Wrong slot is as wrong as nonexistent: a base is not a snake skin.
    if skin.kind != wanted || skin.content_ref_for(Some(viewer)).is_none() {
        return None;
    }
    // You wear what you hold. Publication says a skin *may* be acquired;
    // holding it is what says this player did, and equipping without that
    // would make acquiring it decorative.
    if !owned {
        return None;
    }
    Some(stored_form(&skin_id_reference(skin.skin_id), kind))
}

/// Turn one requested slot into the storage layer's three-valued form,
/// refusing anything the caller may not wear.
///
/// Two kinds of reference arrive here and they are checked against different
/// authorities. A built-in is checked against the compiled catalogue, because
/// what this build can draw is a fact about the binary. A first-class skin
/// (`skin:<id>`) is checked against the store, because whether it may be worn
/// is a fact about that skin and this viewer — and it is the reason an author
/// can equip a skin nobody else can see yet.
async fn resolve_slot(
    state: &AuthState,
    viewer: i32,
    requested: Option<&Option<String>>,
    kind: SkinKind,
) -> Result<Option<Option<String>>, SkinsApiError> {
    let inner = match read_slot(requested, kind)? {
        SlotRequest::Untouched => return Ok(None),
        SlotRequest::Cleared => return Ok(Some(None)),
        SlotRequest::Named(inner) => inner,
    };

    if let Some(skin_id) = crate::skin_store::equipped_skin_id(inner) {
        let skin = state
            .db
            .get_skin(skin_id)
            .await
            .map_err(SkinsApiError::Internal)?
            .ok_or_else(|| SkinsApiError::UnknownSkin(inner.to_string()))?;

        // A creator holds their own work from the moment it exists, so this
        // only has to be asked about somebody else's.
        let owned = skin.creator_user_id == viewer
            || state
                .db
                .has_skin_grant(viewer, skin_id)
                .await
                .map_err(SkinsApiError::Internal)?;

        return wearable_reference(&skin, viewer, kind, owned)
            .map(|reference| Some(Some(reference)))
            .ok_or_else(|| SkinsApiError::NotOwned);
    }

    catalogue_reference(inner, kind)
        .map(|reference| Some(Some(reference)))
        .ok_or_else(|| SkinsApiError::UnknownSkin(inner.to_string()))
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
}

// ---------------------------------------------------------------------------
// Player-authored skins
// ---------------------------------------------------------------------------

/// How long a browser may hold a skin document before asking again.
///
/// Deliberately short and revalidating rather than `immutable`, even though the
/// bytes genuinely never change: this TTL is the moderation propagation bound.
/// A disabled skin has to stop rendering for people who already fetched it, and
/// a year-long cache would make the kill switch advisory.
const DOCUMENT_CACHE_SECONDS: u32 = 300;

/// A skin as the API presents it. Deliberately not the storage struct: the
/// content references are an implementation detail of resolution, and the
/// document is served by its own route.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SkinSummary {
    pub skin_id: i32,
    pub reference: String,
    pub name: String,
    pub kind: crate::skin_store::SkinKind,
    pub namespace: SkinNamespace,
    pub publication: Publication,
    pub creator_user_id: i32,
    pub creator_username: Option<String>,
    pub price_bux: u32,
    /// The revision a viewer would render, if any. Absent for a disabled skin
    /// and for a private draft belonging to someone else.
    pub content_ref: Option<String>,
    pub head_revision: u32,
    pub published_revision: Option<u32>,
    pub pending_revision: Option<u32>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Whether this viewer holds it. Equipping is gated on owning, so this is
    /// what decides whether a row offers "get" or "equip".
    pub owned: bool,
    /// How many players hold it, and how many are wearing it right now.
    pub owner_count: u32,
    pub wearer_count: u32,
}

impl SkinSummary {
    fn of(skin: &Skin, viewer: Option<i32>, owned: bool) -> Self {
        Self {
            owned: owned || viewer == Some(skin.creator_user_id),
            owner_count: skin.owner_count,
            wearer_count: skin.wearer_count,
            skin_id: skin.skin_id,
            reference: skin_id_reference(skin.skin_id),
            name: skin.name.clone(),
            kind: skin.kind,
            namespace: skin.namespace,
            publication: skin.publication,
            creator_user_id: skin.creator_user_id,
            creator_username: skin.creator_username.clone(),
            price_bux: skin.price_bux,
            content_ref: skin.content_ref_for(viewer).map(str::to_string),
            head_revision: skin.head_revision,
            published_revision: skin.published_revision,
            pending_revision: skin.pending_revision,
            created_at_ms: skin.created_at_ms,
            updated_at_ms: skin.updated_at_ms,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SkinListResponse {
    pub skins: Vec<SkinSummary>,
    pub cursor: Option<String>,
}

/// One immutable target in the administrator's review queue.
///
/// This is deliberately not a [`SkinSummary`]. A summary answers "what may
/// this viewer render?", which makes a private draft disappear for an admin
/// who is not its creator and makes a pending edit resolve to the previously
/// published bytes. Review authority instead names the exact pending revision
/// and the exact document hash submitted with it.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct AdminSkinReview {
    pub skin_id: i32,
    pub name: String,
    pub namespace: SkinNamespace,
    pub publication: Publication,
    pub creator_user_id: i32,
    pub creator_username: Option<String>,
    pub head_revision: u32,
    pub published_revision: Option<u32>,
    /// The immutable revision the creator or factory submitted.
    pub pending_revision: u32,
    /// The content hash stored on that exact immutable revision.
    pub pending_content_ref: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl AdminSkinReview {
    fn exact(skin: &Skin, revision: &SkinRevision) -> anyhow::Result<Self> {
        let pending_revision = skin
            .pending_revision
            .ok_or_else(|| anyhow::anyhow!("review queue skin has no pending revision"))?;
        if revision.skin_id != skin.skin_id || revision.revision != pending_revision {
            return Err(anyhow::anyhow!(
                "review queue target does not match skin {} pending revision {}",
                skin.skin_id,
                pending_revision
            ));
        }

        Ok(Self {
            skin_id: skin.skin_id,
            name: skin.name.clone(),
            namespace: skin.namespace,
            publication: skin.publication,
            creator_user_id: skin.creator_user_id,
            creator_username: skin.creator_username.clone(),
            head_revision: skin.head_revision,
            published_revision: skin.published_revision,
            pending_revision,
            pending_content_ref: revision.content_ref.clone(),
            created_at_ms: skin.created_at_ms,
            updated_at_ms: skin.updated_at_ms,
        })
    }

    /// Build a row only while the exact target discovered through the queue
    /// index is still pending on the authoritative skin record.
    ///
    /// DynamoDB secondary indexes are eventually consistent: a removed queue
    /// marker can outlive the transaction that removed it, and a creator can
    /// submit a newer target while this route loads the immutable revision.
    /// Both are normal races, so they omit one stale row rather than poisoning
    /// the other reviews with a 500 response.
    fn if_still_pending(
        skin: &Skin,
        queued_revision: u32,
        revision: &SkinRevision,
    ) -> Option<Self> {
        if skin.pending_revision != Some(queued_revision) {
            return None;
        }
        Self::exact(skin, revision).ok()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct AdminSkinReviewQueueResponse {
    pub skins: Vec<AdminSkinReview>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSkinRequest {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
    /// The document itself, as JSON. Validated by the same `skin-schema` code
    /// the Builder runs in wasm, so a document the editor accepted is a
    /// document this route accepts.
    pub document: serde_json::Value,
    /// Factory optimizer and technique candidates use the real storage and
    /// renderer pipeline, but live in a server-enforced non-publishable
    /// namespace. This must agree with the reserved idempotency-key prefix.
    #[serde(default)]
    pub evaluation_only: bool,
    /// Stable retry identity for automation. Scoped to the authenticated
    /// creator and bound to the exact create payload.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateSkinRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub price_bux: Option<u32>,
    #[serde(default)]
    pub document: Option<serde_json::Value>,
    /// Revision the editor loaded. Required for document writes so concurrent
    /// edits cannot silently overwrite each other's immutable head.
    #[serde(default)]
    pub expected_head_revision: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ListSkinsQuery {
    #[serde(default)]
    pub kind: Option<String>,
    /// `published` (the default, and the only one that needs no account),
    /// `mine`, or `owned`.
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Longest document worth storing. Generous next to anything the Builder
/// produces, and small enough that a revision item stays far under DynamoDB's
/// 400 KB ceiling with room for its metadata.
const MAX_DOCUMENT_BYTES: usize = 32 * 1024;

const MAX_SKIN_NAME_LENGTH: usize = 40;

/// Validate a document and reduce it to the bytes and reference it will be
/// stored under. One place, so create and update cannot diverge on what they
/// accept or on how they name what they accepted.
/// Parse, validate and canonicalise a document of either schema version.
///
/// Dispatch belongs here rather than at the caller because the *storage* rules
/// are version-blind — a revision is bytes named by their hash, and which
/// schema produced them is not part of that question. What differs is the
/// validator, and `load_any` is the single door that picks the right one.
///
/// This read v1 only until the layer schema shipped, which meant a document
/// from the new Builder was refused at save with a parse error about a field
/// it does not have.
#[derive(Debug)]
struct AcceptedDocument {
    canonical: String,
    content_ref: String,
    schema_version: u32,
    texture_refs: Vec<skin_schema::v2::TextureRefV2>,
    required_seam_axes:
        std::collections::BTreeMap<String, std::collections::BTreeSet<crate::texture::SeamAxis>>,
    contains_text: bool,
}

fn collect_tiled_texture_names(
    layers: &[skin_schema::v2::LayerV2],
    names: &mut std::collections::BTreeSet<String>,
) {
    for layer in layers {
        match &layer.body {
            skin_schema::v2::LayerBodyV2::Group { layers } => {
                collect_tiled_texture_names(layers, names);
            }
            skin_schema::v2::LayerBodyV2::Span {
                source:
                    skin_schema::v2::SourceV2::Image {
                        texture,
                        fit: skin_schema::v2::FitV2::Tile { .. },
                        ..
                    },
                ..
            } => {
                names.insert(texture.clone());
            }
            _ => {}
        }
    }
}

fn required_texture_seam_axes(
    textures: &[skin_schema::v2::TextureRefV2],
    layers: &[skin_schema::v2::LayerV2],
) -> std::collections::BTreeMap<String, std::collections::BTreeSet<crate::texture::SeamAxis>> {
    let mut required = std::collections::BTreeMap::new();
    for texture in textures {
        if texture.kind == skin_schema::v2::TextureKindV2::Sheet {
            required
                .entry(texture.name.clone())
                .or_insert_with(std::collections::BTreeSet::new)
                .insert(crate::texture::SeamAxis::Y);
        }
    }
    let mut tiled = std::collections::BTreeSet::new();
    collect_tiled_texture_names(layers, &mut tiled);
    for name in tiled {
        required
            .entry(name)
            .or_insert_with(std::collections::BTreeSet::new)
            .insert(crate::texture::SeamAxis::X);
    }
    required
}

fn accept_document(document: &serde_json::Value) -> Result<AcceptedDocument, SkinsApiError> {
    let json = serde_json::to_string(document)
        .map_err(|error| SkinsApiError::Invalid(vec![format!("document: {error}")]))?;

    let (bytes, schema_version, texture_refs, required_seam_axes, contains_text) =
        match skin_schema::v2::load_any(&json) {
            Ok(skin_schema::v2::AnySkinDoc::V1(doc)) => (
                skin_schema::content::canonical_bytes(&doc),
                doc.schema_version,
                Vec::new(),
                std::collections::BTreeMap::new(),
                false,
            ),
            Ok(skin_schema::v2::AnySkinDoc::V2(doc)) => {
                let texture_refs = doc.textures.clone();
                let required_seam_axes = required_texture_seam_axes(&doc.textures, &doc.layers);
                let contains_text = crate::skin_store::layers_contain_authored_text(&doc.layers);
                (
                    skin_schema::content::canonical_bytes(&doc),
                    doc.schema_version,
                    texture_refs,
                    required_seam_axes,
                    contains_text,
                )
            }
            Err(errors) => {
                return Err(SkinsApiError::Invalid(
                    errors
                        .into_iter()
                        .map(|error| format!("{}: {}", error.field, error.problem))
                        .collect(),
                ));
            }
        };

    let bytes =
        bytes.map_err(|error| SkinsApiError::Invalid(vec![format!("document: {error}")]))?;
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(SkinsApiError::Invalid(vec![format!(
            "document: {} bytes exceeds the {MAX_DOCUMENT_BYTES}-byte limit",
            bytes.len()
        )]));
    }

    let canonical = String::from_utf8(bytes)
        .map_err(|_| SkinsApiError::Invalid(vec!["document: not valid UTF-8".to_string()]))?;
    let reference = skin_schema::content::reference_for_bytes(canonical.as_bytes());
    Ok(AcceptedDocument {
        canonical,
        content_ref: reference,
        schema_version,
        texture_refs,
        required_seam_axes,
        contains_text,
    })
}

/// Resolve and authorize every generated texture before a revision exists.
///
/// The v2 validator proves the descriptor is structurally usable. This check
/// proves it is the immutable descriptor the texture pipeline actually minted
/// and that this author may compose it. Built-ins are client-shipped and do
/// not have database rows.
async fn authorize_texture_references(
    state: &AuthState,
    auth_user: &AuthUser,
    accepted: &AcceptedDocument,
) -> Result<Vec<String>, SkinsApiError> {
    let mut persisted = std::collections::BTreeSet::new();
    let mut problems = Vec::new();
    for (index, reference) in accepted.texture_refs.iter().enumerate() {
        if skin_schema::v2::is_builtin_texture(&reference.content_ref) {
            continue;
        }
        let Some(descriptor) = reference.descriptor.as_ref() else {
            problems.push(format!(
                "textures[{index}].descriptor: is required for a generated texture"
            ));
            continue;
        };
        let required_axes: Vec<_> = accepted
            .required_seam_axes
            .get(&reference.name)
            .into_iter()
            .flat_map(|axes| axes.iter().copied())
            .collect();
        let texture = state
            .db
            .get_texture_for_use(
                &reference.content_ref,
                descriptor,
                auth_user.user_id,
                auth_user.is_admin,
                &required_axes,
            )
            .await
            .map_err(SkinsApiError::Internal)?;
        let Some(_texture) = texture else {
            let axes = required_axes
                .iter()
                .map(|axis| axis.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            problems.push(format!(
                "textures[{index}]: no owned or shareable stored texture exactly matches this ref, descriptor, and required seam axes [{axes}]"
            ));
            continue;
        };
        // The logical canonical ref identifies the texture; the descriptor's
        // rung refs identify every exact byte object a renderer may receive.
        // Persist both on the immutable revision for audit/GC without making
        // either depend on reparsing its JSON envelope later.
        persisted.insert(reference.content_ref.clone());
        persisted.extend(
            descriptor
                .variants
                .iter()
                .map(|variant| variant.content_ref.clone()),
        );
    }
    if problems.is_empty() {
        Ok(persisted.into_iter().collect())
    } else {
        Err(SkinsApiError::Invalid(problems))
    }
}

fn skin_write_error(error: anyhow::Error) -> SkinsApiError {
    if let Some(conflict) = error.downcast_ref::<SkinWriteError>() {
        SkinsApiError::Conflict(conflict.to_string())
    } else {
        SkinsApiError::Internal(error)
    }
}

fn accept_name(name: &str) -> Result<String, SkinsApiError> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.chars().count() > MAX_SKIN_NAME_LENGTH {
        return Err(SkinsApiError::Invalid(vec![format!(
            "name: must be between 1 and {MAX_SKIN_NAME_LENGTH} characters"
        )]));
    }
    Ok(trimmed.to_string())
}

fn create_namespace(
    idempotency_key: Option<&str>,
    evaluation_only: bool,
) -> Result<SkinNamespace, SkinsApiError> {
    let trial_key = idempotency_key.is_some_and(|key| key.starts_with("factory-trial:"));
    if trial_key != evaluation_only {
        return Err(SkinsApiError::Invalid(vec![
            "evaluationOnly: must be true exactly for the reserved factory-trial: namespace"
                .to_string(),
        ]));
    }
    Ok(if evaluation_only {
        SkinNamespace::Evaluation
    } else {
        SkinNamespace::Production
    })
}

/// Create a skin.
pub async fn create_skin(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<CreateSkinRequest>,
) -> Result<Response, SkinsApiError> {
    // Creating costs storage and puts content in front of other players, so it
    // needs a durable account. A guest's row can be orphaned by closing a tab.
    if auth_user.is_guest {
        return Err(SkinsApiError::GuestNotAllowed);
    }

    let name = accept_name(&request.name)?;
    let kind = match request.kind.as_deref() {
        None | Some("snake") => crate::skin_store::SkinKind::Snake,
        Some("base") => crate::skin_store::SkinKind::Base,
        Some(other) => {
            return Err(SkinsApiError::Invalid(vec![format!(
                "kind: {other} is not a kind of skin"
            )]));
        }
    };
    let accepted = accept_document(&request.document)?;
    let texture_refs = authorize_texture_references(&state, &auth_user, &accepted).await?;
    let idempotency_key = request
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty());
    if idempotency_key.is_some_and(|key| key.len() > 128) {
        return Err(SkinsApiError::Invalid(vec![
            "idempotencyKey: must be at most 128 bytes".to_string(),
        ]));
    }
    let namespace = create_namespace(idempotency_key, request.evaluation_only)?;
    let request_hash = idempotency_key.map(|_| {
        crate::wallet::request_fingerprint(&[
            kind.as_str(),
            namespace.as_str(),
            &name,
            &accepted.content_ref,
        ])
    });

    let skin = state
        .db
        .create_skin(NewSkin {
            creator_user_id: auth_user.user_id,
            creator_username: Some(&auth_user.username),
            kind,
            namespace,
            name: &name,
            revision: NewRevision {
                document: &accepted.canonical,
                content_ref: &accepted.content_ref,
                texture_refs: &texture_refs,
                validated_schema: accepted.schema_version,
                contains_text: accepted.contains_text,
            },
            idempotency_key,
            request_hash: request_hash.as_deref(),
        })
        .await
        .map_err(skin_write_error)?;

    let mut response = (
        StatusCode::CREATED,
        Json(SkinSummary::of(&skin, Some(auth_user.user_id), true)),
    )
        .into_response();
    no_store(&mut response);
    Ok(response)
}

/// Append a revision, rename, or re-price.
pub async fn update_skin(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<UpdateSkinRequest>,
) -> Result<Response, SkinsApiError> {
    let skin = load_visible_skin(&state, skin_id, &auth_user).await?;
    if !skin.may_edit(auth_user.user_id, auth_user.is_admin) {
        // Same answer as "no such skin": a caller who may not edit it has no
        // business learning it exists either.
        return Err(SkinsApiError::NotFound);
    }

    let name = request.name.as_deref().map(accept_name).transpose()?;

    // A rename on a published skin waits for the next approval. Letting it
    // through immediately would be a review bypass: get something bland
    // approved, then rename it to whatever you actually wanted.
    if name.is_some() && skin.publication == Publication::Published && !auth_user.is_admin {
        return Err(SkinsApiError::Invalid(vec![
            "name: a published skin is renamed by its next approved revision".to_string(),
        ]));
    }

    if name.is_some() || request.price_bux.is_some() {
        state
            .db
            .update_skin_metadata(skin_id, name.as_deref(), request.price_bux)
            .await
            .map_err(SkinsApiError::Internal)?;
    }

    let updated = match &request.document {
        None => state
            .db
            .get_skin(skin_id)
            .await
            .map_err(SkinsApiError::Internal)?
            .ok_or(SkinsApiError::NotFound)?,
        Some(document) => {
            let expected_head = request.expected_head_revision.ok_or_else(|| {
                SkinsApiError::Invalid(vec![
                    "expectedHeadRevision: is required when appending a document".to_string(),
                ])
            })?;
            let accepted = accept_document(document)?;
            let texture_refs = authorize_texture_references(&state, &auth_user, &accepted).await?;
            state
                .db
                .put_skin_revision(
                    skin_id,
                    expected_head,
                    NewRevision {
                        document: &accepted.canonical,
                        content_ref: &accepted.content_ref,
                        texture_refs: &texture_refs,
                        validated_schema: accepted.schema_version,
                        contains_text: accepted.contains_text,
                    },
                )
                .await
                .map_err(skin_write_error)?
        }
    };

    let mut response =
        Json(SkinSummary::of(&updated, Some(auth_user.user_id), true)).into_response();
    no_store(&mut response);
    Ok(response)
}

/// One skin, if the caller may see it.
pub async fn get_skin(
    State(state): State<AuthState>,
    auth_user: Option<Extension<AuthUser>>,
    Path(skin_id): Path<i32>,
) -> Result<Response, SkinsApiError> {
    let viewer = auth_user.as_ref().map(|Extension(user)| user);
    let skin = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;

    let user_id = viewer.map(|user| user.user_id);
    let is_admin = viewer.is_some_and(|user| user.is_admin);
    let holds_grant = match user_id {
        Some(user_id) => state
            .db
            .has_skin_grant(user_id, skin_id)
            .await
            .map_err(SkinsApiError::Internal)?,
        None => false,
    };

    if !skin.may_view(user_id, is_admin) && !holds_grant {
        return Err(SkinsApiError::NotFound);
    }

    let mut response = Json(SkinSummary::of(&skin, user_id, holds_grant)).into_response();
    no_store(&mut response);
    Ok(response)
}

/// The render path: one revision's document, by the hash of its bytes.
///
/// Anonymous, because a spectator or a replay viewer holding a reference out of
/// a snapshot has to be able to draw it. Three outcomes matter:
///
/// - `200` for anything that was ever published or ever worn in a match;
/// - `410` when the skin has been disabled, which is what makes moderation
///   reach warm clients and old replays rather than only new matches;
/// - `404` for everything else, including private drafts nobody has worn —
///   uniform, so the route cannot be used to discover what exists.
enum DocumentVisibility<'a> {
    Public(&'a crate::skin_store::SkinRevision),
    Private(&'a crate::skin_store::SkinRevision),
    Gone,
    Hidden,
}

fn resolve_document_visibility<'a>(
    content_ref: &str,
    candidates: &'a [(Skin, crate::skin_store::SkinRevision)],
    viewer: Option<(i32, bool)>,
) -> DocumentVisibility<'a> {
    let was_public = |skin: &Skin, revision: &crate::skin_store::SkinRevision| {
        skin.published_content_ref.as_deref() == Some(content_ref)
            || revision.exposed_at_ms.is_some()
            || skin.published_revision == Some(revision.revision)
    };
    let text_is_public = |revision: &crate::skin_store::SkinRevision| {
        !revision.contains_text || revision.review_approved
    };
    if let Some((_, revision)) = candidates.iter().find(|(skin, revision)| {
        skin.publication != Publication::Disabled
            && was_public(skin, revision)
            && text_is_public(revision)
    }) {
        return DocumentVisibility::Public(revision);
    }
    if let Some((user_id, is_admin)) = viewer
        && let Some((_, revision)) = candidates.iter().find(|(skin, _)| {
            skin.publication != Publication::Disabled && skin.may_edit(user_id, is_admin)
        })
    {
        return DocumentVisibility::Private(revision);
    }
    if candidates.iter().any(|(skin, revision)| {
        skin.publication == Publication::Disabled
            && was_public(skin, revision)
            && text_is_public(revision)
    }) {
        DocumentVisibility::Gone
    } else {
        DocumentVisibility::Hidden
    }
}

pub async fn get_document_by_ref(
    State(state): State<AuthState>,
    auth_user: Option<Extension<AuthUser>>,
    Path(content_ref): Path<String>,
) -> Result<Response, SkinsApiError> {
    if !skin_schema::content::is_content_ref(&content_ref) {
        return Err(SkinsApiError::NotFound);
    }

    let candidates = state
        .db
        .resolve_content_ref(&content_ref)
        .await
        .map_err(SkinsApiError::Internal)?;
    if candidates.is_empty() {
        return Err(SkinsApiError::NotFound);
    }

    // Precedence is explicit and independent of GSI row order. Any enabled,
    // legitimately public copy makes these identical bytes public. Otherwise
    // an authenticated creator/admin may preview their private copy. A 410 is
    // returned only when a formerly public copy exists but every eligible copy
    // is disabled; unrelated private duplicates cannot turn a public document
    // into an arbitrary 404 or leak unreviewed text.
    let viewer = auth_user
        .as_ref()
        .map(|Extension(user)| (user.user_id, user.is_admin));
    let (revision, public_cache) =
        match resolve_document_visibility(&content_ref, &candidates, viewer) {
            DocumentVisibility::Public(revision) => (revision, true),
            DocumentVisibility::Private(revision) => (revision, false),
            DocumentVisibility::Gone => return Err(SkinsApiError::Gone),
            DocumentVisibility::Hidden => return Err(SkinsApiError::NotFound),
        };

    let mut response = (
        [(header::CONTENT_TYPE, "application/json")],
        revision.document.clone(),
    )
        .into_response();
    // The URL is the hash of the bytes, so a public revision is safe to cache
    // hard and forever. A revision that was never public is a different
    // matter: the same URL answers 404 for everyone but its author, and a
    // shared cache keyed on the URL alone would hand one of them the other's
    // answer.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        if public_cache {
            HeaderValue::from_str(&format!(
                "public, max-age={DOCUMENT_CACHE_SECONDS}, must-revalidate"
            ))
            .expect("a formatted cache-control header is always valid")
        } else {
            HeaderValue::from_static("private, no-store")
        },
    );
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{content_ref}\""))
            .unwrap_or_else(|_| HeaderValue::from_static("\"skin\"")),
    );
    Ok(response)
}

/// Published skins, or the caller's own.
pub async fn list_skins(
    State(state): State<AuthState>,
    auth_user: Option<Extension<AuthUser>>,
    Query(query): Query<ListSkinsQuery>,
) -> Result<Response, SkinsApiError> {
    let kind = match query.kind.as_deref() {
        None | Some("snake") => crate::skin_store::SkinKind::Snake,
        Some("base") => crate::skin_store::SkinKind::Base,
        Some(other) => {
            return Err(SkinsApiError::Invalid(vec![format!(
                "kind: {other} is not a kind of skin"
            )]));
        }
    };
    let limit = query.limit.unwrap_or(24).clamp(1, 100);
    let viewer = auth_user.as_ref().map(|Extension(user)| user.user_id);

    let page = match query.filter.as_deref() {
        Some("mine") => {
            let Some(user_id) = viewer else {
                return Err(SkinsApiError::AuthRequired);
            };
            state
                .db
                .list_skins_by_creator(user_id, query.cursor.as_deref(), limit)
                .await
                .map_err(SkinsApiError::Internal)?
        }
        _ => state
            .db
            .list_published_skins(kind, query.cursor.as_deref(), limit)
            .await
            .map_err(SkinsApiError::Internal)?,
    };

    // One query for the viewer's whole shelf, not one per row: a page of
    // twenty skins should not be twenty ownership lookups.
    let held: std::collections::HashSet<i32> = match viewer {
        Some(user_id) => state
            .db
            .list_skin_grants(user_id)
            .await
            .map_err(SkinsApiError::Internal)?
            .into_iter()
            .map(|grant| grant.skin_id)
            .collect(),
        None => std::collections::HashSet::new(),
    };

    let mut response = Json(SkinListResponse {
        skins: page
            .skins
            .iter()
            .map(|skin| SkinSummary::of(skin, viewer, held.contains(&skin.skin_id)))
            .collect(),
        cursor: page.cursor,
    })
    .into_response();
    no_store(&mut response);
    Ok(response)
}

/// Load a skin, refusing anything the caller may not see with the same answer
/// a nonexistent skin gets.
async fn load_visible_skin(
    state: &AuthState,
    skin_id: i32,
    auth_user: &AuthUser,
) -> Result<Skin, SkinsApiError> {
    let skin = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;
    if !skin.may_view(Some(auth_user.user_id), auth_user.is_admin) {
        return Err(SkinsApiError::NotFound);
    }
    Ok(skin)
}

// ---------------------------------------------------------------------------
// Review, reporting, and the kill switch
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportRequest {
    /// Why this was reported. A closed set, so the queue can be triaged
    /// without reading every note.
    pub reason: String,
    #[serde(default)]
    pub note: Option<String>,
}

const REPORT_REASONS: &[&str] = &["offensive", "impersonation", "copyright", "other"];
const MAX_REPORT_NOTE_LENGTH: usize = 500;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdminStatusRequest {
    /// `publish`, `reject`, or `setPublication`. Legacy callers may omit this
    /// and send only `publication`.
    #[serde(default)]
    pub decision: Option<String>,
    /// Required only with `setPublication`; one of `unpublished`, `disabled`,
    /// or `private`. Legacy `published` remains an exact publish request.
    #[serde(default)]
    pub publication: Option<String>,
    /// The immutable review target. Required for publish and reject.
    #[serde(default)]
    pub revision: Option<u32>,
    /// Hash of the exact canonical document the reviewer inspected.
    #[serde(default)]
    pub content_ref: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicationRequest {
    /// Exact immutable revision the factory rendered and is asking a human to
    /// review. A moving "current head" is not review authority.
    pub revision: u32,
    /// Hash of the exact canonical document bytes for `revision`.
    pub content_ref: String,
}

/// Ask for a skin to be reviewed.
pub async fn request_publication(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<PublicationRequest>,
) -> Result<Response, SkinsApiError> {
    let skin = load_visible_skin(&state, skin_id, &auth_user).await?;
    if !skin.may_edit(auth_user.user_id, auth_user.is_admin) {
        return Err(SkinsApiError::NotFound);
    }
    if skin.publication == Publication::Disabled {
        return Err(SkinsApiError::Gone);
    }
    if !skin.namespace.is_publishable() {
        return Err(SkinsApiError::Invalid(vec![
            "evaluation-only skins cannot request publication".to_string(),
        ]));
    }

    let revision = state
        .db
        .get_skin_revision(skin_id, request.revision)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or_else(|| {
            SkinsApiError::Conflict("the requested immutable revision does not exist".to_string())
        })?;
    if revision.content_ref != request.content_ref {
        return Err(SkinsApiError::Conflict(
            "publication request must bind the exact revision content hash".to_string(),
        ));
    }

    state
        .db
        .set_skin_pending_revision(skin_id, Some(request.revision))
        .await
        .map_err(skin_write_error)?;

    let mut response = StatusCode::ACCEPTED.into_response();
    no_store(&mut response);
    Ok(response)
}

/// Withdraw one exact review request.
///
/// The expected revision and hash prevent a late rejection of revision N from
/// clearing a newer request for N+1. This is a creator/service operation; it
/// removes review authority but cannot publish anything.
pub async fn cancel_publication_request(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<PublicationRequest>,
) -> Result<Response, SkinsApiError> {
    let skin = load_visible_skin(&state, skin_id, &auth_user).await?;
    if !skin.may_edit(auth_user.user_id, auth_user.is_admin) {
        return Err(SkinsApiError::NotFound);
    }
    if !skin.namespace.is_publishable() {
        return Err(SkinsApiError::Invalid(vec![
            "evaluation-only skins have no publication request".to_string(),
        ]));
    }
    let revision = state
        .db
        .get_skin_revision(skin_id, request.revision)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or_else(|| {
            SkinsApiError::Conflict("the requested immutable revision does not exist".to_string())
        })?;
    if revision.content_ref != request.content_ref {
        return Err(SkinsApiError::Conflict(
            "publication cancellation must bind the exact revision content hash".to_string(),
        ));
    }
    state
        .db
        .clear_skin_pending_revision_exact(skin_id, request.revision)
        .await
        .map_err(skin_write_error)?;

    let mut response = StatusCode::NO_CONTENT.into_response();
    no_store(&mut response);
    Ok(response)
}

/// Report a skin.
///
/// Deliberately available from the moment player content can reach another
/// player's screen, rather than arriving with the admin queue UI later: the
/// report path and the kill switch are what bound the window between a draft
/// being worn in a public match and a human having looked at it.
pub async fn report_skin(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<ReportRequest>,
) -> Result<Response, SkinsApiError> {
    if !REPORT_REASONS.contains(&request.reason.as_str()) {
        return Err(SkinsApiError::Invalid(vec![format!(
            "reason: must be one of {}",
            REPORT_REASONS.join(", ")
        )]));
    }
    if request
        .note
        .as_ref()
        .is_some_and(|note| note.chars().count() > MAX_REPORT_NOTE_LENGTH)
    {
        return Err(SkinsApiError::Invalid(vec![format!(
            "note: must be at most {MAX_REPORT_NOTE_LENGTH} characters"
        )]));
    }

    let skin = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;

    // A report is a review request against the skin's current head, recorded
    // with its reporter so an abusive reporter is as visible as abusive content.
    let note = match &request.note {
        Some(note) => format!(
            "reported ({}) by {}: {note}",
            request.reason, auth_user.username
        ),
        None => format!("reported ({}) by {}", request.reason, auth_user.username),
    };
    state
        .db
        .set_skin_publication(
            skin_id,
            skin.publication,
            None,
            auth_user.user_id,
            Some(&note),
        )
        .await
        .map_err(SkinsApiError::Internal)?;

    let mut response = StatusCode::ACCEPTED.into_response();
    no_store(&mut response);
    Ok(response)
}

/// The review queue: everything waiting on a human, oldest first.
pub async fn admin_review_queue(
    State(state): State<AuthState>,
    Extension(_auth_user): Extension<AuthUser>,
) -> Result<Response, SkinsApiError> {
    let skins = state
        .db
        .list_skins_awaiting_review(50)
        .await
        .map_err(SkinsApiError::Internal)?;

    let mut reviews = Vec::with_capacity(skins.len());
    for queued_skin in &skins {
        let Some(pending_revision) = queued_skin.pending_revision else {
            // The GSI may briefly return a row after its pending marker was
            // removed. The base item is already authoritative, so omit it.
            continue;
        };
        let Some(revision) = state
            .db
            .get_skin_revision(queued_skin.skin_id, pending_revision)
            .await
            .map_err(SkinsApiError::Internal)?
        else {
            warn!(
                skin_id = queued_skin.skin_id,
                pending_revision, "omitting stale review marker for a missing revision"
            );
            continue;
        };
        let Some(current_skin) = state
            .db
            .get_skin(queued_skin.skin_id)
            .await
            .map_err(SkinsApiError::Internal)?
        else {
            continue;
        };
        let Some(review) =
            AdminSkinReview::if_still_pending(&current_skin, pending_revision, &revision)
        else {
            // Queue removal and replacement both race this read safely. A
            // refresh will show the replacement target once its GSI entry is
            // visible; the obsolete target must never be sent to an admin.
            continue;
        };
        reviews.push(review);
    }

    let mut response = Json(AdminSkinReviewQueueResponse { skins: reviews }).into_response();
    no_store(&mut response);
    Ok(response)
}

fn parse_admin_skin_decision(
    request: &AdminStatusRequest,
) -> Result<SkinReviewDecision, SkinsApiError> {
    match request.decision.as_deref() {
        Some("publish") if request.publication.is_none() => Ok(SkinReviewDecision::Publish),
        Some("reject") if request.publication.is_none() => Ok(SkinReviewDecision::Reject),
        Some("setPublication") => {
            let publication = request
                .publication
                .as_deref()
                .and_then(Publication::parse)
                .ok_or_else(|| {
                    SkinsApiError::Invalid(vec![
                        "publication: setPublication requires a valid state".to_string(),
                    ])
                })?;
            if publication == Publication::Published {
                return Err(SkinsApiError::Invalid(vec![
                    "decision: use publish with an exact review target".to_string(),
                ]));
            }
            Ok(SkinReviewDecision::SetPublication(publication))
        }
        None => {
            let publication = request
                .publication
                .as_deref()
                .and_then(Publication::parse)
                .ok_or_else(|| {
                    SkinsApiError::Invalid(vec![
                        "decision: publish, reject, or setPublication is required".to_string(),
                    ])
                })?;
            Ok(if publication == Publication::Published {
                SkinReviewDecision::Publish
            } else {
                SkinReviewDecision::SetPublication(publication)
            })
        }
        Some(other) => Err(SkinsApiError::Invalid(vec![format!(
            "decision: {other} is not publish, reject, or setPublication"
        )])),
    }
}

fn validate_admin_skin_target(
    request: &AdminStatusRequest,
    decision: SkinReviewDecision,
) -> Result<(), SkinsApiError> {
    if matches!(
        decision,
        SkinReviewDecision::Publish | SkinReviewDecision::Reject
    ) {
        if request.revision.is_none() || request.content_ref.is_none() {
            return Err(SkinsApiError::Invalid(vec![
                "revision and contentRef: both exact review targets are required to decide review"
                    .to_string(),
            ]));
        }
    } else if request.revision.is_some() || request.content_ref.is_some() {
        return Err(SkinsApiError::Invalid(vec![
            "revision and contentRef: are only accepted for publish or reject".to_string(),
        ]));
    }
    Ok(())
}

/// Approve, reject, withdraw, or take down a skin.
pub async fn admin_set_status(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<AdminStatusRequest>,
) -> Result<Response, SkinsApiError> {
    let decision = parse_admin_skin_decision(&request)?;
    validate_admin_skin_target(&request, decision)?;
    if matches!(decision, SkinReviewDecision::Publish) {
        let skin = state
            .db
            .get_skin(skin_id)
            .await
            .map_err(SkinsApiError::Internal)?
            .ok_or(SkinsApiError::NotFound)?;
        if !skin.namespace.is_publishable() {
            return Err(SkinsApiError::Invalid(vec![
                "evaluation-only skins cannot be published".to_string(),
            ]));
        }
    }

    // Approval, exact publication, audit, and queue removal are one database
    // transaction. There is no interval in which unreviewed text is public or
    // in which a crash can publish a different revision than the one reviewed.
    state
        .db
        .decide_skin_review(
            skin_id,
            decision,
            request.revision,
            request.content_ref.as_deref(),
            auth_user.user_id,
            request.reason.as_deref(),
        )
        .await
        .map_err(skin_write_error)?;

    let updated = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;

    let mut response =
        Json(SkinSummary::of(&updated, Some(auth_user.user_id), true)).into_response();
    no_store(&mut response);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equip(json: &str) -> EquipRequest {
        serde_json::from_str(json).expect("valid equip request")
    }

    /// The built-in half of `resolve_slot`, without the database the authored
    /// half needs. Everything a compiled reference goes through, and nothing
    /// else, so these cases stay unit tests.
    fn builtin_slot(
        requested: Option<&Option<String>>,
        kind: SkinKind,
    ) -> Result<Option<Option<String>>, SkinsApiError> {
        let inner = match read_slot(requested, kind)? {
            SlotRequest::Untouched => return Ok(None),
            SlotRequest::Cleared => return Ok(Some(None)),
            SlotRequest::Named(inner) => inner,
        };
        catalogue_reference(inner, kind)
            .map(|reference| Some(Some(reference)))
            .ok_or_else(|| SkinsApiError::UnknownSkin(inner.to_string()))
    }

    fn stored(reference: &str) -> Option<Option<String>> {
        Some(Some(reference.to_string()))
    }

    /// A skin as the store would hand it back.
    fn stored_skin(
        skin_id: i32,
        creator: i32,
        publication: Publication,
        published_revision: Option<u32>,
    ) -> Skin {
        Skin {
            skin_id,
            kind: crate::skin_store::SkinKind::Snake,
            namespace: SkinNamespace::Production,
            creator_user_id: creator,
            creator_username: Some("author".to_string()),
            name: "Electric Keys".to_string(),
            publication,
            pending_revision: None,
            price_bux: 0,
            head_revision: published_revision.unwrap_or(1) + 1,
            published_revision,
            head_content_ref: "sha256:head".to_string(),
            published_content_ref: published_revision.map(|_| "sha256:published".to_string()),
            created_at_ms: 0,
            updated_at_ms: 0,
            published_at_ms: None,
            owner_count: 1,
            wearer_count: 0,
        }
    }

    fn stored_revision(
        skin_id: i32,
        revision: u32,
        content_ref: &str,
        contains_text: bool,
        review_approved: bool,
        exposed: bool,
    ) -> crate::skin_store::SkinRevision {
        crate::skin_store::SkinRevision {
            skin_id,
            revision,
            content_ref: content_ref.to_string(),
            document: "{}".to_string(),
            texture_refs: Vec::new(),
            validated_schema: 2,
            exposed_at_ms: exposed.then_some(1),
            review_approved,
            review_rejected: false,
            contains_text,
            created_at_ms: 0,
        }
    }

    #[test]
    fn admin_review_exposes_another_creators_private_draft_exactly() {
        let mut skin = stored_skin(1_001, 42, Publication::Private, None);
        skin.pending_revision = Some(skin.head_revision);
        skin.head_content_ref = format!("sha256:{}", "a".repeat(64));
        let revision = stored_revision(
            skin.skin_id,
            skin.head_revision,
            &skin.head_content_ref,
            false,
            false,
            false,
        );

        assert_eq!(
            SkinSummary::of(&skin, Some(99), false).content_ref,
            None,
            "an ordinary viewer summary intentionally hides this draft"
        );
        let review = AdminSkinReview::exact(&skin, &revision).expect("exact pending target");
        assert_eq!(review.pending_revision, skin.head_revision);
        assert_eq!(review.pending_content_ref, skin.head_content_ref);
    }

    #[test]
    fn admin_review_exposes_pending_edit_not_the_old_published_bytes() {
        let old_ref = format!("sha256:{}", "1".repeat(64));
        let pending_ref = format!("sha256:{}", "2".repeat(64));
        let mut skin = stored_skin(1_002, 42, Publication::Published, Some(1));
        skin.published_content_ref = Some(old_ref.clone());
        skin.head_content_ref = pending_ref.clone();
        skin.pending_revision = Some(skin.head_revision);
        let revision = stored_revision(
            skin.skin_id,
            skin.head_revision,
            &pending_ref,
            false,
            false,
            false,
        );

        assert_eq!(
            SkinSummary::of(&skin, Some(99), false).content_ref,
            Some(old_ref),
            "the catalogue summary keeps showing the published revision"
        );
        let review = AdminSkinReview::exact(&skin, &revision).expect("exact pending target");
        assert_eq!(review.pending_revision, 2);
        assert_eq!(review.pending_content_ref, pending_ref);
    }

    #[test]
    fn admin_review_refuses_a_revision_other_than_the_pending_target() {
        let mut skin = stored_skin(1_003, 42, Publication::Private, None);
        skin.pending_revision = Some(2);
        let wrong = stored_revision(
            skin.skin_id,
            1,
            &format!("sha256:{}", "f".repeat(64)),
            false,
            false,
            false,
        );

        assert!(AdminSkinReview::exact(&skin, &wrong).is_err());
    }

    #[test]
    fn admin_review_omits_stale_or_moving_queue_targets() {
        let content_ref = format!("sha256:{}", "b".repeat(64));
        let revision = stored_revision(1_004, 2, &content_ref, false, false, false);
        let mut current = stored_skin(1_004, 42, Publication::Private, None);

        current.pending_revision = None;
        assert!(AdminSkinReview::if_still_pending(&current, 2, &revision).is_none());

        current.pending_revision = Some(3);
        assert!(AdminSkinReview::if_still_pending(&current, 2, &revision).is_none());

        current.pending_revision = Some(2);
        assert!(AdminSkinReview::if_still_pending(&current, 2, &revision).is_some());
    }

    #[test]
    fn unreviewed_text_is_private_even_if_a_legacy_row_says_exposed() {
        let content_ref = "sha256:published";
        let skin = stored_skin(10, 42, Publication::Private, None);
        let revision = stored_revision(10, skin.head_revision, content_ref, true, false, true);
        let candidates = vec![(skin, revision)];

        assert!(matches!(
            resolve_document_visibility(content_ref, &candidates, None),
            DocumentVisibility::Hidden
        ));
        assert!(matches!(
            resolve_document_visibility(content_ref, &candidates, Some((42, false))),
            DocumentVisibility::Private(_)
        ));

        let mut approved = candidates;
        approved[0].1.review_approved = true;
        assert!(matches!(
            resolve_document_visibility(content_ref, &approved, None),
            DocumentVisibility::Public(_)
        ));
    }

    /// Phase 0 regression: equipping is an account-local choice, while the
    /// resolved game state is shared with every opponent. An author must be
    /// able to keep working on a text skin without that account reference
    /// smuggling the unreviewed document hash (and therefore its words) into a
    /// match snapshot. The authenticated preview path remains available to the
    /// creator and administrators, but the public/opponent by-ref path stays a
    /// uniform 404-equivalent `Hidden` result.
    #[test]
    fn equipped_unapproved_v2_text_never_enters_an_opponents_snapshot_or_by_ref_view() {
        use common::{GameState, GameType, QueueMode};
        use skin_schema::v2::{
            ClipV2, ColorRef, CornerV2, LayerBodyV2, LayerV2, PropExpr, RegionV2, SlotName,
            SourceV2, SpanV2, TransformV2,
        };

        const CREATOR_ID: i32 = 42;
        const OPPONENT_ID: i32 = 7;
        const PRIVATE_WORDS: &str = "NOT YET REVIEWED";

        let v1: skin_schema::SkinDoc =
            serde_json::from_str(include_str!("../../../skin-schema/skins/classic.skin.json"))
                .expect("the shipped classic skin parses");
        let mut v2 = skin_schema::v2::upgrade(&v1);
        v2.id = "private-text-regression".to_string();
        v2.name = "Private text regression".to_string();
        // The v2 conformance fixtures make this same small readability repair
        // when deriving a new id from the exempt classic document.
        v2.palette.free_for_all[2].fill = "#93a3b5".to_string();
        v2.palette.free_for_all[2].outline = "#5d6e81".to_string();
        v2.palette.friendly[0].accent = Some("#0b2033".to_string());
        v2.palette.friendly[1].accent = Some("#0b2033".to_string());
        v2.palette.enemy[0].accent = Some("#2a0b0b".to_string());
        v2.palette.enemy[1].accent = Some("#2a0b0b".to_string());
        for slot in &mut v2.palette.free_for_all {
            slot.accent = Some("#141a20".to_string());
        }
        v2.layers.push(LayerV2 {
            name: "unreviewed words".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr::constant(0.9),
            transform: TransformV2::default(),
            body: LayerBodyV2::Span {
                region: RegionV2::Body,
                clip: ClipV2::Cells,
                span: SpanV2::whole(),
                corner: CornerV2::Fan,
                source: SourceV2::Text {
                    content: PRIVATE_WORDS.to_string(),
                    color: ColorRef::slot(SlotName::Accent),
                    scale: 0.8,
                },
            },
        });
        let accepted =
            accept_document(&serde_json::to_value(v2).expect("the v2 text document serializes"))
                .expect("the shared v2 validator accepts the text document");
        assert!(accepted.contains_text, "save-time text detection must run");
        assert!(accepted.canonical.contains(PRIVATE_WORDS));

        let mut skin = stored_skin(1_000, CREATOR_ID, Publication::Private, None);
        skin.head_revision = 1;
        skin.head_content_ref = accepted.content_ref.clone();
        let revision = crate::skin_store::SkinRevision {
            skin_id: skin.skin_id,
            revision: skin.head_revision,
            content_ref: accepted.content_ref.clone(),
            document: accepted.canonical.clone(),
            texture_refs: Vec::new(),
            validated_schema: accepted.schema_version,
            exposed_at_ms: None,
            review_approved: false,
            review_rejected: false,
            contains_text: accepted.contains_text,
            created_at_ms: 0,
        };

        let equipped = wearable_reference(&skin, CREATOR_ID, SkinKind::Snake, true)
            .expect("the creator may store an account-local reference to their draft");
        assert_eq!(equipped, "skin:1000");
        let match_revision = crate::matchmaking::match_visible_revision(&revision, None);
        assert!(
            match_revision.is_none(),
            "without a separately hashed approved fallback, the text draft is not match-visible"
        );

        let mut snapshot = GameState::new(
            40,
            40,
            GameType::FreeForAll { max_players: 2 },
            QueueMode::Quickmatch,
            Some(99),
            123,
        );
        snapshot
            .add_player(CREATOR_ID as u32, Some("creator".to_string()))
            .expect("creator joins");
        snapshot
            .add_player(OPPONENT_ID as u32, Some("opponent".to_string()))
            .expect("opponent joins");
        // Exercise the same final resolver `apply_player_skin` uses: an
        // authored account reference is not a built-in, so the match receives
        // classic rather than the draft hash.
        snapshot.set_player_skin(
            CREATOR_ID as u32,
            Some(crate::matchmaking::snapshot_skin_reference(
                Some(&equipped),
                match_revision.map(|revision| revision.content_ref.as_str()),
            )),
        );
        assert_eq!(
            snapshot.skins.get(&(CREATOR_ID as u32)).map(String::as_str),
            Some(skin_catalog::DEFAULT_SKIN_REF)
        );
        let wire_snapshot = serde_json::to_string(&snapshot).expect("snapshot serializes");
        assert!(!wire_snapshot.contains(PRIVATE_WORDS));
        assert!(!wire_snapshot.contains(&accepted.content_ref));
        assert!(!wire_snapshot.contains(&equipped));

        let candidates = vec![(skin, revision)];
        for viewer in [None, Some((OPPONENT_ID, false))] {
            assert!(matches!(
                resolve_document_visibility(&accepted.content_ref, &candidates, viewer),
                DocumentVisibility::Hidden
            ));
        }
        for viewer in [(CREATOR_ID, false), (OPPONENT_ID, true)] {
            let DocumentVisibility::Private(visible) =
                resolve_document_visibility(&accepted.content_ref, &candidates, Some(viewer))
            else {
                panic!("creator/admin should retain the authenticated private preview");
            };
            assert_eq!(visible.document, accepted.canonical);
            assert!(visible.document.contains(PRIVATE_WORDS));
        }
    }

    #[test]
    fn duplicate_hash_visibility_is_order_independent_and_enabled_public_wins() {
        let content_ref = "sha256:published";
        let disabled = stored_skin(10, 42, Publication::Disabled, Some(1));
        let live = stored_skin(11, 43, Publication::Published, Some(1));
        let disabled_revision = stored_revision(10, 1, content_ref, false, true, true);
        let live_revision = stored_revision(11, 1, content_ref, false, true, false);
        for candidates in [
            vec![
                (disabled.clone(), disabled_revision.clone()),
                (live.clone(), live_revision.clone()),
            ],
            vec![
                (live.clone(), live_revision.clone()),
                (disabled.clone(), disabled_revision.clone()),
            ],
        ] {
            assert!(matches!(
                resolve_document_visibility(content_ref, &candidates, None),
                DocumentVisibility::Public(_)
            ));
        }
        assert!(matches!(
            resolve_document_visibility(content_ref, &[(disabled, disabled_revision)], None,),
            DocumentVisibility::Gone
        ));
    }

    /// The three-valued encoding is the whole point of the request type: these
    /// three bodies must mean three different things.
    #[test]
    fn absent_null_and_present_are_three_distinct_requests() {
        let untouched = equip(r#"{"selectedBase":"base:aurora@1"}"#);
        assert_eq!(
            untouched.selected_skin, None,
            "absent leaves the slot alone"
        );

        let cleared = equip(r#"{"selectedSkin":null}"#);
        assert_eq!(cleared.selected_skin, Some(None), "null clears the slot");

        let equipped = equip(r#"{"selectedSkin":"aurora@1"}"#);
        assert_eq!(equipped.selected_skin, Some(Some("aurora@1".to_string())));
    }

    #[test]
    fn an_empty_request_touches_nothing() {
        let request = equip("{}");
        assert_eq!(
            builtin_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            None
        );
        assert_eq!(
            builtin_slot(request.selected_base.as_ref(), SkinKind::Base).unwrap(),
            None
        );
    }

    #[test]
    fn a_known_skin_resolves_to_the_catalogues_own_string() {
        let request = equip(r#"{"selectedSkin":"  aurora@1  "}"#);
        let resolved = builtin_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap();
        assert_eq!(resolved, stored("aurora@1"));
    }

    #[test]
    fn a_base_slot_only_accepts_prefixed_references() {
        let prefixed = equip(r#"{"selectedBase":"base:tidewave@1"}"#);
        assert_eq!(
            builtin_slot(prefixed.selected_base.as_ref(), SkinKind::Base).unwrap(),
            stored("base:tidewave@1")
        );

        let bare = equip(r#"{"selectedBase":"tidewave@1"}"#);
        assert!(
            builtin_slot(bare.selected_base.as_ref(), SkinKind::Base).is_err(),
            "a bare snake reference is not a base"
        );
    }

    /// Match preparation forgives an unknown reference because refusing a join
    /// over cosmetics is worse than a wrong-looking snake. An explicit equip is
    /// the opposite case: nobody is mid-join, and silence would leave the player
    /// believing a choice took when it did not.
    #[test]
    fn an_unknown_skin_is_refused_rather_than_silently_defaulted() {
        for body in [
            r#"{"selectedSkin":"nonesuch@9"}"#,
            r#"{"selectedSkin":"../../etc/passwd"}"#,
            r#"{"selectedSkin":"<script>alert(1)</script>"}"#,
            r#"{"selectedSkin":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}"#,
        ] {
            let request = equip(body);
            assert!(
                builtin_slot(request.selected_skin.as_ref(), SkinKind::Snake).is_err(),
                "{body} should have been refused"
            );
        }
    }

    #[test]
    fn an_over_long_reference_is_refused_without_being_echoed_back() {
        let long = "a".repeat(MAX_SKIN_REF_LENGTH + 1);
        let request = equip(&format!(r#"{{"selectedSkin":"{long}"}}"#));
        let error = builtin_slot(request.selected_skin.as_ref(), SkinKind::Snake)
            .expect_err("an over-long reference is not a skin");
        let SkinsApiError::UnknownSkin(message) = error else {
            panic!("expected an unknown-skin error");
        };
        assert!(
            !message.contains(&long),
            "an over-long reference must not be reflected back into the response"
        );
    }

    #[test]
    fn whitespace_only_is_treated_as_clearing_the_slot() {
        let request = equip(r#"{"selectedSkin":"   "}"#);
        assert_eq!(
            builtin_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            Some(None)
        );
    }

    /// The point of the whole feature: you can wear a skin you made before
    /// anyone has approved it, because it is yours and nobody else can see it
    /// anyway. Until this existed, saving a skin produced something that
    /// appeared in no list and could be equipped from nowhere.
    #[test]
    fn a_creator_may_equip_their_own_unpublished_skin() {
        let draft = stored_skin(1000, 42, Publication::Private, None);
        assert_eq!(
            wearable_reference(&draft, 42, SkinKind::Snake, true),
            Some("skin:1000".to_string()),
        );
        assert_eq!(
            wearable_reference(&draft, 7, SkinKind::Snake, true),
            None,
            "somebody else's private draft is not wearable"
        );
    }

    #[test]
    fn a_published_skin_is_wearable_by_anyone() {
        let published = stored_skin(1000, 42, Publication::Published, Some(1));
        assert_eq!(
            wearable_reference(&published, 7, SkinKind::Snake, true),
            Some("skin:1000".to_string()),
        );
    }

    #[test]
    fn an_evaluation_skin_never_enters_equipment_or_public_visibility() {
        let mut evaluation = stored_skin(1000, 42, Publication::Published, Some(1));
        evaluation.namespace = SkinNamespace::Evaluation;

        assert_eq!(
            wearable_reference(&evaluation, 42, SkinKind::Snake, true),
            None,
            "even its creator renders an evaluation only through factory evidence capture"
        );
        assert!(evaluation.may_view(Some(42), false));
        assert!(evaluation.may_view(Some(7), true));
        assert!(!evaluation.may_view(Some(7), false));
        assert!(!evaluation.may_view(None, false));
        assert_eq!(evaluation.content_ref_for(Some(7)), None);
    }

    /// The kill switch has to beat ownership, or moderation would not be
    /// moderation.
    #[test]
    fn a_disabled_skin_is_refused_even_to_the_person_who_made_it() {
        let disabled = stored_skin(1000, 42, Publication::Disabled, Some(1));
        assert_eq!(
            wearable_reference(&disabled, 42, SkinKind::Snake, true),
            None
        );
        assert_eq!(
            wearable_reference(&disabled, 7, SkinKind::Snake, true),
            None
        );
    }

    /// Withdrawn stops new grants, not existing ones — taking a skin back off
    /// someone who already has it is not a thing this system does.
    #[test]
    fn an_unpublished_skin_stays_wearable_for_the_people_who_have_it() {
        let withdrawn = stored_skin(1000, 42, Publication::Unpublished, Some(1));
        assert_eq!(
            wearable_reference(&withdrawn, 7, SkinKind::Snake, true),
            Some("skin:1000".to_string()),
        );
    }

    /// A snake skin in the base slot is as wrong as a reference to nothing.
    #[test]
    fn a_stored_skin_may_only_be_worn_in_its_own_slot() {
        let snake = stored_skin(1000, 42, Publication::Published, Some(1));
        assert_eq!(wearable_reference(&snake, 42, SkinKind::Base, true), None);
    }

    /// The equipped value is rebuilt from the id, so a caller cannot smuggle
    /// bytes of their own choosing into the field every other player reads.
    #[test]
    fn an_authored_reference_is_stored_in_its_canonical_form() {
        let skin = stored_skin(1000, 42, Publication::Published, Some(1));
        assert_eq!(
            wearable_reference(&skin, 42, SkinKind::Snake, true),
            Some(skin_id_reference(1000)),
        );
    }

    /// Publication says a skin may be acquired; a grant says this player did.
    /// Without the second, "get" would be a button that changed nothing.
    #[test]
    fn a_published_skin_is_not_wearable_until_it_has_been_acquired() {
        let published = stored_skin(1000, 42, Publication::Published, Some(1));
        assert_eq!(
            wearable_reference(&published, 7, SkinKind::Snake, false),
            None,
            "browsing a skin is not owning it"
        );
        assert_eq!(
            wearable_reference(&published, 7, SkinKind::Snake, true),
            Some("skin:1000".to_string()),
        );
    }

    /// Parsing is separate from resolving, and it is what strips the prefix a
    /// base wears — including for an authored base.
    #[test]
    fn reading_a_slot_strips_the_base_prefix_and_nothing_else() {
        let snake = Some(Some("skin:1000".to_string()));
        assert_eq!(
            read_slot(snake.as_ref(), SkinKind::Snake).unwrap(),
            SlotRequest::Named("skin:1000"),
        );

        let base = Some(Some("base:skin:1000".to_string()));
        assert_eq!(
            read_slot(base.as_ref(), SkinKind::Base).unwrap(),
            SlotRequest::Named("skin:1000"),
        );

        let bare = Some(Some("base:".to_string()));
        assert!(
            read_slot(bare.as_ref(), SkinKind::Base).is_err(),
            "a prefix with nothing behind it names no skin"
        );
    }

    /// A `skin:` reference is answered by the store, so it must never fall
    /// through to the catalogue and come back as a built-in.
    #[test]
    fn an_authored_reference_is_never_resolved_against_the_catalogue() {
        assert_eq!(catalogue_reference("skin:1000", SkinKind::Snake), None);
        assert_eq!(catalogue_reference("skin:1000", SkinKind::Base), None);
    }

    #[test]
    fn a_report_must_name_one_of_the_known_reasons() {
        for reason in REPORT_REASONS {
            let body = format!(r#"{{"reason":"{reason}"}}"#);
            let request: ReportRequest = serde_json::from_str(&body).expect("parses");
            assert!(REPORT_REASONS.contains(&request.reason.as_str()));
        }
        let freeform: ReportRequest =
            serde_json::from_str(r#"{"reason":"because I said so"}"#).expect("parses");
        assert!(
            !REPORT_REASONS.contains(&freeform.reason.as_str()),
            "an unrecognised reason is refused by the handler"
        );
    }

    /// Documents are accepted by the same validator the Builder runs, so a
    /// document the editor passed is a document this route stores — and the
    /// bytes it stores are the ones its reference names.
    #[test]
    fn an_accepted_document_is_canonical_and_named_by_its_own_hash() {
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../../../skin-schema/skins/aurora.skin.json"))
                .expect("the shipped document parses");

        let accepted = accept_document(&document).expect("a shipped document is valid");

        assert_eq!(accepted.schema_version, skin_schema::SCHEMA_VERSION);
        assert!(skin_schema::content::is_content_ref(&accepted.content_ref));
        assert_eq!(
            accepted.content_ref,
            skin_schema::content::reference_for_bytes(accepted.canonical.as_bytes()),
            "the stored bytes must be the ones the reference names"
        );

        // Canonical form is stable: re-accepting what we stored is a no-op.
        let reparsed: serde_json::Value =
            serde_json::from_str(&accepted.canonical).expect("canonical bytes are JSON");
        let again = accept_document(&reparsed).expect("still valid");
        assert_eq!(accepted.canonical, again.canonical);
        assert_eq!(accepted.content_ref, again.content_ref);
    }

    /// A layer document saves through the same door.
    ///
    /// This route read v1 only until the layer schema shipped, which meant the
    /// new Builder's Save produced a parse error naming a field the document
    /// does not have — the editor validated it, the preview painted it, and
    /// the server refused it. Storage is version-blind by design; only the
    /// validator differs.
    #[test]
    fn a_v2_layer_document_is_accepted_and_named_the_same_way() {
        let v1: skin_schema::SkinDoc =
            serde_json::from_str(include_str!("../../../skin-schema/skins/classic.skin.json"))
                .expect("the shipped document parses");
        let v2 = skin_schema::v2::upgrade(&v1);
        let document = serde_json::to_value(&v2).expect("serializes");

        let accepted = accept_document(&document).expect("a converted document is valid");

        assert_eq!(accepted.schema_version, skin_schema::v2::SCHEMA_VERSION_V2);
        assert_eq!(
            accepted.content_ref,
            skin_schema::content::reference_for_bytes(accepted.canonical.as_bytes()),
            "a v2 revision is named by its bytes exactly as a v1 one is"
        );

        // ...and the v2 validator is the one that ran: a stack the layer rules
        // refuse has to be refused here too.
        let mut broken = v2.clone();
        broken.layers.clear();
        let error = accept_document(&serde_json::to_value(&broken).expect("serializes"))
            .expect_err("an empty stack paints nothing");
        let SkinsApiError::Invalid(problems) = error else {
            panic!("expected a validation error");
        };
        assert!(
            problems.iter().any(|problem| problem.contains("layers")),
            "{problems:?}"
        );
    }

    #[test]
    fn text_detection_reaches_nested_groups_for_the_review_gate() {
        use skin_schema::v2::{
            ClipV2, ColorRef, CornerV2, LayerBodyV2, LayerV2, PropExpr, RegionV2, SlotName,
            SourceV2, SpanV2, TransformV2,
        };
        let text = LayerV2 {
            name: "words".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr::constant(1.0),
            transform: TransformV2::default(),
            body: LayerBodyV2::Span {
                region: RegionV2::Body,
                clip: ClipV2::Cells,
                span: SpanV2::whole(),
                corner: CornerV2::Fan,
                source: SourceV2::Text {
                    content: "HELLO".to_string(),
                    color: ColorRef::slot(SlotName::Accent),
                    scale: 0.8,
                },
            },
        };
        let group = LayerV2 {
            name: "group".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr::constant(1.0),
            transform: TransformV2::default(),
            body: LayerBodyV2::Group { layers: vec![text] },
        };
        assert!(crate::skin_store::layers_contain_authored_text(&[group]));
    }

    #[test]
    fn seam_requirements_follow_nested_image_use_not_texture_kind_guessing() {
        use skin_schema::v2::{
            ClipV2, CornerV2, FitV2, LayerBodyV2, LayerV2, PropExpr, RegionV2, SourceV2, SpanV2,
            TextureKindV2, TextureRefV2, TilePhaseOriginV2, TransformV2,
        };
        let tiled_sheet = LayerV2 {
            name: "tiled motion".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr::constant(1.0),
            transform: TransformV2::default(),
            body: LayerBodyV2::Span {
                region: RegionV2::Body,
                clip: ClipV2::Cells,
                span: SpanV2::whole(),
                corner: CornerV2::Fan,
                source: SourceV2::Image {
                    texture: "motion".to_string(),
                    fit: FitV2::Tile {
                        cells_per_repeat: None,
                        phase_origin: TilePhaseOriginV2::Head,
                    },
                    fade: None,
                    drift_cells: PropExpr::constant(0.0),
                },
            },
        };
        let nested = LayerV2 {
            name: "group".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr::constant(1.0),
            transform: TransformV2::default(),
            body: LayerBodyV2::Group {
                layers: vec![tiled_sheet],
            },
        };
        let textures = vec![TextureRefV2 {
            name: "motion".to_string(),
            content_ref: format!("sha256:{}", "a".repeat(64)),
            kind: TextureKindV2::Sheet,
            descriptor: None,
        }];
        let required = required_texture_seam_axes(&textures, &[nested]);
        assert_eq!(
            required["motion"],
            [crate::texture::SeamAxis::X, crate::texture::SeamAxis::Y]
                .into_iter()
                .collect()
        );
    }

    /// The validator is the gate, and it is the shared one — a document that
    /// would paint a teammate in enemy colours is refused here, not left for a
    /// player to discover mid-match.
    #[test]
    fn a_document_the_shared_validator_rejects_is_refused() {
        let mut document: serde_json::Value =
            serde_json::from_str(include_str!("../../../skin-schema/skins/aurora.skin.json"))
                .expect("parses");
        // Give the friendly palette an enemy-red fill.
        document["palette"]["friendly"][0]["fill"] = serde_json::json!("#ff2b2b");

        let error = accept_document(&document).expect_err("hue windows are enforced");
        let SkinsApiError::Invalid(problems) = error else {
            panic!("expected a validation error");
        };
        assert!(!problems.is_empty());
    }

    #[test]
    fn a_name_must_be_present_and_bounded() {
        assert!(accept_name("  Tidal  ").is_ok());
        assert_eq!(accept_name(" Tidal ").unwrap(), "Tidal");
        assert!(accept_name("   ").is_err());
        assert!(accept_name(&"x".repeat(MAX_SKIN_NAME_LENGTH + 1)).is_err());
    }

    #[test]
    fn evaluation_namespace_requires_both_the_marker_and_reserved_key() {
        assert_eq!(
            create_namespace(Some("factory-trial:attempt-1"), true).unwrap(),
            SkinNamespace::Evaluation
        );
        assert_eq!(
            create_namespace(Some("factory-concept:concept-1"), false).unwrap(),
            SkinNamespace::Production
        );
        assert!(create_namespace(Some("factory-trial:attempt-1"), false).is_err());
        assert!(create_namespace(Some("factory-concept:concept-1"), true).is_err());
        assert!(create_namespace(None, true).is_err());
    }

    #[test]
    fn publication_request_requires_revision_and_exact_content_ref() {
        let request: PublicationRequest = serde_json::from_str(&format!(
            r#"{{"revision":7,"contentRef":"sha256:{}"}}"#,
            "7".repeat(64)
        ))
        .unwrap();
        assert_eq!(request.revision, 7);
        assert_eq!(request.content_ref, "sha256:".to_string() + &"7".repeat(64));
        assert!(serde_json::from_str::<PublicationRequest>(r#"{"revision":7}"#).is_err());
        assert!(
            serde_json::from_str::<PublicationRequest>(
                r#"{"revision":7,"contentRef":"sha256:x","head":7}"#
            )
            .is_err()
        );
    }

    #[test]
    fn admin_publish_and_reject_bind_the_exact_review_target() {
        let content_ref = "sha256:".to_string() + &"8".repeat(64);
        for decision in ["publish", "reject"] {
            let request: AdminStatusRequest = serde_json::from_value(serde_json::json!({
                "decision": decision,
                "revision": 8,
                "contentRef": content_ref.clone(),
                "reason": "reviewed exact bytes"
            }))
            .expect("valid request");
            let parsed = parse_admin_skin_decision(&request).expect("known decision");
            assert!(matches!(
                (decision, parsed),
                ("publish", SkinReviewDecision::Publish) | ("reject", SkinReviewDecision::Reject)
            ));
            validate_admin_skin_target(&request, parsed).expect("exact target is complete");
        }

        let missing_hash: AdminStatusRequest =
            serde_json::from_str(r#"{"decision":"reject","revision":8}"#).unwrap();
        let decision = parse_admin_skin_decision(&missing_hash).unwrap();
        assert!(validate_admin_skin_target(&missing_hash, decision).is_err());

        let state_with_target: AdminStatusRequest = serde_json::from_value(serde_json::json!({
            "decision": "setPublication",
            "publication": "disabled",
            "revision": 8,
            "contentRef": content_ref
        }))
        .unwrap();
        let decision = parse_admin_skin_decision(&state_with_target).unwrap();
        assert!(validate_admin_skin_target(&state_with_target, decision).is_err());
    }

    #[test]
    fn unknown_fields_are_refused_so_a_typo_is_not_silently_ignored() {
        assert!(serde_json::from_str::<EquipRequest>(r#"{"selectedSkim":"aurora@1"}"#).is_err());
    }
}
