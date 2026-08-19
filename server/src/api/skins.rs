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
use tracing::error;

use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::skin_catalog::{self, BASE_REF_PREFIX, CatalogEntry, MAX_SKIN_REF_LENGTH, SkinKind};
use crate::skin_store::{NewRevision, NewSkin, Publication, Skin, skin_id_reference};

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
    /// The reference does not name anything this build can draw. Unlike match
    /// preparation, which quietly falls back to classic rather than refusing a
    /// join, an explicit equip is worth an error: the player is looking at the
    /// result and deserves to know it did not take.
    UnknownSkin(String),
    Internal(anyhow::Error),
}

impl IntoResponse for SkinsApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "No such skin".to_string()),
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
    let snake = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake)?;
    let base = validate_slot(request.selected_base.as_ref(), SkinKind::Base)?;

    state
        .db
        .set_user_equipment(auth_user.user_id, snake, base)
        .await
        .map_err(SkinsApiError::Internal)?;

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

/// Turn one requested slot into the storage layer's three-valued form,
/// refusing anything the catalogue does not contain.
fn validate_slot(
    requested: Option<&Option<String>>,
    kind: SkinKind,
) -> Result<Option<Option<&'static str>>, SkinsApiError> {
    let Some(slot) = requested else {
        return Ok(None);
    };
    let Some(reference) = slot else {
        return Ok(Some(None));
    };

    let trimmed = reference.trim();
    if trimmed.is_empty() {
        return Ok(Some(None));
    }
    if trimmed.len() > MAX_SKIN_REF_LENGTH {
        return Err(SkinsApiError::UnknownSkin(format!(
            "a {}-character reference",
            trimmed.len()
        )));
    }

    // Resolve to the catalogue's own `&'static str` rather than storing the
    // caller's bytes: what lands in the database is then always a reference
    // this build compiled, never merely one that compared equal to it.
    let known = match kind {
        SkinKind::Snake => skin_catalog::CATALOG
            .iter()
            .find(|entry| entry.reference == trimmed)
            .map(|entry| entry.reference),
        SkinKind::Base => trimmed
            .strip_prefix(BASE_REF_PREFIX)
            .and_then(|inner| {
                skin_catalog::CATALOG
                    .iter()
                    .find(|entry| entry.reference == inner)
            })
            .map(|entry| base_reference(entry.reference)),
    };

    known
        .map(|reference| Some(Some(reference)))
        .ok_or_else(|| SkinsApiError::UnknownSkin(trimmed.to_string()))
}

/// The stored form of a base reference.
///
/// Bases are snake references wearing a prefix, and the set is small and
/// compile-time known, so the prefixed form can be a `&'static str` too rather
/// than a string allocated per request.
fn base_reference(snake_reference: &'static str) -> &'static str {
    static PREFIXED: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    let prefixed = PREFIXED.get_or_init(|| {
        skin_catalog::CATALOG
            .iter()
            .map(|entry| format!("{BASE_REF_PREFIX}{}", entry.reference))
            .collect()
    });
    let index = skin_catalog::CATALOG
        .iter()
        .position(|entry| entry.reference == snake_reference)
        .expect("callers pass a reference they just found in the catalogue");
    prefixed[index].as_str()
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
}

impl SkinSummary {
    fn of(skin: &Skin, viewer: Option<i32>) -> Self {
        Self {
            skin_id: skin.skin_id,
            reference: skin_id_reference(skin.skin_id),
            name: skin.name.clone(),
            kind: skin.kind,
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
fn accept_document(document: &serde_json::Value) -> Result<(String, String, u32), SkinsApiError> {
    let doc: skin_schema::SkinDoc = serde_json::from_value(document.clone())
        .map_err(|error| SkinsApiError::Invalid(vec![format!("document: {error}")]))?;

    skin_schema::validate(&doc).map_err(|errors| {
        SkinsApiError::Invalid(
            errors
                .into_iter()
                .map(|error| format!("{}: {}", error.field, error.problem))
                .collect(),
        )
    })?;

    let bytes = skin_schema::content::canonical_bytes(&doc)
        .map_err(|error| SkinsApiError::Invalid(vec![format!("document: {error}")]))?;
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(SkinsApiError::Invalid(vec![format!(
            "document: {} bytes exceeds the {MAX_DOCUMENT_BYTES}-byte limit",
            bytes.len()
        )]));
    }

    let canonical = String::from_utf8(bytes)
        .map_err(|_| SkinsApiError::Invalid(vec!["document: not valid UTF-8".to_string()]))?;
    let reference = skin_schema::content::reference_for_bytes(canonical.as_bytes());
    Ok((canonical, reference, doc.schema_version))
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
    let (document, content_ref, schema_version) = accept_document(&request.document)?;

    let skin = state
        .db
        .create_skin(NewSkin {
            creator_user_id: auth_user.user_id,
            creator_username: Some(&auth_user.username),
            kind,
            name: &name,
            revision: NewRevision {
                document: &document,
                content_ref: &content_ref,
                texture_refs: &[],
                validated_schema: schema_version,
            },
        })
        .await
        .map_err(SkinsApiError::Internal)?;

    let mut response = (
        StatusCode::CREATED,
        Json(SkinSummary::of(&skin, Some(auth_user.user_id))),
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
            let (document, content_ref, schema_version) = accept_document(document)?;
            state
                .db
                .put_skin_revision(
                    skin_id,
                    NewRevision {
                        document: &document,
                        content_ref: &content_ref,
                        texture_refs: &[],
                        validated_schema: schema_version,
                    },
                )
                .await
                .map_err(SkinsApiError::Internal)?
        }
    };

    let mut response = Json(SkinSummary::of(&updated, Some(auth_user.user_id))).into_response();
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

    let mut response = Json(SkinSummary::of(&skin, user_id)).into_response();
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
pub async fn get_document_by_ref(
    State(state): State<AuthState>,
    Path(content_ref): Path<String>,
) -> Result<Response, SkinsApiError> {
    if !skin_schema::content::is_content_ref(&content_ref) {
        return Err(SkinsApiError::NotFound);
    }

    let Some((skin, revision)) = state
        .db
        .resolve_content_ref(&content_ref)
        .await
        .map_err(SkinsApiError::Internal)?
    else {
        return Err(SkinsApiError::NotFound);
    };

    if skin.publication == Publication::Disabled {
        return Err(SkinsApiError::Gone);
    }

    // Never published and never worn means nobody has a legitimate reason to
    // hold this reference.
    let was_public =
        revision.exposed_at_ms.is_some() || skin.published_revision == Some(revision.revision);
    if !was_public {
        return Err(SkinsApiError::NotFound);
    }

    let mut response = (
        [(header::CONTENT_TYPE, "application/json")],
        revision.document,
    )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&format!(
            "public, max-age={DOCUMENT_CACHE_SECONDS}, must-revalidate"
        ))
        .expect("a formatted cache-control header is always valid"),
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

    let mut response = Json(SkinListResponse {
        skins: page
            .skins
            .iter()
            .map(|skin| SkinSummary::of(skin, viewer))
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
    /// One of `published`, `unpublished`, `disabled`, `private`.
    pub publication: String,
    /// Which revision to publish. Required when publishing, ignored otherwise.
    #[serde(default)]
    pub revision: Option<u32>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Ask for a skin to be reviewed.
pub async fn request_publication(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
) -> Result<Response, SkinsApiError> {
    let skin = load_visible_skin(&state, skin_id, &auth_user).await?;
    if !skin.may_edit(auth_user.user_id, auth_user.is_admin) {
        return Err(SkinsApiError::NotFound);
    }
    if skin.publication == Publication::Disabled {
        return Err(SkinsApiError::Gone);
    }

    state
        .db
        .set_skin_pending_revision(skin_id, Some(skin.head_revision))
        .await
        .map_err(SkinsApiError::Internal)?;

    let mut response = StatusCode::ACCEPTED.into_response();
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
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, SkinsApiError> {
    let skins = state
        .db
        .list_skins_awaiting_review(50)
        .await
        .map_err(SkinsApiError::Internal)?;

    let mut response = Json(SkinListResponse {
        skins: skins
            .iter()
            .map(|skin| SkinSummary::of(skin, Some(auth_user.user_id)))
            .collect(),
        cursor: None,
    })
    .into_response();
    no_store(&mut response);
    Ok(response)
}

/// Approve, reject, withdraw, or take down a skin.
pub async fn admin_set_status(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(skin_id): Path<i32>,
    Json(request): Json<AdminStatusRequest>,
) -> Result<Response, SkinsApiError> {
    let publication = Publication::parse(&request.publication).ok_or_else(|| {
        SkinsApiError::Invalid(vec![format!(
            "publication: {} is not a publication state",
            request.publication
        )])
    })?;

    let skin = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;

    // Publishing means approving one specific revision, so the approval and the
    // publication move together. The revision defaults to whatever review was
    // asked about rather than to the head, because the head may have moved
    // since — approving something an admin never looked at is the one mistake
    // this endpoint must not make easy.
    let published_revision = if publication == Publication::Published {
        let revision = request
            .revision
            .or(skin.pending_revision)
            .or(skin.published_revision)
            .ok_or_else(|| {
                SkinsApiError::Invalid(vec![
                    "revision: nothing has been submitted for review".to_string(),
                ])
            })?;
        state
            .db
            .approve_skin_revision(skin_id, revision)
            .await
            .map_err(SkinsApiError::Internal)?;
        Some(revision)
    } else {
        None
    };

    state
        .db
        .set_skin_publication(
            skin_id,
            publication,
            published_revision,
            auth_user.user_id,
            request.reason.as_deref(),
        )
        .await
        .map_err(SkinsApiError::Internal)?;

    // Whatever the decision, the review request is answered. Rejecting an edit
    // clears only this — a published skin keeps its previously approved
    // revision, so a rejection cannot silently unpublish anything.
    state
        .db
        .set_skin_pending_revision(skin_id, None)
        .await
        .map_err(SkinsApiError::Internal)?;

    let updated = state
        .db
        .get_skin(skin_id)
        .await
        .map_err(SkinsApiError::Internal)?
        .ok_or(SkinsApiError::NotFound)?;

    let mut response = Json(SkinSummary::of(&updated, Some(auth_user.user_id))).into_response();
    no_store(&mut response);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn equip(json: &str) -> EquipRequest {
        serde_json::from_str(json).expect("valid equip request")
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
            validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            None
        );
        assert_eq!(
            validate_slot(request.selected_base.as_ref(), SkinKind::Base).unwrap(),
            None
        );
    }

    #[test]
    fn a_known_skin_resolves_to_the_catalogues_own_string() {
        let request = equip(r#"{"selectedSkin":"  aurora@1  "}"#);
        let resolved = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap();
        assert_eq!(resolved, Some(Some("aurora@1")));
    }

    #[test]
    fn a_base_slot_only_accepts_prefixed_references() {
        let prefixed = equip(r#"{"selectedBase":"base:tidewave@1"}"#);
        assert_eq!(
            validate_slot(prefixed.selected_base.as_ref(), SkinKind::Base).unwrap(),
            Some(Some("base:tidewave@1"))
        );

        let bare = equip(r#"{"selectedBase":"tidewave@1"}"#);
        assert!(
            validate_slot(bare.selected_base.as_ref(), SkinKind::Base).is_err(),
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
                validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).is_err(),
                "{body} should have been refused"
            );
        }
    }

    #[test]
    fn an_over_long_reference_is_refused_without_being_echoed_back() {
        let long = "a".repeat(MAX_SKIN_REF_LENGTH + 1);
        let request = equip(&format!(r#"{{"selectedSkin":"{long}"}}"#));
        let error = validate_slot(request.selected_skin.as_ref(), SkinKind::Snake)
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
            validate_slot(request.selected_skin.as_ref(), SkinKind::Snake).unwrap(),
            Some(None)
        );
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

        let (canonical, reference, schema_version) =
            accept_document(&document).expect("a shipped document is valid");

        assert_eq!(schema_version, skin_schema::SCHEMA_VERSION);
        assert!(skin_schema::content::is_content_ref(&reference));
        assert_eq!(
            reference,
            skin_schema::content::reference_for_bytes(canonical.as_bytes()),
            "the stored bytes must be the ones the reference names"
        );

        // Canonical form is stable: re-accepting what we stored is a no-op.
        let reparsed: serde_json::Value =
            serde_json::from_str(&canonical).expect("canonical bytes are JSON");
        let (again, same_reference, _) = accept_document(&reparsed).expect("still valid");
        assert_eq!(canonical, again);
        assert_eq!(reference, same_reference);
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
    fn unknown_fields_are_refused_so_a_typo_is_not_silently_ignored() {
        assert!(serde_json::from_str::<EquipRequest>(r#"{"selectedSkim":"aurora@1"}"#).is_err());
    }
}
