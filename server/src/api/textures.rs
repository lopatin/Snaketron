//! Textures over HTTP: making one, asking a model for one, and serving the
//! pixels.
//!
//! Two rules shape every route here.
//!
//! **Interactive uploads do not decode in the request handler.** A 4 MB upload
//! can declare 60,000 × 60,000 pixels, so that handler checks the PNG header
//! and hands the bytes to a worker. The authenticated offline-forge endpoint
//! is deliberately different: its one job is to decode, hash, and gate every
//! exact ladder object before making an immutable descriptor reachable.
//!
//! **Generation is a job, not a request.** It is slow, it costs money per
//! attempt, and it fails in ways worth reading, so the client gets an id and
//! polls a state machine rather than holding a connection open and being told
//! only "no".

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::api::auth::AuthState;
use crate::api::middleware::AuthUser;
use crate::generation::{GenerationJob, JobState, Spend};
use crate::texture::{self, TextureKind};

/// A generated texture's canonical size, per kind.
///
/// Fixed rather than caller-chosen: the shape rules are the renderer's, and a
/// prompt asking for an arbitrary size would produce art the gates then refuse.
fn canonical_size(kind: TextureKind) -> (u32, u32, u32) {
    match kind {
        // 12 cells of coat at 64 texels, one cell tall.
        TextureKind::Coat => (768, 64, 1),
        // 20 frames of 16px cells, 20 cells along the body.
        TextureKind::Sheet => (320, 320, 20),
        // Four cells of equipment.
        TextureKind::Overlay => (256, 64, 1),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GenerateRequest {
    pub kind: String,
    /// What the player asked for, in their words.
    pub prompt: String,
    /// Textures of theirs to hand the model alongside the prompt.
    ///
    /// Ids rather than bytes: the caller already owns these and the server can
    /// already read them, so re-uploading pixels it is holding would be a
    /// round trip to say something it knows.
    #[serde(default)]
    pub reference_texture_ids: Vec<i32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct JobAccepted {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct TextureListResponse {
    pub textures: Vec<texture::Texture>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateTextureRequest {
    pub shareable: bool,
}

/// Strict, authenticated hand-off from the offline forge.
///
/// The descriptor is the one embedded in SkinDoc v2. Every listed PNG is sent
/// as a `variant` multipart part whose filename is
/// `sha256:<digest>.png`; no resizing or re-encoding occurs after this gate.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeManifest {
    pub schema_version: u32,
    pub content_ref: String,
    pub descriptor: skin_schema::v2::TextureDescriptorV2,
    /// Use-derived joins the offline forge required. A sheet always includes
    /// `y`; an image layer with tile fit additionally requires `x`.
    pub seam_axes: Vec<texture::SeamAxis>,
    #[serde(default)]
    pub shareable: bool,
}

const FORGE_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
pub enum TexturesApiError {
    NotFound,
    GuestNotAllowed,
    Disabled,
    /// The row exists and its pixels are unreachable.
    ///
    /// Distinct from `Disabled`, whose message is about *making* a texture:
    /// a deployment that stores no textures still serves the rest of the API,
    /// and "generation is unavailable" would send the reader looking in
    /// entirely the wrong place.
    StorageUnavailable,
    /// The image, or the request, is not what it claims to be.
    Invalid(Vec<String>),
    Internal(anyhow::Error),
}

impl IntoResponse for TexturesApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "No such texture".to_string()),
            Self::GuestNotAllowed => (
                StatusCode::FORBIDDEN,
                "Making a texture needs a registered account".to_string(),
            ),
            Self::Disabled => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Texture generation is not available right now".to_string(),
            ),
            Self::StorageUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Texture storage is not available right now".to_string(),
            ),
            Self::Invalid(problems) => (StatusCode::BAD_REQUEST, problems.join("; ")),
            Self::Internal(error) => {
                error!(?error, "textures API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        let mut response = (status, Json(serde_json::json!({ "error": message }))).into_response();
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
        );
        response
    }
}

/// Ask a model for a texture.
pub async fn generate(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<GenerateRequest>,
) -> Result<Response, TexturesApiError> {
    if auth_user.is_guest {
        return Err(TexturesApiError::GuestNotAllowed);
    }

    let kind = TextureKind::parse(&request.kind).ok_or_else(|| {
        TexturesApiError::Invalid(vec![format!("kind: {} is not a texture", request.kind)])
    })?;

    let subject = request.prompt.trim();
    if subject.is_empty() || subject.chars().count() > 400 {
        return Err(TexturesApiError::Invalid(vec![
            "prompt: must be between 1 and 400 characters".to_string(),
        ]));
    }

    // The circuit breaker is checked before the job exists, so a halted
    // pipeline costs nothing rather than queueing work it will not do.
    let day_ago = chrono::Utc::now().timestamp_millis() - 24 * 60 * 60 * 1_000;
    let spent = state
        .db
        .generation_spend_since(day_ago)
        .await
        .map_err(TexturesApiError::Internal)?;
    let ceiling = std::env::var("SNAKETRON_GENERATION_DAILY_USD_MICROS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20_000_000);
    if spent >= ceiling {
        return Err(TexturesApiError::Disabled);
    }

    // Only the caller's own textures may be referenced, and each is resolved to
    // the digest of its canonical variant — the same bytes the renderer draws.
    // Anything they do not own is dropped rather than refused: a stale id in a
    // picker is not worth failing a generation over.
    let mut references = Vec::new();
    if !request.reference_texture_ids.is_empty() {
        let owned = state
            .db
            .list_textures_by_owner(auth_user.user_id, 100)
            .await
            .map_err(TexturesApiError::Internal)?;
        for id in &request.reference_texture_ids {
            if let Some(texture) = owned.iter().find(|each| each.texture_id == *id)
                && let Some(canonical) = texture.variants.first()
            {
                references.push(canonical.sha256.clone());
            }
        }
    }

    let (width, height, rows) = canonical_size(kind);
    let now = chrono::Utc::now().timestamp_millis();
    let job = GenerationJob {
        // The id is derived from who asked, for what, and when — so a
        // double-submitted form is one job rather than two bills.
        job_id: crate::wallet::request_fingerprint(&[
            &auth_user.user_id.to_string(),
            kind.as_str(),
            subject,
            &(now / 10_000).to_string(),
        ])
        .trim_start_matches("sha256:")[..32]
            .to_string(),
        owner_user_id: auth_user.user_id,
        kind,
        prompt: texture::build_prompt(kind, subject, width, height, rows),
        state: JobState::Queued,
        spend: Spend::default(),
        texture_id: None,
        failure: None,
        detail: None,
        subject: Some(subject.to_string()),
        source_ref: None,
        reference_refs: references,
        created_at_ms: now,
        updated_at_ms: now,
        // Nothing has claimed it yet.
        lease_until_ms: None,
    };

    // A repeat of the same request inside the window finds its job already
    // there, which is the point of deriving the id.
    let existing = state
        .db
        .get_generation_job(&job.job_id)
        .await
        .map_err(TexturesApiError::Internal)?;
    // The read above and the write here are not atomic, so two copies of one
    // submit can both find nothing and both try to create. The write is
    // conditional and idempotent, which is what makes the race safe.
    if existing.is_none() {
        state
            .db
            .create_generation_job(&job)
            .await
            .map_err(TexturesApiError::Internal)?;
    }

    let mut response = (
        StatusCode::ACCEPTED,
        Json(JobAccepted {
            job_id: job.job_id.clone(),
        }),
    )
        .into_response();
    no_store(&mut response);
    Ok(response)
}

/// Where a job has got to.
pub async fn get_job(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(job_id): Path<String>,
) -> Result<Response, TexturesApiError> {
    let job = state
        .db
        .get_generation_job(&job_id)
        .await
        .map_err(TexturesApiError::Internal)?
        .ok_or(TexturesApiError::NotFound)?;

    // Someone else's job is answered exactly as a nonexistent one, so job ids
    // cannot be used to learn what other people are making.
    if job.owner_user_id != auth_user.user_id && !auth_user.is_admin {
        return Err(TexturesApiError::NotFound);
    }

    let mut response = Json(job).into_response();
    no_store(&mut response);
    Ok(response)
}

/// The caller's own textures, for the Builder's picker.
pub async fn list_mine(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Response, TexturesApiError> {
    let textures = state
        .db
        .list_textures_by_owner(auth_user.user_id, 100)
        .await
        .map_err(TexturesApiError::Internal)?;

    let mut response = Json(TextureListResponse { textures }).into_response();
    no_store(&mut response);
    Ok(response)
}

/// Explicitly allow or revoke reuse of one immutable texture by other authors.
pub async fn update_texture(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Path(texture_id): Path<i32>,
    Json(request): Json<UpdateTextureRequest>,
) -> Result<Response, TexturesApiError> {
    let texture = state
        .db
        .get_texture(texture_id)
        .await
        .map_err(TexturesApiError::Internal)?
        .ok_or(TexturesApiError::NotFound)?;
    if texture.owner_user_id != auth_user.user_id && !auth_user.is_admin {
        return Err(TexturesApiError::NotFound);
    }
    state
        .db
        .set_texture_shareable(texture_id, request.shareable)
        .await
        .map_err(TexturesApiError::Internal)?;

    let mut updated = texture;
    updated.shareable = request.shareable;
    let mut response = Json(updated).into_response();
    no_store(&mut response);
    Ok(response)
}

/// Ingest a complete forge ladder without changing the bytes that were gated.
pub async fn ingest_forge_manifest(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    mut form: axum::extract::Multipart,
) -> Result<Response, TexturesApiError> {
    if auth_user.is_guest {
        return Err(TexturesApiError::GuestNotAllowed);
    }
    let mut manifest_json = None;
    let mut files = std::collections::BTreeMap::<String, Vec<u8>>::new();
    while let Some(field) = form.next_field().await.map_err(|error| {
        TexturesApiError::Invalid(vec![format!("body: could not be read as a form: {error}")])
    })? {
        let field_name = field.name().unwrap_or_default().to_string();
        match field_name.as_str() {
            "manifest" => {
                let value = field.text().await.map_err(|error| {
                    TexturesApiError::Invalid(vec![format!(
                        "manifest: could not be read as text: {error}"
                    )])
                })?;
                if manifest_json.replace(value).is_some() {
                    return Err(TexturesApiError::Invalid(vec![
                        "manifest: supplied twice".to_string(),
                    ]));
                }
            }
            "variant" => {
                let filename = field.file_name().unwrap_or_default().to_string();
                if filename.is_empty() {
                    return Err(TexturesApiError::Invalid(vec![
                        "variant: every file needs a sha256:<digest>.png filename".to_string(),
                    ]));
                }
                let reference = filename
                    .strip_suffix(".png")
                    .unwrap_or(&filename)
                    .to_string();
                let bytes = field.bytes().await.map_err(|error| {
                    TexturesApiError::Invalid(vec![format!(
                        "variant {filename}: could not be read: {error}"
                    )])
                })?;
                if files.insert(reference.clone(), bytes.to_vec()).is_some() {
                    return Err(TexturesApiError::Invalid(vec![format!(
                        "variant {reference}: supplied twice"
                    )]));
                }
            }
            _ => {
                return Err(TexturesApiError::Invalid(vec![format!(
                    "{field_name}: unknown multipart field"
                )]));
            }
        }
    }
    let manifest: ForgeManifest = serde_json::from_str(
        manifest_json
            .as_deref()
            .ok_or_else(|| TexturesApiError::Invalid(vec!["manifest: is required".to_string()]))?,
    )
    .map_err(|error| TexturesApiError::Invalid(vec![format!("manifest: {error}")]))?;
    if manifest.schema_version != FORGE_MANIFEST_SCHEMA_VERSION {
        return Err(TexturesApiError::Invalid(vec![format!(
            "manifest.schema_version: expected {FORGE_MANIFEST_SCHEMA_VERSION}"
        )]));
    }

    let kind = match manifest.descriptor.kind {
        skin_schema::v2::TextureKindV2::Coat => TextureKind::Coat,
        skin_schema::v2::TextureKindV2::Sheet => TextureKind::Sheet,
        skin_schema::v2::TextureKindV2::Overlay => TextureKind::Overlay,
    };
    if manifest.descriptor.body_columns == Some(0) {
        return Err(TexturesApiError::Invalid(vec![
            "descriptor.body_columns: must be positive when present".to_string(),
        ]));
    }
    if manifest.descriptor.raster_overhang_px > skin_schema::v2::MAX_RASTER_OVERHANG_PX {
        return Err(TexturesApiError::Invalid(vec![format!(
            "descriptor.raster_overhang_px: {} exceeds the bounded {}px-per-side bleed apron around the unchanged 16x16 body cell",
            manifest.descriptor.raster_overhang_px,
            skin_schema::v2::MAX_RASTER_OVERHANG_PX
        )]));
    }
    if matches!(kind, TextureKind::Coat | TextureKind::Sheet)
        && manifest.descriptor.body_columns.is_none()
    {
        return Err(TexturesApiError::Invalid(vec![
            "descriptor.body_columns: coat and sheet art must declare body columns".to_string(),
        ]));
    }
    match (kind, manifest.descriptor.frame_rows) {
        (TextureKind::Sheet, Some(rows))
            if (1..=skin_schema::v2::MAX_SPRITE_FRAME_ROWS).contains(&rows) => {}
        (TextureKind::Sheet, _) => {
            return Err(TexturesApiError::Invalid(vec![format!(
                "descriptor.frame_rows: sheets require 1 to {} rows",
                skin_schema::v2::MAX_SPRITE_FRAME_ROWS
            )]));
        }
        (_, Some(_)) => {
            return Err(TexturesApiError::Invalid(vec![
                "descriptor.frame_rows: only sheets have animation rows".to_string(),
            ]));
        }
        (_, None) => {}
    }
    let mut verified_seam_axes = manifest.seam_axes.clone();
    verified_seam_axes.sort();
    verified_seam_axes.dedup();
    if verified_seam_axes.len() != manifest.seam_axes.len() {
        return Err(TexturesApiError::Invalid(vec![
            "seam_axes: each required axis may appear only once".to_string(),
        ]));
    }
    if kind == TextureKind::Sheet && !verified_seam_axes.contains(&texture::SeamAxis::Y) {
        return Err(TexturesApiError::Invalid(vec![
            "seam_axes: a looping sheet must gate the y join".to_string(),
        ]));
    }
    let expected_rungs: Vec<u32> = std::iter::once(kind.canonical_texels_per_cell())
        .chain(kind.ladder().iter().copied())
        .collect();
    if manifest.descriptor.variants.len() != expected_rungs.len() {
        return Err(TexturesApiError::Invalid(vec![format!(
            "descriptor.variants: expected the complete {:?} ladder",
            expected_rungs
        )]));
    }
    if manifest
        .descriptor
        .variants
        .iter()
        .map(|variant| variant.texels_per_cell)
        .ne(expected_rungs.iter().copied())
    {
        return Err(TexturesApiError::Invalid(vec![
            "descriptor.variants: must be ordered canonical-first with every required rung"
                .to_string(),
        ]));
    }
    if manifest
        .descriptor
        .variants
        .first()
        .map(|variant| variant.content_ref.as_str())
        != Some(manifest.content_ref.as_str())
    {
        return Err(TexturesApiError::Invalid(vec![
            "content_ref: must name the canonical first variant".to_string(),
        ]));
    }

    let mut stored_variants = Vec::new();
    let mut gated_seams = texture::SeamReport {
        horizontal_ratio: 0.0,
        vertical_ratio: 0.0,
        repaired: false,
    };
    let mut expected_files = std::collections::BTreeSet::new();
    let mut variant_refs = std::collections::BTreeSet::new();
    for (index, variant) in manifest.descriptor.variants.iter().enumerate() {
        if !skin_schema::content::is_content_ref(&variant.content_ref) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "descriptor.variants[{index}].content_ref: is not a sha256 reference"
            )]));
        }
        if !variant_refs.insert(variant.content_ref.as_str()) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "descriptor.variants[{index}].content_ref: each rung must name different exact bytes"
            )]));
        }
        let expected_url = format!("/api/textures/variants/{}.png", variant.content_ref);
        if variant.url != expected_url {
            return Err(TexturesApiError::Invalid(vec![format!(
                "descriptor.variants[{index}].url: must be {expected_url}"
            )]));
        }
        if variant.width_px == 0
            || variant.height_px == 0
            || variant.width_px > skin_schema::v2::MAX_TEXTURE_DIMENSION_PX
            || variant.height_px > skin_schema::v2::MAX_TEXTURE_DIMENSION_PX
        {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: dimensions must be within the renderer's 1..={}px edge limit",
                variant.content_ref,
                skin_schema::v2::MAX_TEXTURE_DIMENSION_PX
            )]));
        }
        if variant.bytes == 0 || variant.bytes > skin_schema::v2::MAX_TEXTURE_VARIANT_BYTES {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: compressed size must be within the renderer's 1..={} byte limit",
                variant.content_ref,
                skin_schema::v2::MAX_TEXTURE_VARIANT_BYTES
            )]));
        }
        let decoded_bytes = u64::from(variant.width_px)
            .saturating_mul(u64::from(variant.height_px))
            .saturating_mul(4);
        if decoded_bytes > skin_schema::v2::MAX_TEXTURE_DECODED_BYTES {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: decoded RGBA allocation exceeds the renderer limit",
                variant.content_ref
            )]));
        }
        expected_files.insert(variant.content_ref.clone());
        let bytes = files.get(&variant.content_ref).ok_or_else(|| {
            TexturesApiError::Invalid(vec![format!(
                "variant {}: exact PNG bytes are missing",
                variant.content_ref
            )])
        })?;
        if bytes.len() != variant.bytes as usize
            || skin_schema::content::reference_for_bytes(bytes) != variant.content_ref
        {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: byte count or content hash does not match the manifest",
                variant.content_ref
            )]));
        }
        let header = texture::read_png_header(bytes).map_err(|error| {
            TexturesApiError::Invalid(vec![format!(
                "variant {}: {} {}",
                variant.content_ref, error.field, error.problem
            )])
        })?;
        if (header.width_px, header.height_px) != (variant.width_px, variant.height_px) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: PNG dimensions do not match the manifest",
                variant.content_ref
            )]));
        }
        let expected_width = manifest
            .descriptor
            .body_columns
            .map(|columns| columns.saturating_mul(variant.texels_per_cell));
        if expected_width.is_some_and(|width| width != variant.width_px) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: width does not match body_columns × texels_per_cell",
                variant.content_ref
            )]));
        }
        let row_texels = skin_schema::v2::raster_row_texels(
            variant.texels_per_cell,
            manifest.descriptor.raster_overhang_px,
        )
        .ok_or_else(|| {
            TexturesApiError::Invalid(vec![format!(
                "variant {}: its texel density cannot represent raster_overhang_px exactly",
                variant.content_ref
            )])
        })?;
        let expected_height = match kind {
            TextureKind::Sheet => manifest
                .descriptor
                .frame_rows
                .map(|rows| rows.saturating_mul(row_texels)),
            TextureKind::Coat => Some(row_texels),
            TextureKind::Overlay => None,
        };
        if expected_height.is_some_and(|height| height != variant.height_px) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: height does not match its declared rows/kind",
                variant.content_ref
            )]));
        }
        if kind == TextureKind::Overlay {
            let side = skin_schema::v2::raster_overhang_texels(
                variant.texels_per_cell,
                manifest.descriptor.raster_overhang_px,
            )
            .expect("row_texels proved this rung exact");
            let body_height = variant.height_px.checked_sub(side.saturating_mul(2));
            if body_height
                .is_none_or(|height| height == 0 || !height.is_multiple_of(variant.texels_per_cell))
            {
                return Err(TexturesApiError::Invalid(vec![format!(
                    "variant {}: after its bleed aprons, overlay height is not whole body cells",
                    variant.content_ref
                )]));
            }
        }
        let decoded = crate::texture_pixels::decode(bytes).map_err(|error| {
            TexturesApiError::Invalid(vec![format!(
                "variant {}: {} {}",
                variant.content_ref, error.field, error.problem
            )])
        })?;
        let seams = crate::texture_pixels::seam_report(&decoded);
        if !seams.passes_axes(&verified_seam_axes) {
            return Err(TexturesApiError::Invalid(vec![format!(
                "variant {}: failed a required wrap seam axis",
                variant.content_ref
            )]));
        }
        gated_seams.horizontal_ratio = gated_seams.horizontal_ratio.max(seams.horizontal_ratio);
        gated_seams.vertical_ratio = gated_seams.vertical_ratio.max(seams.vertical_ratio);
        if index == 0 {
            texture::validate_shape(texture::ProposedTexture {
                kind,
                width_px: variant.width_px,
                height_px: variant.height_px,
                rows: manifest.descriptor.frame_rows,
                raster_overhang_px: manifest.descriptor.raster_overhang_px,
                byte_len: bytes.len(),
            })
            .map_err(|errors| {
                TexturesApiError::Invalid(
                    errors
                        .into_iter()
                        .map(|error| format!("canonical {} {}", error.field, error.problem))
                        .collect(),
                )
            })?;
        }
        stored_variants.push(texture::TextureVariant {
            texels_per_cell: variant.texels_per_cell,
            width_px: variant.width_px,
            height_px: variant.height_px,
            bytes: variant.bytes,
            sha256: variant
                .content_ref
                .trim_start_matches("sha256:")
                .to_string(),
        });
    }
    let actual_files: std::collections::BTreeSet<_> = files.keys().cloned().collect();
    if actual_files != expected_files {
        return Err(TexturesApiError::Invalid(vec![
            "variant: the multipart files must exactly equal the manifest entries".to_string(),
        ]));
    }

    if let Some(mut existing) = state
        .db
        .get_texture_for_use(
            &manifest.content_ref,
            &manifest.descriptor,
            auth_user.user_id,
            auth_user.is_admin,
            &[],
        )
        .await
        .map_err(TexturesApiError::Internal)?
        .filter(|texture| texture.owner_user_id == auth_user.user_id)
    {
        let mut merged_axes = existing.verified_seam_axes.clone();
        merged_axes.extend(verified_seam_axes.iter().copied());
        merged_axes.sort();
        merged_axes.dedup();
        let merged_seams = texture::SeamReport {
            horizontal_ratio: existing
                .seams
                .horizontal_ratio
                .max(gated_seams.horizontal_ratio),
            vertical_ratio: existing
                .seams
                .vertical_ratio
                .max(gated_seams.vertical_ratio),
            repaired: existing.seams.repaired || gated_seams.repaired,
        };
        if existing.shareable != manifest.shareable
            || existing.verified_seam_axes != merged_axes
            || existing.seams != merged_seams
        {
            state
                .db
                .update_texture_verification(
                    existing.texture_id,
                    manifest.shareable,
                    &merged_axes,
                    merged_seams,
                )
                .await
                .map_err(TexturesApiError::Internal)?;
            existing.shareable = manifest.shareable;
            existing.verified_seam_axes = merged_axes;
            existing.seams = merged_seams;
        }
        let mut response = Json(existing).into_response();
        no_store(&mut response);
        return Ok(response);
    }

    let Some(store) = state.texture_store.as_ref() else {
        return Err(TexturesApiError::Disabled);
    };
    // Store and read back every exact object before a database row makes the
    // descriptor reachable. Content-addressing makes partial attempts harmless.
    for variant in &manifest.descriptor.variants {
        let bytes = files.get(&variant.content_ref).expect("checked above");
        let sha256 = variant.content_ref.trim_start_matches("sha256:");
        store
            .put(
                &crate::texture_store::TextureObject {
                    sha256: sha256.to_string(),
                    content_type: "image/png",
                    byte_len: bytes.len(),
                },
                bytes,
            )
            .await
            .map_err(|error| {
                TexturesApiError::Internal(anyhow::anyhow!(
                    "could not store strict forge variant: {error}"
                ))
            })?;
        let verified = store
            .get(sha256)
            .await
            .map_err(TexturesApiError::Internal)?
            .ok_or(TexturesApiError::StorageUnavailable)?;
        if verified != *bytes {
            return Err(TexturesApiError::StorageUnavailable);
        }
    }

    let texture_id = state
        .db
        .next_texture_id()
        .await
        .map_err(TexturesApiError::Internal)?;
    let texture = texture::Texture {
        texture_id,
        owner_user_id: auth_user.user_id,
        shareable: manifest.shareable,
        content_ref: manifest.content_ref,
        kind,
        width_px: manifest.descriptor.variants[0].width_px,
        height_px: manifest.descriptor.variants[0].height_px,
        repeat_cells: manifest.descriptor.body_columns.map(|value| value as f32),
        rows: manifest.descriptor.frame_rows,
        raster_overhang_px: manifest.descriptor.raster_overhang_px,
        seams: gated_seams,
        verified_seam_axes,
        last_prompt: None,
        variants: stored_variants,
        created_at_ms: chrono::Utc::now().timestamp_millis(),
    };
    if texture.descriptor() != manifest.descriptor {
        return Err(TexturesApiError::Invalid(vec![
            "descriptor: does not round-trip to the server's immutable descriptor".to_string(),
        ]));
    }
    let texture = state
        .db
        .create_texture(&texture)
        .await
        .map_err(TexturesApiError::Internal)?;
    let mut response = (StatusCode::CREATED, Json(texture)).into_response();
    no_store(&mut response);
    Ok(response)
}

/// One texture's sanitized immutable descriptor.
///
/// Owner ids and generation prompts belong to the authenticated author
/// library, never this anonymous render route. Every URL in the descriptor is
/// addressed by the exact variant bytes' own hash.
pub async fn get_manifest(
    State(state): State<AuthState>,
    Path(content_ref): Path<String>,
) -> Result<Response, TexturesApiError> {
    let texture = state
        .db
        .get_texture_by_ref(&content_ref)
        .await
        .map_err(TexturesApiError::Internal)?
        .ok_or(TexturesApiError::NotFound)?;

    let descriptor = texture.descriptor();
    let descriptor_bytes = serde_json::to_vec(&descriptor).map_err(|error| {
        TexturesApiError::Internal(anyhow::anyhow!(
            "could not serialize texture descriptor: {error}"
        ))
    })?;
    let descriptor_ref = skin_schema::content::reference_for_bytes(&descriptor_bytes);
    let mut response = Json(descriptor).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    if let Ok(etag) = HeaderValue::from_str(&format!("\"{descriptor_ref}\"")) {
        response.headers_mut().insert(header::ETAG, etag);
    }
    Ok(response)
}

/// The one database call the byte route needs.
///
/// Narrow on purpose, the way `ReplayGameReader` is: a route that serves public
/// bytes should be testable without a database, and a trait with one method is
/// what makes that a two-line fake rather than a mock of the whole schema.
#[async_trait::async_trait]
pub trait TextureCatalog: Send + Sync {
    async fn get_texture_by_ref(
        &self,
        content_ref: &str,
    ) -> anyhow::Result<Option<texture::Texture>>;
}

struct DatabaseTextureCatalog {
    db: std::sync::Arc<dyn crate::db::Database>,
}

#[async_trait::async_trait]
impl TextureCatalog for DatabaseTextureCatalog {
    async fn get_texture_by_ref(
        &self,
        content_ref: &str,
    ) -> anyhow::Result<Option<texture::Texture>> {
        self.db.get_texture_by_ref(content_ref).await
    }
}

/// What the byte route needs, which is deliberately not `AuthState`.
///
/// Adding a field to `AuthState` would break two integration-test binaries
/// that construct it by hand, for a route that wants none of what it carries.
/// Its own state merged in as a `Router<AuthState>` is the pattern the replay
/// routes already established.
#[derive(Clone)]
pub struct TextureBytesState {
    catalog: std::sync::Arc<dyn TextureCatalog>,
    store: Option<std::sync::Arc<dyn crate::texture_store::TextureStore>>,
}

/// Serve the pixels of one variant.
///
/// Anonymous, like the skin-document route: these are the bytes a spectator's
/// client needs to render somebody else's snake, and gating them behind a
/// session would mean a match could not draw its own players.
///
/// Cached for a year and `immutable`, which is safe for exactly the reason the
/// store gives for the same header on the S3 object: **the key is the hash of
/// the bytes**, so this URL can never mean anything else. Moderation does not
/// reach players through this route and does not need to — it reaches them
/// through the 300-second document route, which stops handing out the
/// reference at all.
pub async fn get_variant_bytes(
    State(state): State<TextureBytesState>,
    Path((content_ref, variant)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Result<Response, TexturesApiError> {
    // Shape first, before the database is touched: a malformed reference is a
    // 404 rather than a lookup, the same posture the document route takes.
    if !skin_schema::content::is_content_ref(&content_ref) {
        return Err(TexturesApiError::NotFound);
    }
    let Some(texels_per_cell) = parse_variant_segment(&variant) else {
        return Err(TexturesApiError::NotFound);
    };

    let texture = state
        .catalog
        .get_texture_by_ref(&content_ref)
        .await
        .map_err(TexturesApiError::Internal)?
        .ok_or(TexturesApiError::NotFound)?;

    // Exact rung only. A nearest-match fallback would let one URL serve
    // different pixel dimensions as the ladder changed, and a compiled skin's
    // atlas region carries the width and height of the rung it *asked* for.
    let entry = texture
        .variants
        .iter()
        .find(|entry| entry.texels_per_cell == texels_per_cell)
        .ok_or(TexturesApiError::NotFound)?;

    // The digest is the strongest possible validator, so the revalidation is
    // free and settles before any object storage is touched.
    let etag = format!("\"{}\"", entry.sha256);
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == etag)
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        apply_immutable_headers(&mut response, &etag);
        return Ok(response);
    }

    // Metadata without storage: the row is real and the pixels are unreachable,
    // which is a deployment state rather than a missing texture.
    let Some(store) = &state.store else {
        return Err(TexturesApiError::StorageUnavailable);
    };
    let bytes = store
        .get(&entry.sha256)
        .await
        .map_err(|error| {
            error!(%content_ref, texels_per_cell, ?error, "failed to read texture bytes");
            TexturesApiError::StorageUnavailable
        })?
        .ok_or(TexturesApiError::NotFound)?;

    let mut response = (StatusCode::OK, bytes).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("image/png"));
    apply_immutable_headers(&mut response, &etag);
    Ok(response)
}

/// Serve one PNG by the hash of those exact PNG bytes.
///
/// Unlike the legacy logical-texture/rung route, this URL contains no mutable
/// indirection at all. It is the URL embedded in pinned v2 descriptors.
pub async fn get_variant_bytes_by_hash(
    State(state): State<TextureBytesState>,
    Path(segment): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Response, TexturesApiError> {
    let content_ref = segment
        .strip_suffix(".png")
        .ok_or(TexturesApiError::NotFound)?;
    if !skin_schema::content::is_content_ref(content_ref) {
        return Err(TexturesApiError::NotFound);
    }
    let sha256 = content_ref
        .strip_prefix("sha256:")
        .ok_or(TexturesApiError::NotFound)?;
    let etag = format!("\"{content_ref}\"");
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == etag)
    {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        apply_immutable_headers(&mut response, &etag);
        return Ok(response);
    }

    let Some(store) = &state.store else {
        return Err(TexturesApiError::StorageUnavailable);
    };
    let bytes = store
        .get(sha256)
        .await
        .map_err(|error| {
            error!(%content_ref, ?error, "failed to read content-addressed texture variant");
            TexturesApiError::StorageUnavailable
        })?
        .ok_or(TexturesApiError::NotFound)?;
    if skin_schema::content::reference_for_bytes(&bytes) != content_ref {
        error!(%content_ref, "content-addressed texture storage returned different bytes");
        return Err(TexturesApiError::StorageUnavailable);
    }
    let mut response = (StatusCode::OK, bytes).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("image/png"));
    apply_immutable_headers(&mut response, &etag);
    Ok(response)
}

/// `"32.png"` names the 32-texel rung. Anything else names nothing.
fn parse_variant_segment(segment: &str) -> Option<u32> {
    let stem = segment.strip_suffix(".png")?;
    // Bounded before parsing so a very long digit string is refused on its
    // shape rather than on overflow.
    if stem.is_empty() || stem.len() > 4 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok().filter(|texels| *texels > 0)
}

fn apply_immutable_headers(response: &mut Response, etag: &str) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    if let Ok(value) = HeaderValue::from_str(etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
}

/// The byte route, with its own state, ready to merge into the API router.
pub fn build_texture_byte_routes(
    db: std::sync::Arc<dyn crate::db::Database>,
    store: Option<std::sync::Arc<dyn crate::texture_store::TextureStore>>,
) -> axum::Router<crate::api::auth::AuthState> {
    texture_byte_route_template().with_state::<crate::api::auth::AuthState>(TextureBytesState {
        catalog: std::sync::Arc::new(DatabaseTextureCatalog { db }),
        store,
    })
}

fn texture_byte_route_template() -> axum::Router<TextureBytesState> {
    axum::Router::new()
        .route(
            "/api/textures/by-ref/:content_ref/:variant",
            axum::routing::get(get_variant_bytes),
        )
        .route(
            "/api/textures/variants/:variant_ref",
            axum::routing::get(get_variant_bytes_by_hash),
        )
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
}

/// Accept art an author already has.
///
/// `multipart/form-data`: a `kind` field and a `file` part holding the PNG,
/// with an optional `subject` describing it for the library. The response is a
/// job id rather than a texture, and that is the PRD's split rather than an
/// implementation detail — decoding, dimension checks, seam measurement and
/// ladder generation all happen in the worker, because synchronous pixel work
/// in the request path is a CPU-exhaustion vector sitting behind free
/// registration.
///
/// What the handler does do is refuse the obvious: the magic bytes decide
/// whether this is a PNG rather than the filename, and the IHDR's declared
/// dimensions are checked *before* anything decodes them, because a four
/// megabyte body can claim sixty thousand pixels square.
pub async fn upload(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    mut form: axum::extract::Multipart,
) -> Result<Response, TexturesApiError> {
    if auth_user.is_guest {
        return Err(TexturesApiError::GuestNotAllowed);
    }

    let mut kind: Option<TextureKind> = None;
    let mut subject: Option<String> = None;
    let mut png: Option<Vec<u8>> = None;

    while let Some(field) = form.next_field().await.map_err(|error| {
        TexturesApiError::Invalid(vec![format!("body: could not be read as a form: {error}")])
    })? {
        match field.name().unwrap_or_default() {
            "kind" => {
                let value = field.text().await.unwrap_or_default();
                kind = TextureKind::parse(&value);
                if kind.is_none() {
                    return Err(TexturesApiError::Invalid(vec![format!(
                        "kind: {value} is not a texture"
                    )]));
                }
            }
            "subject" => subject = field.text().await.ok(),
            "file" => {
                png = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|error| {
                            TexturesApiError::Invalid(vec![format!(
                                "file: could not be read: {error}"
                            )])
                        })?
                        .to_vec(),
                );
            }
            // An unknown part is ignored rather than refused: a browser may add
            // its own, and this is the one route that takes a real form.
            _ => {}
        }
    }

    let kind =
        kind.ok_or_else(|| TexturesApiError::Invalid(vec!["kind: is required".to_string()]))?;
    let png =
        png.ok_or_else(|| TexturesApiError::Invalid(vec!["file: is required".to_string()]))?;

    // The magic bytes and the header, before anything larger happens.
    let header = texture::read_png_header(&png).map_err(|error| {
        TexturesApiError::Invalid(vec![format!("{} {}", error.field, error.problem)])
    })?;
    texture::validate_shape(texture::ProposedTexture {
        kind,
        width_px: header.width_px,
        height_px: header.height_px,
        rows: if kind == TextureKind::Sheet {
            Some(canonical_size(kind).2)
        } else {
            None
        },
        raster_overhang_px: 0,
        byte_len: png.len(),
    })
    .map_err(|errors| {
        TexturesApiError::Invalid(
            errors
                .into_iter()
                .map(|error| format!("{} {}", error.field, error.problem))
                .collect(),
        )
    })?;

    let Some(store) = state.texture_store.as_ref() else {
        return Err(TexturesApiError::Disabled);
    };

    // The bytes go in the store first, addressed by their own hash, and the
    // job carries the digest. That way a retry of the same upload is the same
    // object rather than a second copy, and the worker reads bytes rather than
    // being handed them through a queue that would have to carry megabytes.
    let sha256 = crate::texture_store::digest(&png);
    store
        .put(
            &crate::texture_store::TextureObject {
                sha256: sha256.clone(),
                content_type: "image/png",
                byte_len: png.len(),
            },
            &png,
        )
        .await
        .map_err(|error| {
            error!(error = %error, "could not store an uploaded texture");
            TexturesApiError::StorageUnavailable
        })?;

    let now = chrono::Utc::now().timestamp_millis();
    let job = GenerationJob {
        // Same bytes, same author, same ten seconds: one job, not two.
        job_id: crate::wallet::request_fingerprint(&[
            &auth_user.user_id.to_string(),
            kind.as_str(),
            &sha256,
            &(now / 10_000).to_string(),
        ])
        .trim_start_matches("sha256:")[..32]
            .to_string(),
        owner_user_id: auth_user.user_id,
        kind,
        // No model is asked anything, so there is no prompt to engineer.
        prompt: String::new(),
        state: JobState::Queued,
        spend: Spend::default(),
        texture_id: None,
        failure: None,
        detail: None,
        subject,
        source_ref: Some(sha256),
        reference_refs: Vec::new(),
        created_at_ms: now,
        updated_at_ms: now,
        lease_until_ms: None,
    };

    if state
        .db
        .get_generation_job(&job.job_id)
        .await
        .map_err(|error| {
            error!(error = %error, "could not read a generation job");
            TexturesApiError::Internal(error)
        })?
        .is_none()
    {
        state
            .db
            .create_generation_job(&job)
            .await
            .map_err(|error| {
                error!(error = %error, "could not record an upload job");
                TexturesApiError::Internal(error)
            })?;
    }

    let mut response = (
        StatusCode::ACCEPTED,
        Json(JobAccepted {
            job_id: job.job_id.clone(),
        }),
    )
        .into_response();
    no_store(&mut response);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rung is named exactly, or not at all.
    ///
    /// No nearest-match fallback: one URL has to mean one set of pixels
    /// forever, because a compiled skin's atlas region carries the width and
    /// height of the rung it *asked* for. A fuzzy match would let the same URL
    /// start serving different dimensions as the ladder changed.
    #[test]
    fn a_variant_segment_names_one_rung_or_nothing() {
        assert_eq!(parse_variant_segment("32.png"), Some(32));
        assert_eq!(parse_variant_segment("8.png"), Some(8));

        for segment in [
            "32",        // no extension
            "32.jpg",    // not our format
            "0.png",     // a rung of nothing
            "-8.png",    // not a count
            "3.5.png",   // not an integer
            "99999.png", // past any ladder, refused on shape
            "../secret.png",
            ".png",
            "",
        ] {
            assert_eq!(
                parse_variant_segment(segment),
                None,
                "`{segment}` should name nothing"
            );
        }
    }

    /// The route serves bytes, revalidates for free, and stays quiet about
    /// what it does not have.
    #[tokio::test]
    async fn the_byte_route_serves_verifies_and_refuses() {
        use crate::texture::{SeamReport, Texture, TextureVariant};
        use crate::texture_store::{InMemoryTextureStore, TextureObject, TextureStore, digest};
        use axum::http::HeaderMap;

        let pixels = b"a rung's worth of pixels".to_vec();
        let sha = digest(&pixels);
        let store = InMemoryTextureStore::default();
        store
            .put(
                &TextureObject {
                    sha256: sha.clone(),
                    content_type: "image/png",
                    byte_len: pixels.len(),
                },
                &pixels,
            )
            .await
            .expect("stored");

        let content_ref = format!("sha256:{}", "c".repeat(64));
        struct OneTexture(Texture);
        #[async_trait::async_trait]
        impl TextureCatalog for OneTexture {
            async fn get_texture_by_ref(&self, reference: &str) -> anyhow::Result<Option<Texture>> {
                Ok((reference == self.0.content_ref).then(|| self.0.clone()))
            }
        }

        let state = TextureBytesState {
            catalog: std::sync::Arc::new(OneTexture(Texture {
                texture_id: 1,
                owner_user_id: 7,
                shareable: false,
                content_ref: content_ref.clone(),
                kind: TextureKind::Coat,
                width_px: 768,
                height_px: 64,
                repeat_cells: Some(12.0),
                rows: None,
                raster_overhang_px: 0,
                seams: SeamReport {
                    horizontal_ratio: 0.8,
                    vertical_ratio: 0.8,
                    repaired: false,
                },
                verified_seam_axes: vec![crate::texture::SeamAxis::X],
                last_prompt: None,
                variants: vec![TextureVariant {
                    texels_per_cell: 32,
                    width_px: 384,
                    height_px: 32,
                    bytes: pixels.len() as u32,
                    sha256: sha.clone(),
                }],
                created_at_ms: 0,
            })),
            store: Some(std::sync::Arc::new(store)),
        };

        let fetch = |variant: &str, headers: HeaderMap, reference: String| {
            let state = state.clone();
            let variant = variant.to_string();
            async move { get_variant_bytes(State(state), Path((reference, variant)), headers).await }
        };

        // The rung that exists comes back with the digest as its validator and
        // a year of immutable caching — safe precisely because the key *is*
        // the hash, so this URL can never mean anything else.
        let response = fetch("32.png", HeaderMap::new(), content_ref.clone())
            .await
            .expect("the rung exists");
        assert_eq!(response.status(), StatusCode::OK);
        let etag = response
            .headers()
            .get(header::ETAG)
            .expect("a validator")
            .to_str()
            .expect("ascii")
            .to_string();
        assert_eq!(etag, format!("\"{sha}\""));
        assert!(
            response.headers()[header::CACHE_CONTROL]
                .to_str()
                .unwrap()
                .contains("immutable")
        );

        // The v2 descriptor route is addressed by these exact bytes, with no
        // logical texture or rung lookup in between.
        let direct = get_variant_bytes_by_hash(
            State(state.clone()),
            Path(format!("sha256:{sha}.png")),
            HeaderMap::new(),
        )
        .await
        .expect("variant hash is directly fetchable");
        assert_eq!(direct.status(), StatusCode::OK);
        assert_eq!(
            direct.headers()[header::ETAG].to_str().unwrap(),
            format!("\"sha256:{sha}\"")
        );

        // Revalidation settles before object storage is touched.
        let mut conditional = HeaderMap::new();
        conditional.insert(header::IF_NONE_MATCH, etag.parse().expect("valid"));
        let response = fetch("32.png", conditional, content_ref.clone())
            .await
            .expect("revalidates");
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

        // A rung the ladder does not carry, and a reference nobody minted,
        // are both simply absent — uniform, so neither confirms the other.
        for (variant, reference) in [
            ("16.png", content_ref.clone()),
            ("32.png", format!("sha256:{}", "d".repeat(64))),
            ("32.png", "not-a-reference".to_string()),
        ] {
            let error = fetch(variant, HeaderMap::new(), reference)
                .await
                .expect_err("absent");
            assert!(matches!(error, TexturesApiError::NotFound));
        }

        // Metadata without storage is a deployment state, not a missing
        // texture, and says so with its own message.
        let stateless = TextureBytesState {
            store: None,
            ..state.clone()
        };
        let error = get_variant_bytes(
            State(stateless),
            Path((content_ref, "32.png".to_string())),
            HeaderMap::new(),
        )
        .await
        .expect_err("no storage configured");
        assert!(matches!(error, TexturesApiError::StorageUnavailable));
    }

    /// Generated sizes have to be shapes the validator accepts, or every
    /// generation is thrown away after it has been paid for.
    #[test]
    fn every_generated_size_is_one_the_shape_rules_accept() {
        for kind in [TextureKind::Coat, TextureKind::Sheet, TextureKind::Overlay] {
            let (width, height, rows) = canonical_size(kind);
            let proposed = texture::ProposedTexture {
                kind,
                width_px: width,
                height_px: height,
                rows: (kind == TextureKind::Sheet).then_some(rows),
                raster_overhang_px: 0,
                byte_len: 1024,
            };
            texture::validate_shape(proposed)
                .unwrap_or_else(|errors| panic!("{kind:?} generates an invalid shape: {errors:?}"));
        }
    }

    #[test]
    fn a_generated_size_carries_into_the_prompt_it_asks_for() {
        let (width, height, rows) = canonical_size(TextureKind::Sheet);
        let prompt = texture::build_prompt(TextureKind::Sheet, "a comet", width, height, rows);
        assert!(prompt.contains(&format!("{width}x{height}")));
        assert!(prompt.contains(&format!("{rows} rows")));
    }

    #[test]
    fn an_unknown_kind_is_refused() {
        assert!(TextureKind::parse("sprite").is_none());
        assert!(TextureKind::parse("coat").is_some());
    }
}
