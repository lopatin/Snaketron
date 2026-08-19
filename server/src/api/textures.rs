//! Textures over HTTP: making one, asking a model for one, and serving the
//! pixels.
//!
//! Two rules shape every route here.
//!
//! **Nothing decodes an image in the request handler.** A 4 MB upload can
//! declare 60,000 × 60,000 pixels, so the handler reads the PNG header, checks
//! the shape against the kind's conventions, and hands the bytes to a worker.
//! Doing the pixel work inline would give every free account a multi-second
//! CPU burn behind a rate limit.
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

#[derive(Debug)]
pub enum TexturesApiError {
    NotFound,
    GuestNotAllowed,
    Disabled,
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
        created_at_ms: now,
        updated_at_ms: now,
    };

    // A repeat of the same request inside the window finds its job already
    // there, which is the point of deriving the id.
    let existing = state
        .db
        .get_generation_job(&job.job_id)
        .await
        .map_err(TexturesApiError::Internal)?;
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

/// One texture's manifest: which rungs exist, and what each is called.
///
/// Short-cached and revalidating, unlike the bytes: the rungs can change when
/// an author overrides one with hand-simplified art, and a client holding a
/// year-old manifest would never see it.
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

    let mut response = Json(texture).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=300, must-revalidate"),
    );
    Ok(response)
}

fn no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
