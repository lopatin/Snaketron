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
    axum::Router::new().route(
        "/api/textures/by-ref/:content_ref/:variant",
        axum::routing::get(get_variant_bytes),
    )
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
                content_ref: content_ref.clone(),
                kind: TextureKind::Coat,
                width_px: 768,
                height_px: 64,
                repeat_cells: Some(12.0),
                rows: None,
                seams: SeamReport {
                    horizontal_ratio: 0.8,
                    vertical_ratio: 0.8,
                    repaired: false,
                },
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
