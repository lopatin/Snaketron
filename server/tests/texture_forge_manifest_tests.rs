//! End-to-end strict forge-manifest ingestion through the HTTP handler.

use std::sync::Arc;

use anyhow::Result;
use axum::{
    Extension, Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    routing::post,
};
use server::api::{
    auth::AuthState,
    jwt::JwtManager,
    middleware::AuthUser,
    textures::{ForgeManifest, ingest_forge_manifest},
};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::texture::{SeamReport, Texture, TextureKind, TextureVariant};
use server::texture_store::{InMemoryTextureStore, TextureStore};
use tower::ServiceExt;
use uuid::Uuid;

fn source_png(width: u32, height: u32) -> Vec<u8> {
    let mut image = image::RgbaImage::new(width, height);
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for pixel in image.pixels_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *pixel = image::Rgba([state as u8, (state >> 8) as u8, (state >> 16) as u8, 255]);
    }
    let mut bytes = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, image::ImageFormat::Png)
        .expect("encodes");
    bytes.into_inner()
}

fn multipart(manifest: &ForgeManifest, variants: &[(String, Vec<u8>)]) -> (String, Vec<u8>) {
    let boundary = "snaketron-strict-forge-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"manifest\"\r\n");
    body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
    body.extend_from_slice(&serde_json::to_vec(manifest).expect("manifest serializes"));
    body.extend_from_slice(b"\r\n");
    for (reference, png) in variants {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"variant\"; filename=\"{reference}.png\"\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(b"Content-Type: image/png\r\n\r\n");
        body.extend_from_slice(png);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (boundary.to_string(), body)
}

#[tokio::test]
#[ignore = "requires LocalStack DynamoDB"]
async fn strict_manifest_stores_the_exact_gated_ladder_and_retries_idempotently() -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    // SAFETY: this integration-test binary contains one test.
    unsafe {
        std::env::set_var("DYNAMODB_TABLE_PREFIX", format!("forge_{unique}"));
    }
    let db: Arc<dyn Database> = Arc::new(DynamoDatabase::new().await?);
    let store = Arc::new(InMemoryTextureStore::default());
    let state = AuthState {
        analytics: None,
        db: db.clone(),
        jwt_manager: Arc::new(JwtManager::new("forge-test")),
        user_cache: None,
        crazygames_verifier: None,
        texture_store: Some(store.clone()),
    };
    let app = Router::new()
        .route("/", post(ingest_forge_manifest))
        .layer(Extension(AuthUser {
            user_id: 71_001,
            username: "factory".to_string(),
            is_guest: false,
            is_admin: false,
        }))
        .with_state(state);

    let decoded = server::texture_pixels::decode(&source_png(900, 900)).expect("decodes");
    let shaped = server::texture_pixels::shape(&decoded, TextureKind::Coat, None, false)
        .expect("forge shapes");
    let variants: Vec<(String, Vec<u8>)> = std::iter::once(shaped.canonical)
        .chain(shaped.rungs)
        .map(|rung| {
            (
                skin_schema::content::reference_for_bytes(&rung.bytes),
                rung.bytes,
            )
        })
        .collect();
    let metadata = Texture {
        texture_id: 0,
        owner_user_id: 71_001,
        shareable: false,
        content_ref: variants[0].0.clone(),
        kind: TextureKind::Coat,
        width_px: 768,
        height_px: 64,
        repeat_cells: Some(12.0),
        rows: None,
        seams: SeamReport {
            horizontal_ratio: 0.0,
            vertical_ratio: 0.0,
            repaired: false,
        },
        verified_seam_axes: vec![server::texture::SeamAxis::X],
        last_prompt: None,
        variants: variants
            .iter()
            .zip([64, 32, 16])
            .map(|((reference, bytes), texels_per_cell)| TextureVariant {
                texels_per_cell,
                width_px: 12 * texels_per_cell,
                height_px: texels_per_cell,
                bytes: bytes.len() as u32,
                sha256: reference.trim_start_matches("sha256:").to_string(),
            })
            .collect(),
        created_at_ms: 0,
    };
    let manifest = ForgeManifest {
        schema_version: 1,
        content_ref: metadata.content_ref.clone(),
        descriptor: metadata.descriptor(),
        // A stretched coat needs no wrap join. A later use may re-gate the
        // same immutable bytes for tiling without minting duplicate metadata.
        seam_axes: Vec::new(),
        shareable: false,
    };
    let (boundary, body) = multipart(&manifest, &variants);
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body.clone()))
            .expect("request")
    };

    let mut tampered_variants = variants.clone();
    *tampered_variants[1]
        .1
        .last_mut()
        .expect("encoded PNG is nonempty") ^= 1;
    let (tampered_boundary, tampered_body) = multipart(&manifest, &tampered_variants);
    let tampered = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={tampered_boundary}"),
                )
                .body(Body::from(tampered_body))?,
        )
        .await?;
    assert_eq!(
        tampered.status(),
        StatusCode::BAD_REQUEST,
        "bytes other than the manifest hash names must never become reachable"
    );

    let response = app.clone().oneshot(request()).await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let first: Texture =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await?)?;
    assert_eq!(first.descriptor(), manifest.descriptor);
    assert!(first.verified_seam_axes.is_empty());
    for (reference, exact) in &variants {
        let sha = reference.trim_start_matches("sha256:");
        assert_eq!(store.get(sha).await?, Some(exact.clone()));
    }

    let retry = app.clone().oneshot(request()).await?;
    assert_eq!(retry.status(), StatusCode::OK);
    let second: Texture = serde_json::from_slice(&to_bytes(retry.into_body(), 1024 * 1024).await?)?;
    assert_eq!(second.texture_id, first.texture_id);

    let upgraded_manifest = ForgeManifest {
        schema_version: 1,
        content_ref: manifest.content_ref.clone(),
        descriptor: manifest.descriptor.clone(),
        seam_axes: vec![server::texture::SeamAxis::X],
        shareable: false,
    };
    let (upgrade_boundary, upgrade_body) = multipart(&upgraded_manifest, &variants);
    let upgraded = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .header(
                    header::CONTENT_TYPE,
                    format!("multipart/form-data; boundary={upgrade_boundary}"),
                )
                .body(Body::from(upgrade_body))?,
        )
        .await?;
    assert_eq!(upgraded.status(), StatusCode::OK);
    let upgraded: Texture =
        serde_json::from_slice(&to_bytes(upgraded.into_body(), 1024 * 1024).await?)?;
    assert_eq!(upgraded.texture_id, first.texture_id);
    assert_eq!(
        upgraded.verified_seam_axes,
        vec![server::texture::SeamAxis::X]
    );
    Ok(())
}
