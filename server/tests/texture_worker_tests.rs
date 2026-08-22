//! A queued job becomes a stored texture.
//!
//! Against LocalStack, because the job's journey through storage is most of
//! what is being tested, and with a scripted provider rather than a real one,
//! because a test that spends money to prove the budget works is a test nobody
//! runs twice.

use std::sync::Arc;

use anyhow::{Context, Result};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::generation::{
    Budget, FailureKind, GenerationJob, ImageProvider, JobState, ProviderOutcome, Spend,
};
use server::texture::TextureKind;
use server::texture_store::{InMemoryTextureStore, TextureObject, TextureStore, digest};
use server::texture_worker::Worker;
use uuid::Uuid;

/// A provider that answers from a script, so a test can pin the outcome.
struct Scripted(std::sync::Mutex<Vec<ProviderOutcome>>);

#[async_trait::async_trait]
impl ImageProvider for Scripted {
    fn name(&self) -> &'static str {
        "scripted"
    }
    async fn generate(&self, _: &str, _: u32, _: u32, _: &[Vec<u8>]) -> ProviderOutcome {
        self.0
            .lock()
            .expect("not poisoned")
            .pop()
            .unwrap_or(ProviderOutcome::Unavailable {
                detail: "the script ran out".to_string(),
            })
    }
}

/// A PNG with enough going on that a seam has something to hide in.
fn png(width: u32, height: u32) -> Vec<u8> {
    let mut image = image::RgbaImage::new(width, height);
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for pixel in image.pixels_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let byte = |shift: u32| ((state >> shift) & 0xff) as u8;
        *pixel = image::Rgba([byte(0), byte(8), byte(16), 255]);
    }
    let mut bytes = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, image::ImageFormat::Png)
        .expect("encodes");
    bytes.into_inner()
}

fn job(id: &str, kind: TextureKind, source_ref: Option<String>) -> GenerationJob {
    let now = chrono::Utc::now().timestamp_millis();
    GenerationJob {
        job_id: id.to_string(),
        owner_user_id: 8_181,
        kind,
        prompt: "a scripted prompt".to_string(),
        state: JobState::Queued,
        spend: Spend::default(),
        texture_id: None,
        failure: None,
        detail: None,
        subject: Some("mottled bark".to_string()),
        source_ref,
        reference_refs: Vec::new(),
        created_at_ms: now,
        updated_at_ms: now,
        lease_until_ms: None,
    }
}

/// Start `./test-deps.sh` (LocalStack) before running this.
#[tokio::test]
#[ignore = "requires LocalStack DynamoDB"]
async fn a_queued_job_becomes_a_texture_and_a_refusal_becomes_a_reason() -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    // SAFETY: one test per binary, so nothing else can observe the value.
    unsafe {
        std::env::set_var("DYNAMODB_TABLE_PREFIX", format!("texwork_{unique}"));
    }
    let db: Arc<dyn Database> = Arc::new(DynamoDatabase::new().await?);
    let store: Arc<dyn TextureStore> = Arc::new(InMemoryTextureStore::default());

    let worker = |script: Vec<ProviderOutcome>| Worker {
        db: db.clone(),
        store: store.clone(),
        providers: vec![Box::new(Scripted(std::sync::Mutex::new(script)))],
        budget: Budget::default(),
        name: "test-worker".to_string(),
    };
    let now = || chrono::Utc::now().timestamp_millis();

    // A generation the provider answers.
    let generated = job(&format!("gen-{}", &unique[..12]), TextureKind::Coat, None);
    db.create_generation_job(&generated).await?;
    assert!(
        worker(vec![ProviderOutcome::Image {
            png: png(900, 900),
            usd_micros: 40_000,
        }])
        .tick(now())
        .await?
    );

    let done = db
        .get_generation_job(&generated.job_id)
        .await?
        .context("the job is there")?;
    assert_eq!(done.state, JobState::Done, "detail: {:?}", done.detail);
    assert_eq!(
        done.spend.provider_calls, 1,
        "the call has to reach the record, or the daily ceiling never sees it"
    );
    assert_eq!(done.spend.usd_micros, 40_000);

    let texture_id = done
        .texture_id
        .context("a finished job names its texture")?;
    let textures = db.list_textures_by_owner(8_181, 20).await?;
    let texture = textures
        .iter()
        .find(|each| each.texture_id == texture_id)
        .context("the texture row exists")?;
    assert_eq!((texture.width_px, texture.height_px), (768, 64));
    assert_eq!(
        texture.seams.horizontal_ratio, 0.0,
        "a coat is mirrored, so its wrap is exact"
    );
    assert_eq!(
        texture.last_prompt.as_deref(),
        Some("mottled bark"),
        "the author's words, not the engineered prompt around them"
    );
    // Canonical plus the two rungs below it, every one of them fetchable.
    assert_eq!(texture.variants.len(), 3);
    for variant in &texture.variants {
        assert!(
            store.get(&variant.sha256).await?.is_some(),
            "rung {} was recorded but not stored",
            variant.texels_per_cell
        );
    }

    // An upload, which reaches the same place without a provider.
    let bytes = png(768, 64);
    let sha = digest(&bytes);
    store
        .put(
            &TextureObject {
                sha256: sha.clone(),
                content_type: "image/png",
                byte_len: bytes.len(),
            },
            &bytes,
        )
        .await?;
    let uploaded = job(
        &format!("up-{}", &unique[..12]),
        TextureKind::Coat,
        Some(sha),
    );
    db.create_generation_job(&uploaded).await?;
    assert!(worker(Vec::new()).tick(now()).await?);

    let done = db
        .get_generation_job(&uploaded.job_id)
        .await?
        .context("the upload job is there")?;
    assert_eq!(done.state, JobState::Done, "detail: {:?}", done.detail);
    assert_eq!(
        done.spend.provider_calls, 0,
        "an upload asked nobody for anything"
    );

    // A refusal is a reason, not a retry: asking the next vendor the same
    // question buys the same answer.
    let refused = job(&format!("no-{}", &unique[..12]), TextureKind::Coat, None);
    db.create_generation_job(&refused).await?;
    assert!(
        worker(vec![ProviderOutcome::Refused {
            reason: "the model declined this prompt".to_string(),
        }])
        .tick(now())
        .await?
    );

    let done = db
        .get_generation_job(&refused.job_id)
        .await?
        .context("the refused job is there")?;
    assert_eq!(done.state, JobState::Failed);
    assert_eq!(done.failure, Some(FailureKind::ProviderRefused));
    assert!(
        done.detail
            .is_some_and(|detail| detail.contains("declined")),
        "a failure has to say what happened"
    );

    // And an empty queue is not an error.
    assert!(!worker(Vec::new()).tick(now()).await?);
    Ok(())
}
