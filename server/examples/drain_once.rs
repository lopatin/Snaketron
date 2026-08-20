//! Queue one texture job and let a real worker finish it.
//! `cargo run -p server --example drain_once -- <out-dir> [upload.png]`
use std::sync::Arc;

use server::db::{Database, dynamodb::DynamoDatabase};
use server::generation::{Budget, GenerationJob, JobState, Spend};
use server::texture::TextureKind;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("server=info")
        .init();
    let args: Vec<String> = std::env::args().collect();
    let out = std::path::PathBuf::from(&args[1]);
    std::fs::create_dir_all(&out)?;
    let upload = args.get(2).map(std::fs::read).transpose()?;

    let db: Arc<dyn Database> = Arc::new(DynamoDatabase::new().await?);
    let store: Arc<dyn server::texture_store::TextureStore> =
        Arc::new(server::texture_store::InMemoryTextureStore::default());

    let kind = TextureKind::Coat;
    let subject = "iridescent beetle shell, deep green to violet";
    let (w, h) = server::texture_pixels::canonical_size(kind, None);
    let now = chrono::Utc::now().timestamp_millis();

    // An upload puts its bytes away first and names the digest; a generation
    // has none and asks a model instead.
    let source_ref = match &upload {
        Some(bytes) => {
            let sha = server::texture_store::digest(bytes);
            store
                .put(
                    &server::texture_store::TextureObject {
                        sha256: sha.clone(),
                        content_type: "image/png",
                        byte_len: bytes.len(),
                    },
                    bytes,
                )
                .await?;
            Some(sha)
        }
        None => None,
    };

    let job = GenerationJob {
        job_id: format!("drain-{}", uuid::Uuid::new_v4().simple()),
        owner_user_id: 4242,
        kind,
        prompt: server::texture::build_prompt(kind, subject, w, h, 1),
        state: JobState::Queued,
        spend: Spend::default(),
        texture_id: None,
        failure: None,
        detail: None,
        subject: Some(subject.to_string()),
        source_ref,
        reference_refs: Vec::new(),
        created_at_ms: now,
        updated_at_ms: now,
        lease_until_ms: None,
    };
    db.create_generation_job(&job).await?;
    println!(
        "queued {} ({})",
        job.job_id,
        if upload.is_some() {
            "upload"
        } else {
            "generation"
        }
    );

    let worker = server::texture_worker::Worker {
        db: db.clone(),
        store: store.clone(),
        providers: server::generation_providers::configured_providers(),
        budget: Budget::default(),
        name: "drain-once".to_string(),
    };
    let started = std::time::Instant::now();
    let worked = worker.tick(chrono::Utc::now().timestamp_millis()).await?;
    println!(
        "worker did something: {worked} (in {:?})",
        started.elapsed()
    );

    let finished = db.get_generation_job(&job.job_id).await?.expect("the job");
    println!(
        "state: {:?}  failure: {:?}  detail: {:?}",
        finished.state, finished.failure, finished.detail
    );
    println!(
        "spend: {} calls, ${:.3}",
        finished.spend.provider_calls,
        finished.spend.usd_micros as f64 / 1e6
    );

    if let Some(texture_id) = finished.texture_id {
        let textures = db.list_textures_by_owner(4242, 20).await?;
        let texture = textures
            .iter()
            .find(|t| t.texture_id == texture_id)
            .expect("the texture row");
        println!(
            "texture {} ref {} {}x{} seams h={:.4} rows={:?} lastPrompt={:?}",
            texture.texture_id,
            texture.content_ref,
            texture.width_px,
            texture.height_px,
            texture.seams.horizontal_ratio,
            texture.rows,
            texture.last_prompt
        );
        for variant in &texture.variants {
            let bytes = store
                .get(&variant.sha256)
                .await?
                .expect("variant bytes are in the store");
            let path = out.join(format!(
                "v{}-{}x{}.png",
                variant.texels_per_cell, variant.width_px, variant.height_px
            ));
            std::fs::write(&path, &bytes)?;
            println!(
                "  rung {}tpc {}x{} {} bytes -> {}",
                variant.texels_per_cell,
                variant.width_px,
                variant.height_px,
                variant.bytes,
                path.display()
            );
        }
    }
    Ok(())
}
