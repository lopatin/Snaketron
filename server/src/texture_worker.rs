//! The loop that actually does the work a generation job describes.
//!
//! Everything else in the pipeline existed already — the route that records a
//! job, the ledger that meters it, the ceiling that halts it, the providers
//! that would answer it, the store that would hold the result. What was
//! missing was anything that took a job off the queue, which is why every job
//! ever created sat in `queued` until its lifetime swept it.
//!
//! An upload and a generation are one job with one state machine, differing
//! only in where the first image comes from: an upload already has its bytes
//! in the store, a generation has to ask a model for them. After that they run
//! the same path — shape, measure, ladder, store, record — because they have
//! the same failure modes and deserve the same report.
//!
//! It runs inside the server process, which section 20 permits and section 19
//! did not: what that section refused was a *subprocess* and a torch image, for
//! the LaMa repair it then went on to drop. What is left is HTTP calls and
//! image arithmetic on a worker task, and giving it its own container would be
//! ceremony rather than isolation.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::db::Database;
use crate::generation::{
    Budget, FailureKind, GenerationJob, ImageProvider, JobLedger, JobState, ProviderOutcome,
};
use crate::texture::{ProposedTexture, SeamReport, Texture, TextureVariant};
use crate::texture_pixels::{self, Shaped};
use crate::texture_store::{TextureObject, TextureStore, digest};

/// How long to wait when the queue is empty.
///
/// Long enough that an idle deployment is not polling DynamoDB in a tight
/// loop, short enough that a person who just pressed Generate does not watch a
/// spinner for no reason.
const IDLE_PAUSE: Duration = Duration::from_secs(2);

/// Everything the loop needs, so it can be built in a test without a network.
pub struct Worker {
    pub db: Arc<dyn Database>,
    pub store: Arc<dyn TextureStore>,
    pub providers: Vec<Box<dyn ImageProvider>>,
    pub budget: Budget,
    /// Names this worker in the claim, so a stuck job says who had it.
    pub name: String,
}

impl Worker {
    /// Take one job if there is one. Returns whether anything was done.
    pub async fn tick(&self, now_ms: i64) -> anyhow::Result<bool> {
        let Some(job) = self.db.claim_generation_job(&self.name, now_ms).await? else {
            return Ok(false);
        };
        info!(job = %job.job_id, kind = %job.kind.as_str(), "claimed a texture job");

        // `run` works on the job in place, so what it spent along the way is
        // still on the value written at the end. Passing a clone and writing
        // the original lost every provider call the job made — the spend
        // reached the progress rows and was then overwritten by a final write
        // that still said zero, which is the daily circuit breaker's only
        // input.
        let mut finished = job;
        let outcome = self.run(&mut finished, now_ms).await;
        match outcome {
            Ok(texture) => {
                finished.state = JobState::Done;
                finished.texture_id = Some(texture.texture_id);
                finished.failure = None;
                finished.detail = None;
            }
            Err(failure) => {
                warn!(job = %finished.job_id, detail = %failure.detail, "texture job failed");
                finished.state = JobState::Failed;
                finished.failure = Some(failure.kind);
                finished.detail = Some(failure.detail);
            }
        }
        finished.updated_at_ms = chrono::Utc::now().timestamp_millis();
        self.db.update_generation_job(&finished).await?;
        Ok(true)
    }

    /// Run until told to stop.
    pub async fn run_forever(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let now = chrono::Utc::now().timestamp_millis();
            let worked = match self.tick(now).await {
                Ok(worked) => worked,
                Err(error) => {
                    // A failure to *talk to storage* is not a job failure; the
                    // job stays claimed and its lease will hand it on.
                    warn!(error = %error, "texture worker tick failed");
                    false
                }
            };
            if !worked {
                tokio::select! {
                    _ = tokio::time::sleep(IDLE_PAUSE) => {}
                    _ = shutdown.changed() => return,
                }
            }
        }
    }

    /// Take a job from claimed to a stored texture, in place.
    async fn run(&self, job: &mut GenerationJob, now_ms: i64) -> Result<Texture, Failure> {
        let source = match job.source_ref.clone() {
            // An upload: the bytes are already in the store.
            Some(reference) => {
                self.mark(job, JobState::Validating, now_ms).await;
                self.store
                    .get(&reference)
                    .await
                    .map_err(|error| Failure::pipeline(format!("source unreadable: {error}")))?
                    .ok_or_else(|| Failure::pipeline("the uploaded image is gone"))?
            }
            // A generation: ask, with whatever references came with it.
            None => {
                self.mark(job, JobState::Generating, now_ms).await;
                self.generate(job).await?
            }
        };

        self.mark(job, JobState::Validating, now_ms).await;
        let shaped = self.shape(&source, job)?;
        self.store_texture(job, shaped).await
    }

    /// Ask the providers in turn until one answers with an image.
    async fn generate(&self, job: &mut GenerationJob) -> Result<Vec<u8>, Failure> {
        if self.providers.is_empty() {
            return Err(Failure::new(
                FailureKind::PipelineHalted,
                "no image provider is configured",
            ));
        }

        // References are fetched once, not per provider: they are the same
        // bytes whichever model is asked.
        let mut references = Vec::new();
        for reference in &job.reference_refs {
            if let Ok(Some(bytes)) = self.store.get(reference).await {
                references.push(bytes);
            }
        }

        let (width, height) = texture_pixels::canonical_size(job.kind, job.rows_hint());
        let mut ledger = JobLedger::resume(self.budget, job.spend);
        let mut last = Failure::new(FailureKind::ProviderUnavailable, "no provider answered");

        for provider in &self.providers {
            if !ledger.may_call() {
                return Err(Failure::new(
                    FailureKind::BudgetExhausted,
                    "the job's call budget is spent",
                ));
            }
            match provider
                .generate(&job.prompt, width, height, &references)
                .await
            {
                ProviderOutcome::Image { png, usd_micros } => {
                    ledger.record_attempt(usd_micros);
                    job.spend = ledger.spend();
                    return Ok(png);
                }
                ProviderOutcome::Refused { reason } => {
                    ledger.record_attempt(0);
                    job.spend = ledger.spend();
                    // A refusal is a content judgement, and asking the next
                    // vendor the same question buys the same answer.
                    return Err(Failure::new(FailureKind::ProviderRefused, reason));
                }
                ProviderOutcome::Unavailable { detail } => {
                    ledger.record_attempt(0);
                    job.spend = ledger.spend();
                    last = Failure::new(
                        FailureKind::ProviderUnavailable,
                        format!("{}: {detail}", provider.name()),
                    );
                }
            }
        }
        Err(last)
    }

    /// Decode, shape and measure — the same path for both kinds of job.
    fn shape(&self, source: &[u8], job: &GenerationJob) -> Result<Shaped, Failure> {
        let pixels = texture_pixels::decode(source)
            .map_err(|error| Failure::new(FailureKind::ShapeRejected, error.problem))?;

        // Art that already arrives at the canonical size was meant that way,
        // so it is measured rather than reshaped.
        let (canonical_width, canonical_height) =
            texture_pixels::canonical_size(job.kind, job.rows_hint());
        let (width, height) = pixels.image.dimensions();
        let already_shaped =
            job.source_ref.is_some() && width == canonical_width && height == canonical_height;

        if already_shaped {
            crate::texture::validate_shape(ProposedTexture {
                kind: job.kind,
                width_px: width,
                height_px: height,
                rows: job.rows_hint(),
                byte_len: source.len(),
            })
            .map_err(|errors| {
                Failure::new(
                    FailureKind::ShapeRejected,
                    errors
                        .into_iter()
                        .map(|error| format!("{} {}", error.field, error.problem))
                        .collect::<Vec<_>>()
                        .join("; "),
                )
            })?;
        }

        let shaped = texture_pixels::shape(&pixels, job.kind, job.rows_hint(), already_shaped)
            .map_err(|error| Failure::new(FailureKind::ShapeRejected, error.problem))?;

        if !shaped.seams.passes(job.kind) {
            let (axis, measured) = if job.kind.tiles_along_body() {
                ("horizontal", shaped.seams.horizontal_ratio)
            } else {
                ("vertical", shaped.seams.vertical_ratio)
            };
            return Err(Failure::new(
                FailureKind::SeamsRejected,
                format!(
                    "the {axis} join stood out past {:.0}% of the texture's own steps, and \
                     {:.0}% is the limit",
                    measured * 100.0,
                    SeamReport::ACCEPTABLE_RATIO * 100.0
                ),
            ));
        }
        Ok(shaped)
    }

    /// Put every rung in the store and mint the row that names them.
    async fn store_texture(&self, job: &GenerationJob, shaped: Shaped) -> Result<Texture, Failure> {
        let mut variants = Vec::new();
        // The canonical rung first: its digest is the texture's own name, so a
        // document that references this texture references these exact pixels.
        for rung in std::iter::once(&shaped.canonical).chain(shaped.rungs.iter()) {
            let sha256 = digest(&rung.bytes);
            self.store
                .put(
                    &TextureObject {
                        sha256: sha256.clone(),
                        content_type: "image/png",
                        byte_len: rung.bytes.len(),
                    },
                    &rung.bytes,
                )
                .await
                .map_err(|error| {
                    Failure::pipeline(format!("could not store a variant: {error}"))
                })?;
            variants.push(TextureVariant {
                texels_per_cell: rung.texels_per_cell,
                width_px: rung.width_px,
                height_px: rung.height_px,
                bytes: rung.bytes.len() as u32,
                sha256,
            });
        }

        let texture_id = self
            .db
            .next_texture_id()
            .await
            .map_err(|error| Failure::pipeline(format!("could not mint an id: {error}")))?;

        let texture = Texture {
            texture_id,
            owner_user_id: job.owner_user_id,
            content_ref: format!("sha256:{}", variants[0].sha256),
            kind: job.kind,
            width_px: shaped.canonical.width_px,
            height_px: shaped.canonical.height_px,
            repeat_cells: crate::texture::repeat_cells(job.kind, shaped.canonical.width_px),
            rows: shaped.rows,
            seams: shaped.seams,
            // The author's words, not the engineered prompt around them.
            last_prompt: job.subject.clone(),
            variants,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };

        self.db
            .create_texture(&texture)
            .await
            .map_err(|error| Failure::pipeline(format!("could not record the texture: {error}")))
    }

    /// Write a stage so the Builder's poller shows real progress, and renew the
    /// lease while doing it. A failure to report is not a failure of the job.
    async fn mark(&self, job: &mut GenerationJob, state: JobState, now_ms: i64) {
        job.state = state;
        job.updated_at_ms = now_ms;
        job.lease_until_ms =
            Some(chrono::Utc::now().timestamp_millis() + crate::generation::LEASE_MS);
        if let Err(error) = self.db.update_generation_job(job).await {
            warn!(job = %job.job_id, error = %error, "could not report progress");
        }
    }
}

/// Why a job stopped, in the two parts the job record keeps.
struct Failure {
    kind: FailureKind,
    detail: String,
}

impl Failure {
    fn new(kind: FailureKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    fn pipeline(detail: impl Into<String>) -> Self {
        Self::new(FailureKind::PipelineHalted, detail)
    }
}
