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
        if let Some(reference) = job.source_ref.clone() {
            // An upload: the bytes are already in the store. It is never sent
            // to an image provider without an explicit author request.
            self.mark(job, JobState::Validating, now_ms).await;
            let source = self
                .store
                .get(&reference)
                .await
                .map_err(|error| Failure::pipeline(format!("source unreadable: {error}")))?
                .ok_or_else(|| Failure::pipeline("the uploaded image is gone"))?;
            let shaped = self.shape(&source, job)?;
            return self.store_texture(job, shaped).await;
        }

        // A provider result that fails the seam/post-process gate is feedback,
        // not an immediate terminal state. Regenerate inside the same durable
        // job and the same call/attempt ceilings; the persisted spend means a
        // worker restart cannot reset the allowance.
        let original_prompt = job.prompt.clone();
        let mut last_rejection: Option<String> = None;
        for attempt in 0..self.budget.max_attempts_per_texture {
            if let Some(rejection) = &last_rejection {
                job.prompt = format!(
                    "{original_prompt}\n\nThe previous candidate failed the exact shipped-byte gate: \
                     {rejection}. Generate a materially different candidate with invisible wrap joins."
                );
                self.mark(job, JobState::Repairing, now_ms).await;
            }
            self.mark(job, JobState::Generating, now_ms).await;
            let source = self.generate(job).await?;
            self.mark(job, JobState::Validating, now_ms).await;
            let result = match self.shape(&source, job) {
                Ok(shaped) => self.store_texture(job, shaped).await,
                Err(rejection) => Err(rejection),
            };
            match result {
                Ok(texture) => return Ok(texture),
                Err(rejection)
                    if rejection.kind == FailureKind::SeamsRejected
                        && attempt + 1 < self.budget.max_attempts_per_texture =>
                {
                    // This includes a lower ladder rung rejected after the
                    // store/read-back gate, not just the canonical shaping
                    // pass. Partial objects are harmless because their names
                    // are their hashes and no metadata row exists yet.
                    last_rejection = Some(rejection.detail);
                }
                Err(rejection) => return Err(rejection),
            }
        }
        Err(Failure::new(
            FailureKind::SeamsRejected,
            last_rejection.unwrap_or_else(|| "the bounded repair path was exhausted".to_string()),
        ))
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
        let mut verified_seams = shaped.seams;
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

            // Gate the object after storage, because these are the bytes the
            // anonymous route will actually return. A successful encoder or
            // PUT is not evidence that storage retained the same complete PNG.
            let stored = self
                .store
                .get(&sha256)
                .await
                .map_err(|error| {
                    Failure::pipeline(format!("could not verify a stored variant: {error}"))
                })?
                .ok_or_else(|| Failure::pipeline("a stored variant disappeared before verify"))?;
            if stored.len() != rung.bytes.len() || digest(&stored) != sha256 {
                return Err(Failure::pipeline(
                    "stored variant bytes do not match their strict manifest entry",
                ));
            }
            let decoded = texture_pixels::decode(&stored).map_err(|error| {
                Failure::new(
                    FailureKind::ShapeRejected,
                    format!("stored variant {} {}", error.field, error.problem),
                )
            })?;
            if decoded.image.dimensions() != (rung.width_px, rung.height_px) {
                return Err(Failure::new(
                    FailureKind::ShapeRejected,
                    "stored variant dimensions differ from their strict manifest entry",
                ));
            }
            let stored_seams = texture_pixels::seam_report(&decoded);
            if !stored_seams.passes(job.kind) {
                return Err(Failure::new(
                    FailureKind::SeamsRejected,
                    "a stored ladder variant failed a required wrap axis",
                ));
            }
            verified_seams.horizontal_ratio = verified_seams
                .horizontal_ratio
                .max(stored_seams.horizontal_ratio);
            verified_seams.vertical_ratio = verified_seams
                .vertical_ratio
                .max(stored_seams.vertical_ratio);
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
            shareable: false,
            content_ref: format!("sha256:{}", variants[0].sha256),
            kind: job.kind,
            width_px: shaped.canonical.width_px,
            height_px: shaped.canonical.height_px,
            repeat_cells: crate::texture::repeat_cells(job.kind, shaped.canonical.width_px),
            rows: shaped.rows,
            seams: verified_seams,
            verified_seam_axes: job.kind.worker_seam_axes().to_vec(),
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
