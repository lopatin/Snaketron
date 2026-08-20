//! Asking a model for a texture, and paying for it.
//!
//! Generation is a job rather than a request because it is slow, it can fail
//! in ways worth showing a person, and it costs money per attempt. The job
//! record is the whole state: what was asked for, what stage it reached, what
//! it cost, and — when it failed — which gate refused it and by how much, so
//! "try again" is an informed decision rather than a coin toss.
//!
//! Three ceilings, all enforced here rather than alarmed on afterwards:
//!
//! - a **per-job call budget**, because one prompt can otherwise fan out into
//!   several textures times several repair attempts times a replan;
//! - a **per-plan image budget**, because a generated skin's layer count is
//!   chosen by a model and a twelve-layer plan is twelve generations;
//! - a **global daily spend** that halts the pipeline rather than paging
//!   somebody after the money is gone.
//!
//! An alarm tells you it happened. A ceiling means it did not.

use serde::{Deserialize, Serialize};

use crate::texture::TextureKind;

/// Where a job has got to.
///
/// A closed sequence, so a client polling it can show a real stage rather than
/// a spinner: the stages are what actually takes the time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum JobState {
    Queued,
    /// Waiting on the image model.
    Generating,
    /// Moving the joins and inpainting them.
    Repairing,
    /// Measuring the result against the gates.
    Validating,
    Done,
    Failed,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Generating => "generating",
            Self::Repairing => "repairing",
            Self::Validating => "validating",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "generating" => Some(Self::Generating),
            "repairing" => Some(Self::Repairing),
            "validating" => Some(Self::Validating),
            "done" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Whether the job is finished, either way.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed)
    }
}

/// Why a job stopped without producing anything.
///
/// Named rather than collapsed into a string, because the client shows a
/// different thing for each and because the counters want to tell them apart:
/// a wave of refusals is a moderation signal, a wave of seam failures is a
/// prompt-quality one, and a budget stop is a capacity one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum FailureKind {
    /// The provider declined the prompt. Not retried — retrying a refusal is
    /// how a refusal becomes a bill.
    ProviderRefused,
    /// The provider errored or timed out.
    ProviderUnavailable,
    /// The image came back structurally wrong for its kind.
    ShapeRejected,
    /// The joins could not be brought inside the gate.
    SeamsRejected,
    /// This job used its allowance.
    BudgetExhausted,
    /// The pipeline is halted.
    PipelineHalted,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderRefused => "providerRefused",
            Self::ProviderUnavailable => "providerUnavailable",
            Self::ShapeRejected => "shapeRejected",
            Self::SeamsRejected => "seamsRejected",
            Self::BudgetExhausted => "budgetExhausted",
            Self::PipelineHalted => "pipelineHalted",
        }
    }

    /// Whether another attempt could plausibly do better.
    ///
    /// A refusal cannot: the prompt is the problem and it has not changed. A
    /// seam failure can, because the retry carries the measurements back to the
    /// model.
    pub fn is_worth_retrying(self) -> bool {
        matches!(self, Self::SeamsRejected | Self::ProviderUnavailable)
    }
}

/// The ceilings a job runs inside.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Budget {
    /// Provider calls this job may make, across every texture and retry.
    pub max_provider_calls: u32,
    /// Image-bearing layers one generated skin plan may ask for.
    pub max_images_per_plan: u32,
    /// Attempts at one texture before it is given up on.
    pub max_attempts_per_texture: u32,
    /// Replans allowed when the validator refuses an assembled document.
    pub max_replans: u32,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_provider_calls: 10,
            max_images_per_plan: 4,
            max_attempts_per_texture: 3,
            max_replans: 2,
        }
    }
}

/// Running cost, in provider calls and money.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Spend {
    pub provider_calls: u32,
    pub usd_micros: u64,
}

impl Spend {
    pub fn add_call(&mut self, usd_micros: u64) {
        self.provider_calls += 1;
        self.usd_micros += usd_micros;
    }
}

/// One generation job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GenerationJob {
    pub job_id: String,
    pub owner_user_id: i32,
    pub kind: TextureKind,
    pub prompt: String,
    pub state: JobState,
    pub spend: Spend,
    /// Set when the job produced a texture.
    pub texture_id: Option<i32>,
    /// Set when it did not.
    pub failure: Option<FailureKind>,
    /// What the failure actually was, in words, including measurements.
    pub detail: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// When this job's claim goes stale, if it is claimed.
    ///
    /// A worker that dies mid-job would otherwise strand it forever: the state
    /// says `generating` and nothing will ever say otherwise, so the client
    /// polls a value that cannot change and the work is never redone. The
    /// lease is what makes a dead worker's job re-claimable, and it lives on
    /// the job rather than being set once at claim time because an update
    /// rewrites the whole item — a lease written only by the claim would be
    /// erased by the first progress write, which is the same shape as the bug
    /// that once made the job's lifetime disappear.
    ///
    /// A worker renews it by writing progress; see [`LEASE_MS`].
    #[serde(default)]
    pub lease_until_ms: Option<i64>,
}

/// How long a claim holds before another worker may take the job.
///
/// Long enough that a slow image model is not treated as a crash — the
/// provider timeout plus the repair attempts it is allowed — and short enough
/// that a real crash is not a job lost for the rest of the day.
pub const LEASE_MS: i64 = 5 * 60 * 1000;

impl GenerationJob {
    /// Whether a claim on this job has lapsed, so another worker may take it.
    ///
    /// A job with no lease at all counts as lapsed: that is either a job
    /// written before leases existed or one whose claim never recorded one,
    /// and in both cases leaving it unclaimable forever is the failure the
    /// lease exists to prevent.
    pub fn claim_is_stale(&self, now_ms: i64) -> bool {
        !self.state.is_terminal() && self.lease_until_ms.is_none_or(|until| until <= now_ms)
    }
}

/// What an image provider returns.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderOutcome {
    Image {
        png: Vec<u8>,
        usd_micros: u64,
    },
    /// The provider declined on content grounds.
    Refused {
        reason: String,
    },
    Unavailable {
        detail: String,
    },
}

/// An image model.
///
/// A trait so the pipeline can be exercised end to end without a network or a
/// key: the tests below drive it through a provider that answers from a script,
/// which is the only way to assert the budget behaviour without spending money
/// to find out.
#[async_trait::async_trait]
pub trait ImageProvider: Send + Sync {
    /// A short name for logs and job records.
    fn name(&self) -> &'static str;

    async fn generate(
        &self,
        prompt: &str,
        width: u32,
        height: u32,
        references: &[Vec<u8>],
    ) -> ProviderOutcome;
}

/// Whether the pipeline may start another job at all.
///
/// The daily ceiling is a circuit breaker rather than an alert: crossing it
/// stops work instead of describing it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DailySpend {
    pub usd_micros: u64,
    pub ceiling_usd_micros: u64,
}

impl DailySpend {
    pub fn is_halted(&self) -> bool {
        self.usd_micros >= self.ceiling_usd_micros
    }
}

/// A job's own accounting, as it runs.
#[derive(Debug, Clone)]
pub struct JobLedger {
    budget: Budget,
    spend: Spend,
    attempts: u32,
}

impl JobLedger {
    pub fn new(budget: Budget) -> Self {
        Self::resume(budget, Spend::default())
    }

    /// Carry on from what a job has already spent.
    ///
    /// The distinction is not cosmetic. A job is persisted with its spend, and
    /// a worker that crashed mid-flight has its job re-claimed later — so a
    /// ledger that always starts at zero turns a per-job ceiling into a
    /// per-*attempt-of-the-job* ceiling, which is precisely the unbounded
    /// fan-out the budget exists to stop. Every resumption has to inherit what
    /// was already paid.
    pub fn resume(budget: Budget, spent: Spend) -> Self {
        Self {
            budget,
            spend: spent,
            attempts: 0,
        }
    }

    pub fn spend(&self) -> Spend {
        self.spend
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Whether another provider call is allowed.
    ///
    /// Checked *before* the call, so the ceiling is a ceiling rather than a
    /// description of what already happened.
    pub fn may_call(&self) -> bool {
        self.spend.provider_calls < self.budget.max_provider_calls
            && self.attempts < self.budget.max_attempts_per_texture
    }

    pub fn record_attempt(&mut self, usd_micros: u64) {
        self.attempts += 1;
        self.spend.add_call(usd_micros);
    }

    /// Start a fresh texture within the same job.
    ///
    /// `attempts` is per texture and the call count is per job, so a plan
    /// producing several textures has to reset one and keep the other.
    /// Sharing a single counter would stop a four-texture plan after three
    /// attempts *in total* rather than three per texture.
    pub fn next_texture(&mut self) {
        self.attempts = 0;
    }

    /// Why the job cannot continue, if it cannot.
    pub fn exhaustion(&self) -> Option<FailureKind> {
        if self.may_call() {
            None
        } else {
            Some(FailureKind::BudgetExhausted)
        }
    }
}

/// Fold the measurements of a failed attempt back into the next prompt.
///
/// A retry with the identical prompt is a retry with the identical odds. The
/// local tooling's own philosophy is that a structurally disagreeing source is
/// re-generated rather than force-repaired, and this is what makes the
/// re-generation better informed than the first try.
pub fn retry_prompt(base: &str, failing_axis: &str, ratio: f32) -> String {
    format!(
        "{base}\n\nThe previous attempt did not tile: the {failing_axis} join was {ratio:.2}× \
         more different than the surrounding texture, and the limit is {:.2}×. Make the \
         {failing_axis} edges continue into each other exactly — the same shapes and the same \
         brightness on both sides of the join.",
        crate::texture::SeamReport::ACCEPTABLE_RATIO
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScriptedProvider {
        script: std::sync::Mutex<Vec<ProviderOutcome>>,
        calls: std::sync::atomic::AtomicU32,
    }

    impl ScriptedProvider {
        fn new(script: Vec<ProviderOutcome>) -> Self {
            Self {
                script: std::sync::Mutex::new(script),
                calls: std::sync::atomic::AtomicU32::new(0),
            }
        }

        fn calls(&self) -> u32 {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl ImageProvider for ScriptedProvider {
        fn name(&self) -> &'static str {
            "scripted"
        }

        async fn generate(
            &self,
            _prompt: &str,
            _width: u32,
            _height: u32,
            _references: &[Vec<u8>],
        ) -> ProviderOutcome {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut script = self.script.lock().expect("script lock");
            if script.is_empty() {
                ProviderOutcome::Unavailable {
                    detail: "the script ran out".to_string(),
                }
            } else {
                script.remove(0)
            }
        }
    }

    /// Drive one texture through the pipeline's retry rule.
    async fn run(provider: &ScriptedProvider, budget: Budget, seams_pass_on: u32) -> JobLedger {
        let mut ledger = JobLedger::new(budget);
        let mut round = 0;
        while ledger.may_call() {
            round += 1;
            match provider.generate("prompt", 320, 320, &[]).await {
                ProviderOutcome::Image { usd_micros, .. } => {
                    ledger.record_attempt(usd_micros);
                    if round >= seams_pass_on {
                        break;
                    }
                }
                // A refusal ends the job: the prompt is the problem and it has
                // not changed.
                ProviderOutcome::Refused { .. } => {
                    ledger.record_attempt(0);
                    break;
                }
                ProviderOutcome::Unavailable { .. } => ledger.record_attempt(0),
            }
        }
        ledger
    }

    #[tokio::test]
    async fn a_texture_that_needs_two_tries_costs_two_calls() {
        let provider = ScriptedProvider::new(vec![
            ProviderOutcome::Image {
                png: vec![1],
                usd_micros: 40_000,
            },
            ProviderOutcome::Image {
                png: vec![2],
                usd_micros: 40_000,
            },
        ]);
        let ledger = run(&provider, Budget::default(), 2).await;

        assert_eq!(provider.calls(), 2);
        assert_eq!(ledger.spend().provider_calls, 2);
        assert_eq!(ledger.spend().usd_micros, 80_000);
        assert_eq!(ledger.exhaustion(), None, "two of three attempts used");
    }

    /// The ceiling has to stop the work, not describe it afterwards: a texture
    /// that never satisfies the gates must not generate forever.
    #[tokio::test]
    async fn a_texture_that_never_passes_stops_at_its_attempt_ceiling() {
        let provider = ScriptedProvider::new(vec![
            ProviderOutcome::Image {
                png: vec![1],
                usd_micros: 40_000,
            },
            ProviderOutcome::Image {
                png: vec![2],
                usd_micros: 40_000,
            },
            ProviderOutcome::Image {
                png: vec![3],
                usd_micros: 40_000,
            },
            ProviderOutcome::Image {
                png: vec![4],
                usd_micros: 40_000,
            },
        ]);
        // `seams_pass_on` beyond the ceiling: nothing ever satisfies it.
        let ledger = run(&provider, Budget::default(), 99).await;

        assert_eq!(provider.calls(), 3, "three attempts, not four");
        assert_eq!(ledger.exhaustion(), Some(FailureKind::BudgetExhausted));
    }

    /// The whole-job budget binds even when per-texture attempts would allow
    /// more, which is what stops a many-texture plan from multiplying out.
    #[tokio::test]
    async fn the_job_wide_call_budget_binds_before_the_per_texture_one() {
        let budget = Budget {
            max_provider_calls: 2,
            max_attempts_per_texture: 10,
            ..Budget::default()
        };
        let provider = ScriptedProvider::new(vec![
            ProviderOutcome::Image {
                png: vec![1],
                usd_micros: 1,
            },
            ProviderOutcome::Image {
                png: vec![2],
                usd_micros: 1,
            },
            ProviderOutcome::Image {
                png: vec![3],
                usd_micros: 1,
            },
        ]);
        let ledger = run(&provider, budget, 99).await;

        assert_eq!(provider.calls(), 2);
        assert_eq!(ledger.exhaustion(), Some(FailureKind::BudgetExhausted));
    }

    /// Retrying a refusal is how a refusal becomes a bill.
    #[tokio::test]
    async fn a_refusal_is_not_retried() {
        let provider = ScriptedProvider::new(vec![
            ProviderOutcome::Refused {
                reason: "policy".to_string(),
            },
            ProviderOutcome::Image {
                png: vec![1],
                usd_micros: 40_000,
            },
        ]);
        let ledger = run(&provider, Budget::default(), 1).await;

        assert_eq!(
            provider.calls(),
            1,
            "the second script entry is never reached"
        );
        assert_eq!(ledger.attempts(), 1);
        assert!(!FailureKind::ProviderRefused.is_worth_retrying());
        assert!(FailureKind::SeamsRejected.is_worth_retrying());
    }

    #[test]
    fn the_daily_ceiling_halts_rather_than_reports() {
        let under = DailySpend {
            usd_micros: 900,
            ceiling_usd_micros: 1_000,
        };
        let at = DailySpend {
            usd_micros: 1_000,
            ceiling_usd_micros: 1_000,
        };
        assert!(!under.is_halted());
        assert!(at.is_halted(), "at the ceiling is over it");
    }

    /// A retry that carries no information is a retry with the same odds.
    #[test]
    fn a_retry_prompt_tells_the_model_what_went_wrong() {
        let retry = retry_prompt("Create a seamless leopard print.", "horizontal", 2.4);
        assert!(retry.starts_with("Create a seamless leopard print."));
        assert!(retry.contains("horizontal"));
        assert!(retry.contains("2.40"));
        assert!(retry.contains("1.50"), "and what the limit was");
    }

    #[test]
    fn states_and_failures_round_trip_and_know_when_they_are_final() {
        for state in [
            JobState::Queued,
            JobState::Generating,
            JobState::Repairing,
            JobState::Validating,
            JobState::Done,
            JobState::Failed,
        ] {
            assert_eq!(JobState::parse(state.as_str()), Some(state));
        }
        assert!(JobState::Done.is_terminal());
        assert!(JobState::Failed.is_terminal());
        assert!(!JobState::Repairing.is_terminal());
        assert_eq!(JobState::parse("thinking"), None);
    }

    /// A job resumed after a crash has to inherit what it already spent.
    ///
    /// Otherwise the per-job ceiling becomes a per-*attempt-of-the-job*
    /// ceiling: every re-claim hands back a full budget, and a job that keeps
    /// failing mid-flight bills without limit. That is the exact fan-out this
    /// module exists to bound, and it would happen quietly.
    #[test]
    fn a_resumed_job_carries_the_money_it_has_already_spent() {
        let budget = Budget::default();
        let mut fresh = JobLedger::new(budget);
        assert!(fresh.may_call());

        // Spend the whole per-job call allowance.
        for _ in 0..budget.max_provider_calls {
            fresh.record_attempt(40_000);
        }
        assert!(!fresh.may_call());
        let spent = fresh.spend();
        assert_eq!(spent.provider_calls, budget.max_provider_calls);

        // A worker picking the job back up must not get a second budget.
        let resumed = JobLedger::resume(budget, spent);
        assert!(
            !resumed.may_call(),
            "a re-claimed job was handed a fresh budget"
        );
        assert_eq!(resumed.spend().usd_micros, spent.usd_micros);
    }

    /// Attempts are per texture; calls are per job. A plan making several
    /// textures resets one and keeps the other, or it stops after three
    /// attempts in total rather than three per texture.
    #[test]
    fn a_new_texture_resets_its_attempts_but_not_the_jobs_bill() {
        let budget = Budget::default();
        let mut ledger = JobLedger::new(budget);
        for _ in 0..budget.max_attempts_per_texture {
            ledger.record_attempt(1_000);
        }
        assert!(!ledger.may_call(), "this texture is out of attempts");

        ledger.next_texture();
        assert!(ledger.may_call(), "the next texture gets its own attempts");
        assert_eq!(
            ledger.spend().provider_calls,
            budget.max_attempts_per_texture,
            "but the job's bill carries over"
        );
    }
}
