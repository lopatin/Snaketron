//! Supervision for hosted services.
//!
//! One supervisor task per registered factory. It owns the service's whole
//! lifecycle: acquiring an exclusion lease when one is declared, building an
//! instance, running it under a child cancellation token, renewing the lease,
//! and applying the failure policy when the instance stops.
//!
//! The two properties worth stating explicitly, because both are easy to get
//! subtly wrong:
//!
//! - **Fail-closed.** The child token is cancelled as soon as the lease is not
//!   provably held for another operation window, measured from the last
//!   *confirmed* renewal. A supervisor that trusted wall-clock time would let a
//!   paused process keep working past its expiry.
//! - **The host outlives its plugins.** A failing service is restarted with
//!   backoff and eventually disabled; it never takes the game server down
//!   unless it explicitly opted into `FailurePolicy::FailHost`.

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use snaketron_service_api::{
    FailurePolicy, HostedServiceFactory, LeaseHandle, ServiceContext, ServiceError,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::lease::ExclusionLeaseStore;

/// How often a lease is renewed relative to its TTL. A third leaves room for
/// two consecutive failed renewals before the fail-closed deadline.
///
/// Module-visible because the DynamoDB store derives its minimum global lease
/// term from it: the clock-skew allowance is only a margin while it stays
/// narrower than one renewal interval, and a second copy of the cadence there
/// could drift out of step with this one unnoticed.
pub(super) const RENEW_DIVISOR: u32 = 3;
/// How long to wait before re-attempting acquisition of a contended lease.
const ACQUIRE_RETRY: Duration = Duration::from_millis(500);
const BACKOFF_BASE: Duration = Duration::from_millis(250);
const BACKOFF_CEILING: Duration = Duration::from_secs(30);

/// Everything the supervisor needs that is not the factory itself.
pub struct SupervisorConfig {
    pub context: ServiceContext,
    pub region_leases: Option<ExclusionLeaseStore>,
    pub global_leases: Option<ExclusionLeaseStore>,
    pub holder_id: String,
    /// Per-service ceiling on how long a stop may take. Clamped by the host's
    /// remaining global deadline at shutdown, so one slow service delays only
    /// itself.
    pub stop_budget: Duration,
}

fn jittered_backoff(consecutive: u32) -> Duration {
    let exponent = consecutive.min(10);
    let base = BACKOFF_BASE.saturating_mul(1u32 << exponent);
    let capped = base.min(BACKOFF_CEILING);
    // Full jitter: a fleet recovering from a shared dependency outage must not
    // retry in lockstep.
    let millis = rand::thread_rng().gen_range(0..=capped.as_millis().max(1) as u64);
    Duration::from_millis(millis)
}

/// Spawns the supervisor. The returned handle is retained and awaited by the
/// host: dropping it would keep the task running but lose every `JoinError`,
/// so the supervisor could no longer restart, count, or attribute a failure.
pub fn spawn_supervisor(
    factory: Arc<dyn HostedServiceFactory>,
    config: SupervisorConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let name = factory.name();
        if let Err(error) = supervise(factory, config, cancel).await {
            error!("hosted service {name} supervisor exited: {error}");
        }
    })
}

async fn supervise(
    factory: Arc<dyn HostedServiceFactory>,
    config: SupervisorConfig,
    cancel: CancellationToken,
) -> Result<(), ServiceError> {
    let name = factory.name();
    let policy = factory.failure_policy();
    let mut consecutive: u32 = 0;

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }

        match run_once(&factory, &config, &cancel).await {
            Outcome::Cancelled => return Ok(()),
            Outcome::LeaseUnavailable => {
                // Someone else holds it. This is the steady state for every
                // task that is not the elected one, so it must be quiet.
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(ACQUIRE_RETRY) => continue,
                }
            }
            Outcome::LeaseLost => {
                info!("hosted service {name} lost its exclusion lease; standing down");
                consecutive = 0;
            }
            Outcome::Completed => {
                // Returning Ok before cancellation is an unexpected exit: a
                // service that is genuinely finished should park on the token.
                warn!("hosted service {name} returned before cancellation");
                consecutive = consecutive.saturating_add(1);
            }
            Outcome::Failed(error) => {
                consecutive = consecutive.saturating_add(1);
                warn!("hosted service {name} failed (attempt {consecutive}): {error}");
                match policy {
                    FailurePolicy::FailHost => return Err(error),
                    FailurePolicy::Disable => {
                        error!("hosted service {name} disabled after failure");
                        return Ok(());
                    }
                    FailurePolicy::Restart { max_consecutive }
                        if consecutive >= max_consecutive =>
                    {
                        error!(
                            "hosted service {name} disabled after {consecutive} consecutive failures; \
                             the host is unaffected"
                        );
                        return Ok(());
                    }
                    FailurePolicy::Restart { .. } => {}
                }
            }
        }

        let delay = jittered_backoff(consecutive);
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

enum Outcome {
    Cancelled,
    Completed,
    Failed(ServiceError),
    LeaseUnavailable,
    LeaseLost,
}

async fn run_once(
    factory: &Arc<dyn HostedServiceFactory>,
    config: &SupervisorConfig,
    cancel: &CancellationToken,
) -> Outcome {
    let key = factory.exclusion_key(&config.context);

    // `_elected` is bound, not discarded: it must live to the end of this
    // function so every early return below still clears the signal.
    let (lease, store, _elected) = match key {
        None => (None, None, None),
        Some(key) => {
            let store = match key.domain {
                snaketron_service_api::ExclusionDomain::Region => config.region_leases.clone(),
                snaketron_service_api::ExclusionDomain::Global => config.global_leases.clone(),
            };
            let Some(store) = store else {
                return Outcome::Failed(ServiceError::MissingDependency(format!(
                    "no lease store configured for {key}"
                )));
            };
            // Before the attempt, so the tasks that lose the election are also
            // observable — they are the baseline the elected task is compared
            // against.
            // Before the attempt, so the tasks that lose the election are also
            // observable — they are the baseline the elected task is compared
            // against.
            crate::resilience_metrics::record_hosted_service_contention(factory.name());
            match store.try_acquire(&key, &config.holder_id).await {
                Err(error) => return Outcome::Failed(error),
                Ok(None) => return Outcome::LeaseUnavailable,
                Ok(Some(lease)) => (
                    Some(lease),
                    Some(store),
                    Some(crate::resilience_metrics::record_hosted_service_election(
                        factory.name(),
                    )),
                ),
            }
        }
    };

    let mut ctx = config.context.clone();
    ctx.lease = lease.clone();

    let service = match factory.build(ctx).await {
        Ok(service) => service,
        Err(error) => {
            if let (Some(store), Some(lease)) = (store.as_ref(), lease.as_ref()) {
                let _ = store.release(lease).await;
            }
            return Outcome::Failed(error);
        }
    };

    // A child token so losing the lease cancels only this service.
    let child = cancel.child_token();
    let outcome = match (lease.clone(), store.clone()) {
        (Some(lease), Some(store)) => {
            run_with_lease(
                factory.name(),
                service,
                lease.clone(),
                store,
                &child,
                cancel,
            )
            .await
        }
        _ => run_plain(service, &child, cancel).await,
    };

    if let (Some(store), Some(lease)) = (store.as_ref(), lease.as_ref())
        && let Err(error) = store.release(lease).await
    {
        warn!(
            "hosted service {} failed to release its lease: {error}",
            factory.name()
        );
    }
    outcome
}

async fn run_plain(
    mut service: Box<dyn snaketron_service_api::HostedService>,
    child: &CancellationToken,
    parent: &CancellationToken,
) -> Outcome {
    let result = service.run(child.clone()).await;
    classify(result, parent)
}

async fn run_with_lease(
    name: &'static str,
    mut service: Box<dyn snaketron_service_api::HostedService>,
    lease: LeaseHandle,
    store: ExclusionLeaseStore,
    child: &CancellationToken,
    parent: &CancellationToken,
) -> Outcome {
    let renew_interval = store.ttl() / RENEW_DIVISOR;
    let operation_timeout = store.operation_timeout();
    let renewer_lease = lease.clone();
    let renewer_token = child.clone();
    let renewer = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(renew_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = renewer_token.cancelled() => return false,
                _ = ticker.tick() => {}
            }
            match store.renew(&renewer_lease).await {
                Ok(true) => {}
                Ok(false) => {
                    // Someone else holds it now.
                    renewer_token.cancel();
                    return true;
                }
                Err(error) => {
                    warn!("hosted service {name} lease renewal error: {error}");
                }
            }
            // Fail closed: stop the service before the lease could expire,
            // measured from the last CONFIRMED renewal rather than from now.
            if !renewer_lease.is_held_for(operation_timeout) {
                warn!("hosted service {name} lease not provably held; standing down");
                renewer_token.cancel();
                return true;
            }
        }
    });

    let result = service.run(child.clone()).await;
    child.cancel();
    let lost = renewer.await.unwrap_or(false);

    if lost && !parent.is_cancelled() {
        return Outcome::LeaseLost;
    }
    classify(result, parent)
}

fn classify(result: Result<(), ServiceError>, parent: &CancellationToken) -> Outcome {
    match result {
        Ok(()) if parent.is_cancelled() => Outcome::Cancelled,
        Ok(()) => Outcome::Completed,
        Err(error) => Outcome::Failed(error),
    }
}
