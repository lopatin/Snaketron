use anyhow::{Context, Result, anyhow};
use clap::Parser;
use common::{GameType, QueueMode};
use loadtest::config::{self, Args, Config, GameMode, Population};
use loadtest::report::{
    InfrastructureSample, LoadTestRun, RampStageRecord, SessionFailureRecord, SessionOutcome,
    SessionPhase, SessionRecord, aggregate_report, unix_time_ms, write_report,
};
use loadtest::session::{
    MatchGroupResult, MatchGroupSpec, SessionActivityEvent, SessionSettings,
    deterministic_username, run_match_group,
};
use loadtest::target::{TargetOptions, TargetResolver};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Barrier, mpsc};
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;

const OBSERVATION_INTERVAL: Duration = Duration::from_secs(5);
const SPAWN_INTERVAL: Duration = Duration::from_secs(1);
const CANCEL_GRACE: Duration = Duration::from_secs(10);
const FAILURE_CIRCUIT_BREAKER_MIN_SESSIONS: usize = 4;
const FAILURE_CIRCUIT_BREAKER_RATE: f64 = 0.20;

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionConcurrencyState {
    Pending,
    Connected,
    Terminal,
}

/// Tracks virtual users individually while match-group tasks remain the unit
/// of deterministic launch and result collection.
#[derive(Debug, Default)]
struct SessionConcurrencyTracker {
    states: HashMap<u64, SessionConcurrencyState>,
    pending: usize,
    connected: usize,
    peak_connected: usize,
}

impl SessionConcurrencyTracker {
    fn reserve(&mut self, session_indices: &[u64]) {
        for session_index in session_indices {
            if self.states.contains_key(session_index) {
                // Session indices are monotonic and must never be reused. Keep
                // release builds safe from double-counting if that invariant
                // is accidentally violated.
                warn!(session_index, "duplicate virtual-user reservation ignored");
            } else {
                self.states
                    .insert(*session_index, SessionConcurrencyState::Pending);
                self.pending = self.pending.saturating_add(1);
            }
        }
    }

    fn observe(&mut self, event: SessionActivityEvent) {
        let (session_index, next_state) = match event {
            SessionActivityEvent::Connected { session_index } => {
                (session_index, SessionConcurrencyState::Connected)
            }
            SessionActivityEvent::Terminal { session_index } => {
                (session_index, SessionConcurrencyState::Terminal)
            }
        };
        let Some(current_state) = self.states.get_mut(&session_index) else {
            warn!(
                session_index,
                "activity received for an unplanned virtual user"
            );
            return;
        };

        match (*current_state, next_state) {
            (SessionConcurrencyState::Pending, SessionConcurrencyState::Connected) => {
                self.pending = self.pending.saturating_sub(1);
                self.connected = self.connected.saturating_add(1);
                self.peak_connected = self.peak_connected.max(self.connected);
                *current_state = SessionConcurrencyState::Connected;
            }
            (SessionConcurrencyState::Pending, SessionConcurrencyState::Terminal) => {
                self.pending = self.pending.saturating_sub(1);
                *current_state = SessionConcurrencyState::Terminal;
            }
            (SessionConcurrencyState::Connected, SessionConcurrencyState::Terminal) => {
                self.connected = self.connected.saturating_sub(1);
                *current_state = SessionConcurrencyState::Terminal;
            }
            // Duplicate messages and a late Connected message drained after a
            // task result reconciled the VU are deliberately idempotent.
            _ => {}
        }
    }

    fn mark_terminal(&mut self, session_indices: &[u64]) {
        for session_index in session_indices {
            self.observe(SessionActivityEvent::Terminal {
                session_index: *session_index,
            });
        }
    }

    const fn pending(&self) -> usize {
        self.pending
    }

    const fn connected(&self) -> usize {
        self.connected
    }

    const fn peak_connected(&self) -> usize {
        self.peak_connected
    }

    fn reserved(&self) -> usize {
        self.pending.saturating_add(self.connected)
    }
}

fn observe_stage_activity(
    event: SessionActivityEvent,
    activity_rx: &mut mpsc::UnboundedReceiver<SessionActivityEvent>,
    concurrency: &mut SessionConcurrencyTracker,
    target: usize,
    target_reached: &mut bool,
    target_reached_at: &mut Option<u64>,
) {
    apply_stage_activity(
        event,
        concurrency,
        target,
        target_reached,
        target_reached_at,
    );
    drain_stage_activity_events(
        activity_rx,
        concurrency,
        target,
        target_reached,
        target_reached_at,
    );
}

fn drain_stage_activity_events(
    activity_rx: &mut mpsc::UnboundedReceiver<SessionActivityEvent>,
    concurrency: &mut SessionConcurrencyTracker,
    target: usize,
    target_reached: &mut bool,
    target_reached_at: &mut Option<u64>,
) {
    while let Ok(event) = activity_rx.try_recv() {
        apply_stage_activity(
            event,
            concurrency,
            target,
            target_reached,
            target_reached_at,
        );
    }
}

fn apply_stage_activity(
    event: SessionActivityEvent,
    concurrency: &mut SessionConcurrencyTracker,
    target: usize,
    target_reached: &mut bool,
    target_reached_at: &mut Option<u64>,
) {
    concurrency.observe(event);
    if !*target_reached && concurrency.connected() >= target {
        *target_reached = true;
        *target_reached_at = Some(unix_time_ms());
    }
}

fn drain_activity_events(
    activity_rx: &mut mpsc::UnboundedReceiver<SessionActivityEvent>,
    concurrency: &mut SessionConcurrencyTracker,
) {
    while let Ok(event) = activity_rx.try_recv() {
        concurrency.observe(event);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    let config = Args::parse().into_config()?;
    run(config).await
}

async fn run(config: Config) -> Result<()> {
    if config.is_production() {
        warn!(
            "production load test confirmed; synthetic guest users and completed matches may be persisted by the current server"
        );
    }
    let resolver = TargetResolver::new(config.connect_timeout)?;
    info!(target = %config.target_url, "preflighting load-test target with strict TLS");
    let preflight = resolver
        .preflight(&TargetOptions {
            target: config.target_url.to_string(),
            region: config.region.clone(),
            require_same_origin: config.require_same_origin,
        })
        .await
        .context("target preflight failed before any sessions were launched")?;

    let websocket_url = match &config.ws_url {
        Some(url) => url.clone(),
        None => Url::parse(&preflight.websocket_url)
            .context("preflight returned an invalid WebSocket URL")?,
    };
    let api_origin =
        Url::parse(&preflight.api_origin).context("preflight returned an invalid API origin")?;
    let selected_origin = Url::parse(&preflight.selected_origin)
        .context("preflight returned an invalid selected-region origin")?;
    if config.require_same_origin {
        ensure_effective_endpoints_same_origin(
            &config.target_url,
            &api_origin,
            &selected_origin,
            &websocket_url,
        )?;
    }
    ensure_effective_endpoints_confirmed(
        config.production_confirmed,
        &[&api_origin, &selected_origin, &websocket_url],
    )?;
    let settings = SessionSettings {
        api_origin,
        websocket_url,
        origin: selected_origin.to_string(),
        game_type: game_type(config.mode),
        queue_mode: queue_mode(config.queue_mode),
        selected_mode: config.mode.as_str().to_owned(),
        competitive: matches!(config.queue_mode, config::QueueMode::Competitive),
        population: config.population,
        connect_timeout: config.connect_timeout,
        lobby_timeout: config.lobby_timeout,
        queue_timeout: config.queue_timeout,
        untimed_play_duration: config.untimed_play_duration,
        command_profile: config.command_profile,
        backend_hints: resolver.backend_hints(),
    };

    info!(
        run_id = %config.run_id,
        region = %preflight.selected_region.id,
        websocket = %settings.websocket_url,
        max_sessions = config.max_concurrency(),
        mode = %config.mode,
        queue = %config.queue_mode,
        "starting coordinated load test"
    );

    let started_at = unix_time_ms();
    let mut run = LoadTestRun::new(
        config.run_id.clone(),
        config.target_url.to_string(),
        started_at,
        config.max_concurrency(),
    );
    run.metadata
        .insert("region".to_owned(), preflight.selected_region.id.clone());
    run.metadata.insert(
        "region_name".to_owned(),
        preflight.selected_region.name.clone(),
    );
    run.metadata.insert(
        "websocket_url".to_owned(),
        settings.websocket_url.to_string(),
    );
    run.metadata
        .insert("api_origin".to_owned(), settings.api_origin.to_string());
    run.metadata
        .insert("selected_origin".to_owned(), settings.origin.clone());
    run.metadata.insert(
        "require_same_origin".to_owned(),
        config.require_same_origin.to_string(),
    );
    run.metadata
        .insert("mode".to_owned(), config.mode.to_string());
    run.metadata
        .insert("population".to_owned(), config.population.to_string());
    run.metadata
        .insert("queue_mode".to_owned(), config.queue_mode.to_string());
    run.metadata.insert(
        "command_profile".to_owned(),
        config.command_profile.to_string(),
    );
    run.metadata.insert(
        "spawn_rate_per_second".to_owned(),
        config.spawn_rate.to_string(),
    );
    run.metadata.insert(
        "open_loop_admission".to_owned(),
        config.open_loop_admission.to_string(),
    );
    run.metadata.insert(
        "max_total_sessions".to_owned(),
        config.max_total_sessions.to_string(),
    );
    run.metadata.insert(
        "untimed_play_duration_ms".to_owned(),
        config.untimed_play_duration.as_millis().to_string(),
    );
    run.metadata.insert(
        "lobby_topology".to_owned(),
        if config.population == Population::Game && config.sessions_per_group() > 1 {
            format!(
                "one_full_party_lobby_per_game_{}_members",
                config.sessions_per_group()
            )
        } else if config.population == Population::Idle {
            "none_idle_authenticated_socket".to_owned()
        } else if config.population == Population::Matchmaking {
            "one_player_lobby_held_in_queue".to_owned()
        } else if config.population == Population::Lobby {
            "one_player_lobby_held_outside_queue".to_owned()
        } else {
            "one_player_lobby_per_game".to_owned()
        },
    );
    run.metadata.insert(
        "production_confirmation".to_owned(),
        config.production_confirmed.to_string(),
    );
    run.infrastructure_samples.push(InfrastructureSample {
        observed_at_unix_ms: preflight
            .initial_user_counts
            .endpoint
            .observed_at_unix_ms
            .max(preflight.initial_server_counts.endpoint.observed_at_unix_ms),
        regional_user_counts: preflight.initial_user_counts.counts.clone(),
        regional_server_counts: preflight.initial_server_counts.counts.clone(),
        observed_backend_hints: resolver.backend_hints().observed_backend_count(),
        error: None,
    });

    let cancellation = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let mut planned_groups = HashMap::new();
    let (activity_tx, mut activity_rx) = mpsc::unbounded_channel();
    let mut concurrency = SessionConcurrencyTracker::default();
    let mut total_sessions_launched = 0usize;
    let mut next_session_index = 1u64;
    let mut next_group_index = 1u64;
    let mut next_wave_index = 0u32;
    let mut interrupted = false;
    let mut signal = Box::pin(shutdown_signal());
    let mut observation_interval = interval(OBSERVATION_INTERVAL);
    observation_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut spawn_interval = interval_at(Instant::now() + SPAWN_INTERVAL, SPAWN_INTERVAL);
    spawn_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let baseline_backend_hints = resolver.backend_hints().observed_backend_count();
    let baseline_server_count = preflight
        .initial_server_counts
        .counts
        .get(&preflight.selected_region.id)
        .copied()
        .unwrap_or(0);

    for (stage_index, stage) in config.stages.iter().enumerate() {
        if interrupted {
            break;
        }
        let stage_started_at = unix_time_ms();
        drain_activity_events(&mut activity_rx, &mut concurrency);
        let connected_at_start = concurrency.connected();
        let mut sessions_launched = 0usize;
        info!(
            stage = stage_index + 1,
            target = stage.target_concurrency,
            hold_seconds = stage.duration.as_secs_f64(),
            connected = concurrency.connected(),
            pending = concurrency.pending(),
            "entering ramp stage"
        );

        // Seed only the first stage immediately. Every later ramp step uses
        // the single global one-second ticker below, so a stage boundary can
        // never grant an extra wave outside the configured spawn rate.
        let launched = if stage_index == 0 {
            spawn_deficit_wave(
                stage.target_concurrency,
                launch_budget(&config, total_sessions_launched),
                &mut concurrency,
                &mut next_session_index,
                &mut next_group_index,
                &mut next_wave_index,
                &mut tasks,
                &mut planned_groups,
                &config,
                &settings,
                resolver.http_client(),
                &activity_tx,
                cancellation.clone(),
            )
        } else {
            0
        };
        sessions_launched += launched;
        total_sessions_launched = total_sessions_launched.saturating_add(launched);
        run.games.expected = run
            .games
            .expected
            .saturating_add(config.expected_games(launched));
        let mut target_reached = concurrency.connected() >= stage.target_concurrency;
        let mut target_reached_at = target_reached.then(unix_time_ms);

        let stage_deadline = Instant::now() + stage.duration;
        while Instant::now() < stage_deadline {
            tokio::select! {
                signal_result = &mut signal => {
                    match signal_result {
                        Ok(()) => warn!("interrupt received; stopping ramp and draining sessions"),
                        Err(error) => warn!(%error, "failed to install interrupt handler; stopping ramp"),
                    }
                    interrupted = true;
                    break;
                }
                event = activity_rx.recv() => {
                    if let Some(event) = event {
                        observe_stage_activity(
                            event,
                            &mut activity_rx,
                            &mut concurrency,
                            stage.target_concurrency,
                            &mut target_reached,
                            &mut target_reached_at,
                        );
                    }
                }
                joined = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    if let Some(joined) = joined {
                        drain_stage_activity_events(
                            &mut activity_rx,
                            &mut concurrency,
                            stage.target_concurrency,
                            &mut target_reached,
                            &mut target_reached_at,
                        );
                        collect_group_result(joined, &mut planned_groups, &mut run, &mut concurrency);
                    }
                    if let Some(reason) = failure_circuit_breaker(&run) {
                        error!(%reason, "failure circuit breaker stopped further load generation");
                        run.metadata.insert("circuit_breaker".to_owned(), reason);
                        interrupted = true;
                        break;
                    }
                }
                _ = spawn_interval.tick() => {
                    if concurrency.reserved() < stage.target_concurrency
                        && Instant::now() < stage_deadline
                    {
                        let budget = launch_budget(&config, total_sessions_launched);
                        if budget < config.sessions_per_group() {
                            let reason = format!(
                                "maximum total session limit {} reached before concurrency target {} could be maintained",
                                config.max_total_sessions,
                                stage.target_concurrency,
                            );
                            error!(%reason, "session safety limit stopped further load generation");
                            run.metadata.insert("session_limit".to_owned(), reason);
                            interrupted = true;
                            break;
                        }
                        let launched = spawn_deficit_wave(
                            stage.target_concurrency,
                            budget,
                            &mut concurrency,
                            &mut next_session_index,
                            &mut next_group_index,
                            &mut next_wave_index,
                            &mut tasks,
                            &mut planned_groups,
                            &config,
                            &settings,
                            resolver.http_client(),
                            &activity_tx,
                            cancellation.clone(),
                        );
                        sessions_launched += launched;
                        total_sessions_launched = total_sessions_launched.saturating_add(launched);
                        run.games.expected = run
                            .games
                            .expected
                            .saturating_add(config.expected_games(launched));
                    }
                }
                _ = observation_interval.tick() => {
                    sample_infrastructure(&resolver, &preflight.api_origin, &mut run).await;
                }
                _ = tokio::time::sleep_until(stage_deadline) => break,
            }
        }

        drain_stage_activity_events(
            &mut activity_rx,
            &mut concurrency,
            stage.target_concurrency,
            &mut target_reached,
            &mut target_reached_at,
        );

        run.ramp_stages.push(RampStageRecord {
            stage_index: stage_index as u32,
            target_concurrency: stage.target_concurrency,
            started_at_unix_ms: stage_started_at,
            finished_at_unix_ms: unix_time_ms(),
            sessions_launched,
            active_sessions_at_start: connected_at_start,
            active_sessions_at_end: concurrency.connected(),
            target_reached,
            target_reached_at_unix_ms: target_reached_at,
        });
    }

    info!(
        connected = concurrency.connected(),
        pending = concurrency.pending(),
        "ramp complete; draining authenticated sessions"
    );
    let drain_deadline = Instant::now() + config.drain_timeout;
    while !tasks.is_empty() && Instant::now() < drain_deadline {
        tokio::select! {
            event = activity_rx.recv() => {
                if let Some(event) = event {
                    concurrency.observe(event);
                    drain_activity_events(&mut activity_rx, &mut concurrency);
                }
            }
            joined = tasks.join_next_with_id() => {
                if let Some(joined) = joined {
                    drain_activity_events(&mut activity_rx, &mut concurrency);
                    collect_group_result(joined, &mut planned_groups, &mut run, &mut concurrency);
                }
            }
            _ = observation_interval.tick() => {
                sample_infrastructure(&resolver, &preflight.api_origin, &mut run).await;
            }
            _ = tokio::time::sleep_until(drain_deadline) => break,
        }
    }

    if !tasks.is_empty() {
        warn!(
            remaining_groups = tasks.len(),
            "drain timeout reached; cancelling remaining sessions"
        );
        cancellation.cancel();
        let grace_deadline = Instant::now() + CANCEL_GRACE;
        while !tasks.is_empty() && Instant::now() < grace_deadline {
            if let Ok(Some(joined)) =
                tokio::time::timeout_at(grace_deadline, tasks.join_next_with_id()).await
            {
                drain_activity_events(&mut activity_rx, &mut concurrency);
                collect_group_result(joined, &mut planned_groups, &mut run, &mut concurrency);
            } else {
                break;
            }
        }
        if !tasks.is_empty() {
            error!(
                remaining_groups = tasks.len(),
                "session tasks did not honor cancellation; aborting them"
            );
            tasks.abort_all();
            while let Some(joined) = tasks.join_next_with_id().await {
                drain_activity_events(&mut activity_rx, &mut concurrency);
                collect_group_result(joined, &mut planned_groups, &mut run, &mut concurrency);
            }
        }
    }

    drain_activity_events(&mut activity_rx, &mut concurrency);

    sample_infrastructure(&resolver, &preflight.api_origin, &mut run).await;
    run.finished_at_unix_ms = unix_time_ms();
    let observed_backend_hints = resolver.backend_hints().observed_backend_count();
    let peak_server_count = run
        .infrastructure_samples
        .iter()
        .filter_map(|sample| {
            sample
                .regional_server_counts
                .get(&preflight.selected_region.id)
                .copied()
        })
        .max()
        .unwrap_or(baseline_server_count);
    let scale_out_observed = baseline_server_count > 0 && peak_server_count > baseline_server_count;
    run.metadata.insert(
        "baseline_selected_region_servers".to_owned(),
        baseline_server_count.to_string(),
    );
    run.metadata.insert(
        "peak_selected_region_servers".to_owned(),
        peak_server_count.to_string(),
    );
    run.metadata.insert(
        "require_scale_out".to_owned(),
        config.require_scale_out.to_string(),
    );
    run.metadata.insert(
        "require_planned_handoff".to_owned(),
        config.require_planned_handoff.to_string(),
    );
    run.metadata.insert(
        "observed_backend_hints".to_owned(),
        observed_backend_hints.to_string(),
    );
    run.metadata.insert(
        "autoscaling_signal".to_owned(),
        if scale_out_observed {
            "active_server_count_increased"
        } else if observed_backend_hints > baseline_backend_hints {
            "backend_hint_increased_without_server_count_increase"
        } else {
            "no_scale_out_observed"
        }
        .to_owned(),
    );
    if interrupted {
        run.metadata
            .insert("interrupted".to_owned(), "true".to_owned());
    }
    run.metadata.insert(
        "sessions_launched".to_owned(),
        total_sessions_launched.to_string(),
    );
    run.metadata.insert(
        "coordinator_peak_authenticated_concurrency".to_owned(),
        concurrency.peak_connected().to_string(),
    );
    run.peak_authenticated_concurrency = concurrency.peak_connected();
    if !planned_groups.is_empty() {
        // This should be unreachable because every JoinSet result is drained,
        // but keep the report denominator complete if Tokio ever returns an
        // unexpected coordinator state.
        for (_, spec) in planned_groups.drain() {
            concurrency.mark_terminal(&spec.session_indices);
            run.sessions.extend(synthetic_group_failures(
                &spec,
                "coordinator lost task bookkeeping before a terminal result was produced",
            ));
            increment_metadata_counter(&mut run, "coordinator_task_failures");
        }
    }

    let report_dir = report_directory(&config.report_dir, &config.run_id);
    let aggregate = aggregate_report(&run);
    let completed_rate = if aggregate.session_counts.total == 0 {
        0.0
    } else {
        aggregate.session_counts.completed as f64 / aggregate.session_counts.total as f64
    };
    let coordinator_failures = metadata_counter(&run, "coordinator_task_failures");
    let all_sessions_accounted = aggregate.session_counts.total == total_sessions_launched;
    let all_games_observed = run.games.observed == run.games.expected;
    let peak_authenticated_target_reached =
        aggregate.session_counts.peak_authenticated_concurrency >= config.max_concurrency();
    let all_stages_completed = !interrupted && run.ramp_stages.len() == config.stages.len();
    let all_targets_reached =
        all_stages_completed && run.ramp_stages.iter().all(|stage| stage.target_reached);
    let mut threshold_failures = Vec::new();
    if aggregate.session_counts.total == 0 {
        threshold_failures.push("no sessions were reported".to_owned());
    } else if completed_rate < 0.98 {
        threshold_failures.push(format!(
            "session completion rate {:.2}% was below 98%",
            completed_rate * 100.0
        ));
    }
    if !config.open_loop_admission && !peak_authenticated_target_reached {
        threshold_failures.push(format!(
            "peak server-authenticated sessions {} never reached configured maximum {}",
            aggregate.session_counts.peak_authenticated_concurrency,
            config.max_concurrency()
        ));
    }
    if !all_sessions_accounted {
        threshold_failures.push(format!(
            "reported {} of {} launched sessions",
            aggregate.session_counts.total, total_sessions_launched
        ));
    }
    if coordinator_failures > 0 {
        threshold_failures.push(format!(
            "{coordinator_failures} coordinator task failures occurred"
        ));
    }
    if !all_games_observed {
        threshold_failures.push(format!(
            "observed {} of {} launched games",
            run.games.observed, run.games.expected
        ));
    }
    if run.games.pairing_violations > 0 {
        threshold_failures.push(format!(
            "{} deterministic pairing violations occurred",
            run.games.pairing_violations
        ));
    }
    if !all_stages_completed {
        threshold_failures.push("the configured stage plan did not complete".to_owned());
    } else if !config.open_loop_admission && !all_targets_reached {
        threshold_failures.push(
            "one or more server-authenticated session concurrency targets were not reached"
                .to_owned(),
        );
    }
    if config.require_scale_out && !scale_out_observed {
        threshold_failures.push(format!(
            "active selected-region servers did not rise above baseline {baseline_server_count}"
        ));
    }
    if aggregate
        .metrics
        .planned_handoffs
        .pending_commands_at_finish
        > 0
    {
        threshold_failures.push(format!(
            "{} game commands lacked a terminal outcome at session finish",
            aggregate
                .metrics
                .planned_handoffs
                .pending_commands_at_finish
        ));
    }
    if config.require_planned_handoff {
        let handoffs = &aggregate.metrics.planned_handoffs;
        if handoffs.attempts == 0 {
            threshold_failures.push("no planned drain handoff was observed".to_owned());
        }
        if handoffs.failures > 0 || handoffs.successes != handoffs.attempts {
            threshold_failures.push(format!(
                "planned handoffs were not lossless: {} attempts, {} successes, {} failures",
                handoffs.attempts, handoffs.successes, handoffs.failures
            ));
        }
        if config.population == Population::Game
            && handoffs
                .outcome_barriers
                .saturating_add(handoffs.terminal_completions)
                != handoffs.successes
        {
            threshold_failures.push(format!(
                "only {} of {} planned handoffs observed an outcome barrier or terminal snapshot",
                handoffs
                    .outcome_barriers
                    .saturating_add(handoffs.terminal_completions),
                handoffs.successes
            ));
        }
        if handoffs.continuity_proofs != handoffs.successes {
            threshold_failures.push(format!(
                "only {} of {} successful planned handoffs had observed old/new continuity",
                handoffs.continuity_proofs, handoffs.successes
            ));
        }
        if config.population == Population::Game
            && config.command_profile.sends_unchanged_turns()
            && handoffs.successes > 0
            && handoffs.commands_sent == 0
        {
            threshold_failures.push(
                "no every-tick game command was emitted on an old socket while a planned candidate was prepared"
                    .to_owned(),
            );
        }
        if aggregate.metrics.usable_session_gap_ms.max_ms.unwrap_or(0) > 0 {
            threshold_failures.push(format!(
                "usable-session gap reached {} ms during planned-handoff certification",
                aggregate.metrics.usable_session_gap_ms.max_ms.unwrap_or(0)
            ));
        }
    }
    let passed = threshold_failures.is_empty();
    run.metadata.insert(
        "sessions_accounted".to_owned(),
        aggregate.session_counts.total.to_string(),
    );
    run.metadata.insert(
        "all_games_observed".to_owned(),
        all_games_observed.to_string(),
    );
    run.metadata.insert(
        "peak_authenticated_target_reached".to_owned(),
        peak_authenticated_target_reached.to_string(),
    );
    run.metadata.insert(
        "all_stages_completed".to_owned(),
        all_stages_completed.to_string(),
    );
    run.metadata.insert(
        "all_stage_targets_reached".to_owned(),
        all_targets_reached.to_string(),
    );
    run.metadata.insert(
        "threshold_result".to_owned(),
        if passed { "passed" } else { "failed" }.to_owned(),
    );
    if !threshold_failures.is_empty() {
        run.metadata.insert(
            "threshold_failures".to_owned(),
            threshold_failures.join("; "),
        );
    }
    // Rebuild after threshold metadata is present and write all artifacts.
    let written = write_report(&report_dir, &run)?;
    let aggregate = aggregate_report(&run);

    info!(
        sessions = aggregate.session_counts.total,
        completed = aggregate.session_counts.completed,
        failed = aggregate.session_counts.failed,
        peak_token_sent = aggregate.session_counts.peak_token_sent_concurrency,
        peak_authenticated = aggregate.session_counts.peak_authenticated_concurrency,
        peak_active_games = aggregate.session_counts.peak_active_game_concurrency,
        games_completed = aggregate.games.completed,
        games_timeboxed = aggregate.games.timeboxed,
        games_expected = aggregate.games.expected,
        pairing_violations = aggregate.games.pairing_violations,
        queue_p95_ms = ?aggregate.metrics.matchmaking_wait_ms.p95_ms,
        report = %written.index_html.display(),
        "load test finished"
    );

    if !passed {
        return Err(anyhow!(
            "load-test thresholds failed: {}; report: {}",
            threshold_failures.join("; "),
            written.index_html.display()
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_deficit_wave(
    target: usize,
    launch_budget: usize,
    concurrency: &mut SessionConcurrencyTracker,
    next_session_index: &mut u64,
    next_group_index: &mut u64,
    next_wave_index: &mut u32,
    tasks: &mut JoinSet<MatchGroupResult>,
    planned_groups: &mut HashMap<Id, MatchGroupSpec>,
    config: &Config,
    settings: &SessionSettings,
    http_client: reqwest::Client,
    activity_tx: &mpsc::UnboundedSender<SessionActivityEvent>,
    cancellation: CancellationToken,
) -> usize {
    let players_per_game = config.sessions_per_group();
    let group_count = groups_to_launch(
        target,
        concurrency.reserved(),
        launch_budget,
        players_per_game,
    );
    if group_count == 0 {
        return 0;
    }
    let barrier = Arc::new(Barrier::new(group_count));
    let wave_index = *next_wave_index;
    *next_wave_index = next_wave_index.saturating_add(1);

    let session_groups = plan_session_groups(*next_session_index, group_count, players_per_game);
    for session_indices in session_groups {
        let group_index = *next_group_index;
        *next_group_index = next_group_index.saturating_add(1);
        let spec = MatchGroupSpec {
            run_id: config.run_id.clone(),
            wave_index,
            group_index,
            session_indices,
        };
        // Reserve before spawning so a very fast prepare failure cannot race
        // its terminal notification ahead of coordinator bookkeeping.
        concurrency.reserve(&spec.session_indices);
        let handle = tasks.spawn(run_match_group(
            spec.clone(),
            settings.clone(),
            http_client.clone(),
            activity_tx.clone(),
            barrier.clone(),
            cancellation.clone(),
        ));
        planned_groups.insert(handle.id(), spec);
    }
    let launched = group_count * players_per_game;
    *next_session_index = next_session_index.saturating_add(launched as u64);
    info!(
        wave = wave_index,
        groups = group_count,
        sessions = launched,
        connected = concurrency.connected(),
        pending = concurrency.pending(),
        reserved = concurrency.reserved(),
        "launched coordinated wave"
    );
    launched
}

fn groups_to_launch(
    target: usize,
    reserved: usize,
    launch_budget: usize,
    players_per_game: usize,
) -> usize {
    if reserved >= target || players_per_game == 0 {
        return 0;
    }
    // Replacements remain full match groups. If a single VU from a group
    // exits early, the bounded overshoot is therefore less than one group.
    (target - reserved)
        .div_ceil(players_per_game)
        .min(launch_budget / players_per_game)
}

fn record_successful_game_outcome(
    run: &mut LoadTestRun,
    result: &MatchGroupResult,
    planned_count: usize,
) {
    if result.observed_game_ids.len() != result.expected_game_count
        || result.sessions.len() != planned_count
        || !result
            .sessions
            .iter()
            .all(|session| session.outcome == SessionOutcome::Completed)
    {
        return;
    }

    let timeboxed = result.sessions.iter().any(|session| {
        session
            .diagnostics
            .get("completion_kind")
            .is_some_and(|kind| kind == "timeboxed")
    });
    if timeboxed {
        run.games.timeboxed = run
            .games
            .timeboxed
            .saturating_add(result.expected_game_count);
    } else {
        run.games.completed = run
            .games
            .completed
            .saturating_add(result.expected_game_count);
    }
}

fn collect_group_result(
    joined: std::result::Result<(Id, MatchGroupResult), JoinError>,
    planned_groups: &mut HashMap<Id, MatchGroupSpec>,
    run: &mut LoadTestRun,
    concurrency: &mut SessionConcurrencyTracker,
) {
    match joined {
        Ok((task_id, mut result)) => {
            let spec = planned_groups.remove(&task_id);
            let planned_count = spec
                .as_ref()
                .map_or(result.sessions.len(), |spec| spec.session_indices.len());

            if let Some(spec) = &spec {
                // Usually idempotent with per-VU terminal events. This also
                // closes reservations if a task panics before creating leases.
                concurrency.mark_terminal(&spec.session_indices);
                let returned: BTreeSet<&str> = result
                    .sessions
                    .iter()
                    .map(|session| session.session_id.as_str())
                    .collect();
                let missing: Vec<u64> = spec
                    .session_indices
                    .iter()
                    .copied()
                    .filter(|index| !returned.contains(format!("session-{index:08}").as_str()))
                    .collect();
                if !missing.is_empty() || result.sessions.len() != planned_count {
                    increment_metadata_counter(run, "coordinator_task_failures");
                    for session_index in missing {
                        result.sessions.push(synthetic_session_failure(
                            spec,
                            session_index,
                            "match-group task returned without this planned session record",
                        ));
                    }
                }
            } else {
                increment_metadata_counter(run, "coordinator_task_failures");
                error!(
                    ?task_id,
                    "completed match-group task had no coordinator plan"
                );
            }

            run.games.observed = run
                .games
                .observed
                .saturating_add(result.observed_game_ids.len());
            record_successful_game_outcome(run, &result, planned_count);
            if let Some(violation) = result.pairing_violation {
                run.games.pairing_violations = run.games.pairing_violations.saturating_add(1);
                run.pairing_violation_details.push(violation);
            }
            run.sessions.extend(result.sessions);
        }
        Err(error) => {
            let task_id = error.id();
            let spec = planned_groups.remove(&task_id);
            increment_metadata_counter(run, "coordinator_task_failures");
            if let Some(spec) = spec {
                concurrency.mark_terminal(&spec.session_indices);
                run.sessions.extend(synthetic_group_failures(
                    &spec,
                    &format!("match-group coordinator task failed: {error}"),
                ));
            }
            error!(%error, "match-group task failed before producing session records");
        }
    }
}

fn synthetic_group_failures(spec: &MatchGroupSpec, message: &str) -> Vec<SessionRecord> {
    spec.session_indices
        .iter()
        .map(|session_index| synthetic_session_failure(spec, *session_index, message))
        .collect()
}

fn synthetic_session_failure(
    spec: &MatchGroupSpec,
    session_index: u64,
    message: &str,
) -> SessionRecord {
    let now = unix_time_ms();
    let mut record = SessionRecord::new(
        format!("session-{session_index:08}"),
        deterministic_username(&spec.run_id, session_index),
        spec.wave_index,
        spec.group_id(),
        now,
    );
    record.fail(
        SessionFailureRecord::new(SessionPhase::Cleanup, now, message)
            .with_context("coordinator_failure", "true")
            .with_context("session_index", session_index.to_string()),
    );
    record
}

fn increment_metadata_counter(run: &mut LoadTestRun, key: &str) {
    run.metadata
        .entry(key.to_owned())
        .and_modify(|value| {
            let count = value.parse::<u64>().unwrap_or(0).saturating_add(1);
            *value = count.to_string();
        })
        .or_insert_with(|| "1".to_owned());
}

fn metadata_counter(run: &LoadTestRun, key: &str) -> u64 {
    run.metadata
        .get(key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn launch_budget(config: &Config, total_sessions_launched: usize) -> usize {
    config.spawn_rate.min(
        config
            .max_total_sessions
            .saturating_sub(total_sessions_launched),
    )
}

async fn sample_infrastructure(resolver: &TargetResolver, api_origin: &str, run: &mut LoadTestRun) {
    let (users, servers) = tokio::join!(
        resolver.sample_user_counts_from_origin(api_origin),
        resolver.sample_server_counts_from_origin(api_origin),
    );
    let mut errors = Vec::new();
    let regional_user_counts = match users {
        Ok(sample) => sample.counts,
        Err(error) => {
            errors.push(format!("user counts: {error}"));
            Default::default()
        }
    };
    let regional_server_counts = match servers {
        Ok(sample) => sample.counts,
        Err(error) => {
            errors.push(format!("server counts: {error}"));
            Default::default()
        }
    };
    run.infrastructure_samples.push(InfrastructureSample {
        observed_at_unix_ms: unix_time_ms(),
        regional_user_counts,
        regional_server_counts,
        observed_backend_hints: resolver.backend_hints().observed_backend_count(),
        error: (!errors.is_empty()).then(|| errors.join("; ")),
    });
}

fn game_type(mode: GameMode) -> GameType {
    match mode {
        GameMode::Solo => GameType::Solo,
        GameMode::Duel => GameType::TeamMatch { per_team: 1 },
        GameMode::TwoVTwo => GameType::TeamMatch { per_team: 2 },
        GameMode::FreeForAll => GameType::FreeForAll { max_players: 4 },
    }
}

fn queue_mode(mode: config::QueueMode) -> QueueMode {
    match mode {
        config::QueueMode::Quickmatch => QueueMode::Quickmatch,
        config::QueueMode::Competitive => QueueMode::Competitive,
    }
}

fn ensure_effective_endpoints_confirmed(
    production_confirmed: bool,
    endpoints: &[&Url],
) -> Result<()> {
    if !production_confirmed
        && endpoints
            .iter()
            .any(|endpoint| config::is_snaketron_production_url(endpoint))
    {
        return Err(anyhow!(
            "target discovery resolved to snaketron.io production; pass --confirm-production to acknowledge the load"
        ));
    }
    Ok(())
}

fn ensure_effective_endpoints_same_origin(
    target: &Url,
    api_origin: &Url,
    selected_origin: &Url,
    websocket_url: &Url,
) -> Result<()> {
    let mut mismatches = Vec::new();
    if target.scheme() != api_origin.scheme() || !same_authority(target, api_origin) {
        mismatches.push("API origin");
    }
    if target.scheme() != selected_origin.scheme() || !same_authority(target, selected_origin) {
        mismatches.push("selected regional origin");
    }
    let expected_websocket_scheme = match target.scheme() {
        "http" => "ws",
        "https" => "wss",
        scheme => {
            return Err(anyhow!(
                "same-origin gate does not support target scheme '{scheme}'"
            ));
        }
    };
    if websocket_url.scheme() != expected_websocket_scheme || !same_authority(target, websocket_url)
    {
        mismatches.push("WebSocket endpoint");
    }

    if mismatches.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "--require-same-origin rejected incoherent endpoints before guest creation: {}; target={target}, api_origin={api_origin}, selected_origin={selected_origin}, websocket_url={websocket_url}",
        mismatches.join(", ")
    ))
}

fn same_authority(left: &Url, right: &Url) -> bool {
    left.host_str()
        .zip(right.host_str())
        .is_some_and(|(left_host, right_host)| left_host.eq_ignore_ascii_case(right_host))
        && effective_port(left) == effective_port(right)
}

fn effective_port(url: &Url) -> Option<u16> {
    url.port().or(match url.scheme() {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    })
}

fn report_directory(root: &Path, run_id: &str) -> PathBuf {
    root.join(run_id)
}

fn failure_circuit_breaker(run: &LoadTestRun) -> Option<String> {
    let terminal = run.sessions.len();
    if terminal < FAILURE_CIRCUIT_BREAKER_MIN_SESSIONS {
        return None;
    }
    let failures = run
        .sessions
        .iter()
        .filter(|session| session.outcome == SessionOutcome::Failed)
        .count();
    let rate = failures as f64 / terminal as f64;
    (rate > FAILURE_CIRCUIT_BREAKER_RATE).then(|| {
        format!(
            "{failures}/{terminal} terminal sessions failed ({:.1}%, limit {:.1}%)",
            rate * 100.0,
            FAILURE_CIRCUIT_BREAKER_RATE * 100.0
        )
    })
}

fn plan_session_groups(
    first_session_index: u64,
    group_count: usize,
    players_per_game: usize,
) -> Vec<Vec<u64>> {
    (0..group_count)
        .map(|group_offset| {
            let first = first_session_index + (group_offset * players_per_game) as u64;
            (0..players_per_game)
                .map(|player_offset| first + player_offset as u64)
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_duel_sessions_are_planned_as_two_deterministic_games() {
        assert_eq!(plan_session_groups(1, 2, 2), vec![vec![1, 2], vec![3, 4]]);
    }

    #[test]
    fn pending_sessions_reserve_capacity_without_counting_as_connected() {
        let mut concurrency = SessionConcurrencyTracker::default();
        concurrency.reserve(&[1, 2]);

        assert_eq!(concurrency.pending(), 2);
        assert_eq!(concurrency.connected(), 0);
        assert_eq!(concurrency.reserved(), 2);
        assert_eq!(groups_to_launch(2, concurrency.reserved(), 2, 2), 0);
    }

    #[test]
    fn individual_terminal_session_releases_capacity_before_its_group_finishes() {
        let mut concurrency = SessionConcurrencyTracker::default();
        concurrency.reserve(&[1, 2]);
        concurrency.observe(SessionActivityEvent::Connected { session_index: 1 });
        concurrency.observe(SessionActivityEvent::Connected { session_index: 2 });

        concurrency.observe(SessionActivityEvent::Terminal { session_index: 1 });

        assert_eq!(concurrency.connected(), 1);
        assert_eq!(concurrency.reserved(), 1);
        assert_eq!(groups_to_launch(2, concurrency.reserved(), 2, 2), 1);
    }

    #[test]
    fn replacement_launches_stay_match_group_aligned_and_bounded() {
        // One missing member of a four-player group requires one complete
        // replacement group, producing at most a three-session overshoot.
        assert_eq!(groups_to_launch(4, 3, 8, 4), 1);
        assert_eq!(3 + groups_to_launch(4, 3, 8, 4) * 4, 7);
        assert_eq!(groups_to_launch(4, 4, 8, 4), 0);
    }

    #[test]
    fn late_connected_event_cannot_resurrect_a_terminal_session() {
        let mut concurrency = SessionConcurrencyTracker::default();
        concurrency.reserve(&[1]);
        concurrency.mark_terminal(&[1]);
        concurrency.observe(SessionActivityEvent::Connected { session_index: 1 });

        assert_eq!(concurrency.connected(), 0);
        assert_eq!(concurrency.reserved(), 0);
        assert_eq!(concurrency.peak_connected(), 0);
    }

    #[test]
    fn stage_drain_records_a_transient_authenticated_target_before_terminal() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut concurrency = SessionConcurrencyTracker::default();
        concurrency.reserve(&[1]);
        sender
            .send(SessionActivityEvent::Connected { session_index: 1 })
            .unwrap();
        sender
            .send(SessionActivityEvent::Terminal { session_index: 1 })
            .unwrap();
        let mut target_reached = false;
        let mut target_reached_at = None;

        drain_stage_activity_events(
            &mut receiver,
            &mut concurrency,
            1,
            &mut target_reached,
            &mut target_reached_at,
        );

        assert!(target_reached);
        assert!(target_reached_at.is_some());
        assert_eq!(concurrency.connected(), 0);
        assert_eq!(concurrency.peak_connected(), 1);
    }

    #[test]
    fn solo_and_four_player_modes_use_the_expected_lobby_topology() {
        assert_eq!(
            plan_session_groups(1, 4, GameMode::Solo.players_per_game()),
            vec![vec![1], vec![2], vec![3], vec![4]]
        );
        assert_eq!(
            plan_session_groups(1, 1, GameMode::FreeForAll.players_per_game()),
            vec![vec![1, 2, 3, 4]]
        );
        assert_eq!(
            plan_session_groups(1, 1, GameMode::TwoVTwo.players_per_game()),
            vec![vec![1, 2, 3, 4]]
        );
    }

    #[test]
    fn successful_games_are_split_between_authoritative_and_timeboxed_counts() {
        fn result(timebox_first: bool) -> MatchGroupResult {
            let mut first = SessionRecord::new("s1", "u1", 0, "g1", 0);
            if timebox_first {
                first
                    .diagnostics
                    .insert("completion_kind".to_owned(), "timeboxed".to_owned());
            }
            first.complete(1);
            let mut second = SessionRecord::new("s2", "u2", 0, "g1", 0);
            second.complete(1);
            MatchGroupResult {
                sessions: vec![first, second],
                expected_game_count: 1,
                observed_game_ids: BTreeSet::from([42]),
                pairing_violation: None,
            }
        }

        let mut run = LoadTestRun::new("run", "target", 0, 2);
        record_successful_game_outcome(&mut run, &result(false), 2);
        record_successful_game_outcome(&mut run, &result(true), 2);

        assert_eq!(run.games.completed, 1);
        assert_eq!(run.games.timeboxed, 1);
    }

    #[test]
    fn unsuccessful_group_is_not_counted_as_a_completed_game() {
        let mut completed = SessionRecord::new("s1", "u1", 0, "g1", 0);
        completed.complete(1);
        let failed = SessionRecord::new("s2", "u2", 0, "g1", 0);
        let result = MatchGroupResult {
            sessions: vec![completed, failed],
            expected_game_count: 1,
            observed_game_ids: BTreeSet::from([42]),
            pairing_violation: None,
        };
        let mut run = LoadTestRun::new("run", "target", 0, 2);

        record_successful_game_outcome(&mut run, &result, 2);

        assert_eq!(run.games.completed, 0);
        assert_eq!(run.games.timeboxed, 0);
    }

    #[test]
    fn systemic_failures_trip_the_load_circuit_breaker() {
        let mut run = LoadTestRun::new("run", "target", 0, 4);
        for index in 0..4 {
            let mut session = loadtest::report::SessionRecord::new(
                format!("s{index}"),
                format!("u{index}"),
                0,
                "g",
                0,
            );
            session.fail(loadtest::report::SessionFailureRecord::new(
                loadtest::report::SessionPhase::WebSocketConnect,
                1,
                "down",
            ));
            run.sessions.push(session);
        }
        assert!(failure_circuit_breaker(&run).is_some());
    }

    #[test]
    fn discovered_production_endpoint_requires_confirmation() {
        let custom_api = Url::parse("https://staging.example.test").unwrap();
        let production_region = Url::parse("https://use1.snaketron.io").unwrap();
        let production_websocket = Url::parse("wss://use1.snaketron.io/ws").unwrap();

        assert!(
            ensure_effective_endpoints_confirmed(
                false,
                &[&custom_api, &production_region, &production_websocket],
            )
            .is_err()
        );
        assert!(
            ensure_effective_endpoints_confirmed(
                true,
                &[&custom_api, &production_region, &production_websocket],
            )
            .is_ok()
        );
        assert!(ensure_effective_endpoints_confirmed(false, &[&custom_api]).is_ok());
    }

    #[test]
    fn same_origin_gate_accepts_only_coherent_effective_endpoints() {
        let target = Url::parse("https://stg-123-1.snaketron.io/").unwrap();
        let api = Url::parse("https://stg-123-1.snaketron.io:443/").unwrap();
        let selected = Url::parse("https://stg-123-1.snaketron.io/").unwrap();
        let websocket = Url::parse("wss://stg-123-1.snaketron.io/socket").unwrap();

        assert!(
            ensure_effective_endpoints_same_origin(&target, &api, &selected, &websocket).is_ok()
        );
    }

    #[test]
    fn same_origin_gate_reports_every_endpoint_that_escaped_the_target() {
        let target = Url::parse("https://stg-123-1.snaketron.io/").unwrap();
        let production_api = Url::parse("https://api.snaketron.io/").unwrap();
        let production_region = Url::parse("https://use1.snaketron.io/").unwrap();
        let production_websocket = Url::parse("wss://use1.snaketron.io/ws").unwrap();

        let error = ensure_effective_endpoints_same_origin(
            &target,
            &production_api,
            &production_region,
            &production_websocket,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("before guest creation"));
        assert!(error.contains("API origin"));
        assert!(error.contains("selected regional origin"));
        assert!(error.contains("WebSocket endpoint"));
        assert!(error.contains("target=https://stg-123-1.snaketron.io/"));
    }

    #[test]
    fn same_origin_gate_rejects_scheme_or_port_changes_on_the_same_host() {
        let target = Url::parse("http://localhost:8080/").unwrap();
        let api = Url::parse("http://localhost:8080/").unwrap();
        let selected = Url::parse("https://localhost:8080/").unwrap();
        let websocket = Url::parse("ws://localhost:8081/ws").unwrap();

        let error = ensure_effective_endpoints_same_origin(&target, &api, &selected, &websocket)
            .unwrap_err()
            .to_string();
        assert!(error.contains("selected regional origin"));
        assert!(error.contains("WebSocket endpoint"));
        assert!(!error.contains("API origin"));
    }

    #[tokio::test]
    async fn panicked_group_is_synthesized_into_session_failures() {
        async fn panic_group() -> MatchGroupResult {
            panic!("coordinator test panic")
        }

        let spec = MatchGroupSpec {
            run_id: "run".to_owned(),
            wave_index: 3,
            group_index: 7,
            session_indices: vec![41, 42],
        };
        let mut tasks = JoinSet::new();
        let handle = tasks.spawn(panic_group());
        let mut planned = HashMap::from([(handle.id(), spec)]);
        let joined = tasks.join_next_with_id().await.unwrap();
        let mut run = LoadTestRun::new("run", "target", 0, 2);
        let mut concurrency = SessionConcurrencyTracker::default();
        concurrency.reserve(&[41, 42]);

        collect_group_result(joined, &mut planned, &mut run, &mut concurrency);

        assert_eq!(concurrency.reserved(), 0);
        assert!(planned.is_empty());
        assert_eq!(run.sessions.len(), 2);
        assert!(run.sessions.iter().all(SessionRecord::is_failed));
        assert_eq!(metadata_counter(&run, "coordinator_task_failures"), 1);
    }
}
