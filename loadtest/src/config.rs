//! Command-line configuration and safety validation for the load-test runner.
//!
//! Parsing and validation are intentionally separate. [`Args`] describes the
//! CLI, while [`Config`] is the validated input consumed by the coordinator.

use clap::{Parser, ValueEnum};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

const DEFAULT_TARGET: &str = "http://localhost:8080";
const DEFAULT_STAGES: &str = "4@30s,16@30s,64@1m,128@2m,256@5m";
const DEFAULT_SPAWN_RATE: usize = 4;
const DEFAULT_MAX_TOTAL_SESSIONS: usize = 4_096;
const MAX_RUN_ID_LEN: usize = 64;

/// CLI arguments for the Snaketron load test.
///
/// Production is deliberately not the default. Any target on `snaketron.io`
/// also requires `--confirm-production` before [`Config`] can be created.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "snaketron-loadtest",
    about = "Run coordinated, AI-driven Snaketron load-test sessions"
)]
pub struct Args {
    /// Base HTTP(S) URL used for guest authentication and target identification.
    #[arg(long, default_value = DEFAULT_TARGET, value_name = "URL")]
    pub target: String,

    /// Region identifier to request or record (for example, `use1`).
    #[arg(long, value_name = "REGION")]
    pub region: Option<String>,

    /// Connect directly to this WebSocket endpoint instead of deriving one.
    #[arg(long, value_name = "WS_URL")]
    pub ws_url: Option<String>,

    /// Match type exercised by each coordinated group.
    #[arg(long, value_enum, default_value_t = GameMode::Duel)]
    pub mode: GameMode,

    /// Matchmaking queue to exercise.
    #[arg(long, value_enum, default_value_t = QueueMode::Quickmatch)]
    pub queue_mode: QueueMode,

    /// Lifecycle state held by each virtual user.
    ///
    /// `game` is the normal AI-driven load. The other values are staging
    /// probes that deliberately remain authenticated, in a lobby, or queued
    /// without entering a game.
    #[arg(long, value_enum, default_value_t = Population::Game)]
    pub population: Population,

    /// Comma-separated target concurrency and ramp duration stages.
    ///
    /// Example: `4@30s,8@1m` gives the 4-session ramp/hold 30 seconds, then
    /// gives the 8-session ramp/hold one minute. Once reached, a target is
    /// maintained by replacing completed groups through the spawn limiter.
    #[arg(long, default_value = DEFAULT_STAGES, value_name = "N@DURATION,...")]
    pub stages: StagePlan,

    /// Maximum virtual-user sessions started per second.
    ///
    /// Launches are rounded down to whole deterministic match groups. This
    /// rate also limits replacement sessions when games finish or fail.
    #[arg(long, default_value_t = DEFAULT_SPAWN_RATE, value_name = "SESSIONS")]
    pub spawn_rate: usize,

    /// Emit idle admission probes at the configured spawn rate without
    /// requiring the stage's concurrency ceiling to be reached.
    ///
    /// This is intentionally limited to the `idle` population. The stage
    /// target remains a hard in-flight ceiling, and `max-total-sessions`
    /// remains the run-wide launch limit.
    #[arg(long)]
    pub open_loop_admission: bool,

    /// Hard safety cap on virtual-user sessions created during the entire run.
    #[arg(
        long,
        default_value_t = DEFAULT_MAX_TOTAL_SESSIONS,
        value_name = "SESSIONS"
    )]
    pub max_total_sessions: usize,

    /// Maximum time allowed to establish a WebSocket connection.
    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub connect_timeout: Duration,

    /// Maximum time allowed to create and observe a ready lobby.
    #[arg(
        long,
        default_value = "10s",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub lobby_timeout: Duration,

    /// Maximum time a coordinated group may wait in matchmaking.
    #[arg(
        long,
        default_value = "1m",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub queue_timeout: Duration,

    /// Maximum time to wait for active sessions after the final stage.
    #[arg(
        long,
        default_value = "5m",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub drain_timeout: Duration,

    /// How long a healthy Solo/FFA session plays, or a non-game population
    /// probe holds its lifecycle state, before leaving successfully.
    ///
    /// Solo/FFA have no authoritative server time limit. Their timeboxed leave
    /// is reported separately from an authoritatively completed game.
    #[arg(
        long,
        default_value = "2m",
        value_parser = parse_duration,
        value_name = "DURATION"
    )]
    pub untimed_play_duration: Duration,

    /// Directory under which the run's aggregate and failure reports are written.
    #[arg(long, default_value = "loadtest-reports", value_name = "PATH")]
    pub report_dir: PathBuf,

    /// Stable identifier used in reports and deterministic session names.
    ///
    /// If omitted, a process-local identifier is generated.
    #[arg(long, value_name = "ID")]
    pub run_id: Option<String>,

    /// How frequently the AI emits turn commands.
    #[arg(long, value_enum, default_value_t = CommandProfile::Realistic)]
    pub command_profile: CommandProfile,

    /// Acknowledge that this run intentionally sends load to snaketron.io.
    #[arg(long)]
    pub confirm_production: bool,

    /// Fail before creating guests unless the effective API, regional, and
    /// WebSocket endpoints all remain on the explicit target origin.
    #[arg(long)]
    pub require_same_origin: bool,

    /// Fail the run unless the selected region's active server count rises
    /// above its preflight baseline.
    #[arg(long)]
    pub require_scale_out: bool,

    /// Fail unless at least one planned drain completes make-before-break,
    /// every such handoff has an old/new continuity proof, and no usable gap is
    /// measured. Game populations additionally require the command-outcome
    /// barrier. Intended for the staged scale-down certification runner.
    #[arg(long)]
    pub require_planned_handoff: bool,
}

impl Args {
    /// Validate parsed CLI arguments and convert them into coordinator input.
    pub fn into_config(self) -> Result<Config, ConfigError> {
        Config::try_from(self)
    }
}

/// Supported multiplayer game modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GameMode {
    #[value(name = "solo")]
    Solo,
    #[value(name = "duel")]
    Duel,
    #[value(name = "2v2", alias = "two-v-two")]
    TwoVTwo,
    #[value(name = "ffa", alias = "free-for-all")]
    FreeForAll,
}

impl GameMode {
    pub const fn players_per_game(self) -> usize {
        match self {
            Self::Solo => 1,
            Self::Duel => 2,
            Self::TwoVTwo | Self::FreeForAll => 4,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Solo => "solo",
            Self::Duel => "duel",
            Self::TwoVTwo => "2v2",
            Self::FreeForAll => "ffa",
        }
    }
}

impl fmt::Display for GameMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Matchmaking queue selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum QueueMode {
    #[value(name = "quickmatch")]
    Quickmatch,
    #[value(name = "competitive")]
    Competitive,
}

/// Lifecycle population exercised by a load-test process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Population {
    /// Create deterministic matches and drive them with the configured AI.
    #[value(name = "game")]
    Game,
    /// Authenticate a WebSocket and otherwise remain idle.
    #[value(name = "idle")]
    Idle,
    /// Create a lobby and remain in it without entering matchmaking.
    #[value(name = "lobby")]
    Lobby,
    /// Create a one-player lobby and remain queued without being matched.
    #[value(name = "matchmaking")]
    Matchmaking,
}

impl Population {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Game => "game",
            Self::Idle => "idle",
            Self::Lobby => "lobby",
            Self::Matchmaking => "matchmaking",
        }
    }

    pub const fn sessions_per_group(self, mode: GameMode) -> usize {
        match self {
            Self::Game => mode.players_per_game(),
            Self::Idle | Self::Lobby | Self::Matchmaking => 1,
        }
    }
}

impl fmt::Display for Population {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl QueueMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quickmatch => "quickmatch",
            Self::Competitive => "competitive",
        }
    }
}

impl fmt::Display for QueueMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Command emission policy for AI-driven sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CommandProfile {
    /// Emit a turn only when the AI changes direction, matching normal UI input.
    #[value(name = "realistic", alias = "changed-only")]
    Realistic,
    /// Emit the AI decision every game tick, including unchanged directions.
    #[value(name = "every-tick", alias = "saturating")]
    EveryTick,
}

impl CommandProfile {
    pub const fn sends_unchanged_turns(self) -> bool {
        matches!(self, Self::EveryTick)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Realistic => "realistic",
            Self::EveryTick => "every-tick",
        }
    }
}

impl fmt::Display for CommandProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One bounded ramp-and-hold stage. `duration` includes the time spent rising
/// to `target_concurrency`; after reaching it, the coordinator maintains it for
/// the remainder of the stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadStage {
    pub target_concurrency: usize,
    pub duration: Duration,
}

impl LoadStage {
    pub const fn new(target_concurrency: usize, duration: Duration) -> Self {
        Self {
            target_concurrency,
            duration,
        }
    }
}

/// Parsed sequence of concurrency targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagePlan(Vec<LoadStage>);

impl StagePlan {
    pub fn new(stages: Vec<LoadStage>) -> Result<Self, ConfigError> {
        if stages.is_empty() {
            return Err(ConfigError::EmptyStages);
        }
        Ok(Self(stages))
    }

    pub fn as_slice(&self) -> &[LoadStage] {
        &self.0
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &LoadStage> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn max_concurrency(&self) -> usize {
        self.0
            .iter()
            .map(|stage| stage.target_concurrency)
            .max()
            .unwrap_or(0)
    }
}

impl AsRef<[LoadStage]> for StagePlan {
    fn as_ref(&self) -> &[LoadStage] {
        self.as_slice()
    }
}

impl IntoIterator for StagePlan {
    type Item = LoadStage;
    type IntoIter = std::vec::IntoIter<LoadStage>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a StagePlan {
    type Item = &'a LoadStage;
    type IntoIter = std::slice::Iter<'a, LoadStage>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl FromStr for StagePlan {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("stage plan must contain at least one stage".to_string());
        }

        let mut stages = Vec::new();
        for (index, raw_stage) in value.split(',').enumerate() {
            let raw_stage = raw_stage.trim();
            let display_index = index + 1;
            if raw_stage.is_empty() {
                return Err(format!("stage {display_index} is empty"));
            }

            let (concurrency, duration) = raw_stage.split_once('@').ok_or_else(|| {
                format!("stage {display_index} must use N@DURATION syntax (received '{raw_stage}')")
            })?;

            if duration.contains('@') {
                return Err(format!("stage {display_index} contains more than one '@'"));
            }

            let target_concurrency = concurrency.trim().parse::<usize>().map_err(|_| {
                format!(
                    "stage {display_index} has invalid concurrency '{}'",
                    concurrency.trim()
                )
            })?;
            if target_concurrency == 0 {
                return Err(format!(
                    "stage {display_index} target concurrency must be greater than zero"
                ));
            }

            let duration = parse_duration(duration.trim())
                .map_err(|error| format!("stage {display_index}: {error}"))?;
            stages.push(LoadStage::new(target_concurrency, duration));
        }

        Ok(Self(stages))
    }
}

/// Validated configuration consumed by the load-test coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub target_url: Url,
    pub region: Option<String>,
    pub ws_url: Option<Url>,
    pub mode: GameMode,
    pub queue_mode: QueueMode,
    pub population: Population,
    pub stages: StagePlan,
    pub spawn_rate: usize,
    pub open_loop_admission: bool,
    pub max_total_sessions: usize,
    pub connect_timeout: Duration,
    pub lobby_timeout: Duration,
    pub queue_timeout: Duration,
    pub drain_timeout: Duration,
    pub untimed_play_duration: Duration,
    pub report_dir: PathBuf,
    pub run_id: String,
    pub command_profile: CommandProfile,
    pub production_confirmed: bool,
    pub require_same_origin: bool,
    pub require_scale_out: bool,
    pub require_planned_handoff: bool,
}

impl Config {
    pub const fn sessions_per_group(&self) -> usize {
        self.population.sessions_per_group(self.mode)
    }

    pub const fn expected_games(&self, launched_sessions: usize) -> usize {
        match self.population {
            Population::Game => launched_sessions / self.mode.players_per_game(),
            Population::Idle | Population::Lobby | Population::Matchmaking => 0,
        }
    }

    pub fn max_concurrency(&self) -> usize {
        self.stages.max_concurrency()
    }

    pub fn is_production(&self) -> bool {
        is_snaketron_production_url(&self.target_url)
            || self
                .ws_url
                .as_ref()
                .is_some_and(is_snaketron_production_url)
    }
}

impl TryFrom<Args> for Config {
    type Error = ConfigError;

    fn try_from(args: Args) -> Result<Self, Self::Error> {
        let target_url = parse_target_url(&args.target)?;
        let ws_url = args
            .ws_url
            .as_deref()
            .map(parse_websocket_url)
            .transpose()?;

        let region = args.region.map(validate_region).transpose()?;

        let sessions_per_group = args.population.sessions_per_group(args.mode);
        if args.open_loop_admission && args.population != Population::Idle {
            return Err(ConfigError::OpenLoopAdmissionRequiresIdle);
        }
        validate_stages(&args.stages, sessions_per_group)?;
        if args.spawn_rate < sessions_per_group {
            return Err(ConfigError::SpawnRateBelowMatchSize {
                spawn_rate: args.spawn_rate,
                players_per_game: sessions_per_group,
            });
        }
        if args.max_total_sessions < args.stages.max_concurrency() {
            return Err(ConfigError::SessionLimitBelowConcurrency {
                max_total_sessions: args.max_total_sessions,
                max_concurrency: args.stages.max_concurrency(),
            });
        }
        validate_timeout("connect timeout", args.connect_timeout)?;
        validate_timeout("lobby timeout", args.lobby_timeout)?;
        validate_timeout("queue timeout", args.queue_timeout)?;
        validate_timeout("drain timeout", args.drain_timeout)?;
        validate_timeout("untimed play duration", args.untimed_play_duration)?;

        if args.report_dir.as_os_str().is_empty() {
            return Err(ConfigError::InvalidReportDirectory);
        }

        let run_id = args.run_id.unwrap_or_else(generate_run_id);
        validate_run_id(&run_id)?;

        let production_target = is_snaketron_production_url(&target_url)
            || ws_url.as_ref().is_some_and(is_snaketron_production_url);
        if production_target && !args.confirm_production {
            return Err(ConfigError::ProductionConfirmationRequired);
        }

        Ok(Self {
            target_url,
            region,
            ws_url,
            mode: args.mode,
            queue_mode: args.queue_mode,
            population: args.population,
            stages: args.stages,
            spawn_rate: args.spawn_rate,
            open_loop_admission: args.open_loop_admission,
            max_total_sessions: args.max_total_sessions,
            connect_timeout: args.connect_timeout,
            lobby_timeout: args.lobby_timeout,
            queue_timeout: args.queue_timeout,
            drain_timeout: args.drain_timeout,
            untimed_play_duration: args.untimed_play_duration,
            report_dir: args.report_dir,
            run_id,
            command_profile: args.command_profile,
            production_confirmed: args.confirm_production,
            require_same_origin: args.require_same_origin,
            require_scale_out: args.require_scale_out,
            require_planned_handoff: args.require_planned_handoff,
        })
    }
}

/// Configuration errors that are discovered after clap has parsed the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    InvalidTargetUrl(String),
    InvalidWebSocketUrl(String),
    InvalidRegion(String),
    EmptyStages,
    InvalidStage {
        stage_index: usize,
        reason: String,
    },
    StageConcurrencyMisaligned {
        stage_index: usize,
        target_concurrency: usize,
        players_per_game: usize,
    },
    StagesNotIncreasing {
        stage_index: usize,
        previous_target: usize,
        target_concurrency: usize,
    },
    SpawnRateBelowMatchSize {
        spawn_rate: usize,
        players_per_game: usize,
    },
    OpenLoopAdmissionRequiresIdle,
    SessionLimitBelowConcurrency {
        max_total_sessions: usize,
        max_concurrency: usize,
    },
    InvalidTimeout {
        name: &'static str,
    },
    InvalidReportDirectory,
    InvalidRunId(String),
    ProductionConfirmationRequired,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTargetUrl(reason) => write!(formatter, "invalid target URL: {reason}"),
            Self::InvalidWebSocketUrl(reason) => {
                write!(formatter, "invalid WebSocket URL: {reason}")
            }
            Self::InvalidRegion(reason) => write!(formatter, "invalid region: {reason}"),
            Self::EmptyStages => formatter.write_str("at least one load stage is required"),
            Self::InvalidStage {
                stage_index,
                reason,
            } => write!(formatter, "invalid stage {}: {reason}", stage_index + 1),
            Self::StageConcurrencyMisaligned {
                stage_index,
                target_concurrency,
                players_per_game,
            } => write!(
                formatter,
                "stage {} target concurrency {} must be a multiple of {} players per game",
                stage_index + 1,
                target_concurrency,
                players_per_game
            ),
            Self::StagesNotIncreasing {
                stage_index,
                previous_target,
                target_concurrency,
            } => write!(
                formatter,
                "stage {} target concurrency {} must be greater than the previous target {}",
                stage_index + 1,
                target_concurrency,
                previous_target
            ),
            Self::SpawnRateBelowMatchSize {
                spawn_rate,
                players_per_game,
            } => write!(
                formatter,
                "spawn rate {spawn_rate} must be at least one complete {players_per_game}-player match group per second"
            ),
            Self::OpenLoopAdmissionRequiresIdle => {
                formatter.write_str("open-loop admission requires the idle population")
            }
            Self::SessionLimitBelowConcurrency {
                max_total_sessions,
                max_concurrency,
            } => write!(
                formatter,
                "maximum total sessions {max_total_sessions} must be at least the configured peak concurrency {max_concurrency}"
            ),
            Self::InvalidTimeout { name } => {
                write!(formatter, "{name} must be greater than zero")
            }
            Self::InvalidReportDirectory => {
                formatter.write_str("report directory must not be empty")
            }
            Self::InvalidRunId(reason) => write!(formatter, "invalid run ID: {reason}"),
            Self::ProductionConfirmationRequired => formatter.write_str(
                "snaketron.io is a production target; pass --confirm-production to acknowledge the load",
            ),
        }
    }
}

impl Error for ConfigError {}

/// Parse a positive duration with integer `ms`, `s`, `m`, or `h` components.
/// Compound values such as `1m30s` are supported.
pub fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration must not be empty".to_string());
    }

    let bytes = value.as_bytes();
    let mut cursor = 0;
    let mut total_millis = 0_u64;

    while cursor < bytes.len() {
        let number_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if number_start == cursor {
            return Err(format!(
                "duration '{value}' must contain an integer before each unit"
            ));
        }

        let amount = value[number_start..cursor]
            .parse::<u64>()
            .map_err(|_| format!("duration component in '{value}' is too large"))?;

        let (unit_millis, unit_len) = if value[cursor..].starts_with("ms") {
            (1_u64, 2)
        } else if value[cursor..].starts_with('s') {
            (1_000, 1)
        } else if value[cursor..].starts_with('m') {
            (60_000, 1)
        } else if value[cursor..].starts_with('h') {
            (3_600_000, 1)
        } else {
            return Err(format!("duration '{value}' must use ms, s, m, or h units"));
        };
        cursor += unit_len;

        let component_millis = amount
            .checked_mul(unit_millis)
            .ok_or_else(|| format!("duration '{value}' is too large"))?;
        total_millis = total_millis
            .checked_add(component_millis)
            .ok_or_else(|| format!("duration '{value}' is too large"))?;
    }

    if total_millis == 0 {
        return Err("duration must be greater than zero".to_string());
    }

    Ok(Duration::from_millis(total_millis))
}

fn parse_target_url(value: &str) -> Result<Url, ConfigError> {
    let mut url =
        Url::parse(value).map_err(|error| ConfigError::InvalidTargetUrl(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ConfigError::InvalidTargetUrl(
            "scheme must be http or https".to_string(),
        ));
    }
    if url.host_str().is_none() {
        return Err(ConfigError::InvalidTargetUrl(
            "URL must include a host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::InvalidTargetUrl(
            "embedded credentials are not allowed".to_string(),
        ));
    }
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn parse_websocket_url(value: &str) -> Result<Url, ConfigError> {
    let url =
        Url::parse(value).map_err(|error| ConfigError::InvalidWebSocketUrl(error.to_string()))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(ConfigError::InvalidWebSocketUrl(
            "scheme must be ws or wss".to_string(),
        ));
    }
    if url.host_str().is_none() {
        return Err(ConfigError::InvalidWebSocketUrl(
            "URL must include a host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::InvalidWebSocketUrl(
            "embedded credentials are not allowed".to_string(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ConfigError::InvalidWebSocketUrl(
            "query strings and fragments are not allowed".to_string(),
        ));
    }
    Ok(url)
}

fn validate_region(region: String) -> Result<String, ConfigError> {
    if region.is_empty() {
        return Err(ConfigError::InvalidRegion(
            "region identifier must not be empty".to_string(),
        ));
    }
    if !region
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ConfigError::InvalidRegion(
            "use only ASCII letters, numbers, '-' or '_'".to_string(),
        ));
    }
    Ok(region)
}

fn validate_stages(stages: &StagePlan, sessions_per_group: usize) -> Result<(), ConfigError> {
    if stages.is_empty() {
        return Err(ConfigError::EmptyStages);
    }

    let mut previous_target = 0;
    for (stage_index, stage) in stages.iter().enumerate() {
        if stage.target_concurrency == 0 {
            return Err(ConfigError::InvalidStage {
                stage_index,
                reason: "target concurrency must be greater than zero".to_string(),
            });
        }
        if stage.duration.is_zero() {
            return Err(ConfigError::InvalidStage {
                stage_index,
                reason: "duration must be greater than zero".to_string(),
            });
        }
        if stage.target_concurrency % sessions_per_group != 0 {
            return Err(ConfigError::StageConcurrencyMisaligned {
                stage_index,
                target_concurrency: stage.target_concurrency,
                players_per_game: sessions_per_group,
            });
        }
        if stage_index > 0 && stage.target_concurrency <= previous_target {
            return Err(ConfigError::StagesNotIncreasing {
                stage_index,
                previous_target,
                target_concurrency: stage.target_concurrency,
            });
        }
        previous_target = stage.target_concurrency;
    }

    Ok(())
}

fn validate_timeout(name: &'static str, timeout: Duration) -> Result<(), ConfigError> {
    if timeout.is_zero() {
        return Err(ConfigError::InvalidTimeout { name });
    }
    Ok(())
}

fn validate_run_id(run_id: &str) -> Result<(), ConfigError> {
    if run_id.is_empty() {
        return Err(ConfigError::InvalidRunId(
            "run ID must not be empty".to_string(),
        ));
    }
    if run_id.len() > MAX_RUN_ID_LEN {
        return Err(ConfigError::InvalidRunId(format!(
            "run ID must be no more than {MAX_RUN_ID_LEN} characters"
        )));
    }
    if !run_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ConfigError::InvalidRunId(
            "use only ASCII letters, numbers, '-' or '_'".to_string(),
        ));
    }
    Ok(())
}

fn generate_run_id() -> String {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("load-{timestamp_ms}-{}", std::process::id())
}

/// Return whether a resolved HTTP or WebSocket URL targets Snaketron's public
/// production domain. The coordinator calls this again after region discovery
/// so a custom discovery endpoint cannot bypass the production confirmation.
pub fn is_snaketron_production_url(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        // A trailing DNS root label is equivalent to the same public host and
        // must not bypass the explicit production acknowledgement.
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        host == "snaketron.io" || host.ends_with(".snaketron.io")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_args() -> Args {
        Args {
            target: DEFAULT_TARGET.to_string(),
            region: None,
            ws_url: None,
            mode: GameMode::Duel,
            queue_mode: QueueMode::Quickmatch,
            population: Population::Game,
            stages: DEFAULT_STAGES.parse().unwrap(),
            spawn_rate: DEFAULT_SPAWN_RATE,
            open_loop_admission: false,
            max_total_sessions: DEFAULT_MAX_TOTAL_SESSIONS,
            connect_timeout: Duration::from_secs(10),
            lobby_timeout: Duration::from_secs(10),
            queue_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(5 * 60),
            untimed_play_duration: Duration::from_secs(2 * 60),
            report_dir: PathBuf::from("reports"),
            run_id: Some("test-run".to_string()),
            command_profile: CommandProfile::Realistic,
            confirm_production: false,
            require_same_origin: false,
            require_scale_out: false,
            require_planned_handoff: false,
        }
    }

    #[test]
    fn parses_example_stage_plan() {
        let plan: StagePlan = "4@30s,8@1m".parse().unwrap();

        assert_eq!(
            plan.as_slice(),
            &[
                LoadStage::new(4, Duration::from_secs(30)),
                LoadStage::new(8, Duration::from_secs(60)),
            ]
        );
        assert_eq!(plan.max_concurrency(), 8);
    }

    #[test]
    fn parses_compound_and_millisecond_durations() {
        assert_eq!(parse_duration("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(
            parse_duration("1500ms").unwrap(),
            Duration::from_millis(1_500)
        );
    }

    #[test]
    fn rejects_malformed_or_zero_durations() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("1.5s").is_err());
        assert!(parse_duration("0s").is_err());
    }

    #[test]
    fn rejects_malformed_stage_plan() {
        assert!("".parse::<StagePlan>().is_err());
        assert!("4".parse::<StagePlan>().is_err());
        assert!("0@1m".parse::<StagePlan>().is_err());
        assert!("4@1m,".parse::<StagePlan>().is_err());
        assert!("four@1m".parse::<StagePlan>().is_err());
    }

    #[test]
    fn exposes_players_per_game() {
        assert_eq!(GameMode::Solo.players_per_game(), 1);
        assert_eq!(GameMode::Duel.players_per_game(), 2);
        assert_eq!(GameMode::TwoVTwo.players_per_game(), 4);
        assert_eq!(GameMode::FreeForAll.players_per_game(), 4);
    }

    #[test]
    fn non_game_populations_use_one_session_groups() {
        for population in [Population::Idle, Population::Lobby, Population::Matchmaking] {
            assert_eq!(population.sessions_per_group(GameMode::TwoVTwo), 1);
        }
        assert_eq!(Population::Game.sessions_per_group(GameMode::TwoVTwo), 4);
    }

    #[test]
    fn parses_every_supported_game_mode() {
        for value in ["solo", "duel", "2v2", "ffa"] {
            let args = Args::try_parse_from(["loadtest", "--mode", value]).unwrap();
            assert_eq!(args.mode.as_str(), value);
        }
    }

    #[test]
    fn accepts_aligned_increasing_duel_stages() {
        let config = test_args().into_config().unwrap();

        assert_eq!(config.sessions_per_group(), 2);
        assert_eq!(config.max_concurrency(), 256);
        assert!(!config.is_production());
    }

    #[test]
    fn rejects_concurrency_not_aligned_to_game_size() {
        let mut args = test_args();
        args.mode = GameMode::TwoVTwo;
        args.stages = "4@10s,6@10s".parse().unwrap();

        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::StageConcurrencyMisaligned {
                stage_index: 1,
                target_concurrency: 6,
                players_per_game: 4,
            }
        );
    }

    #[test]
    fn one_session_probe_population_does_not_require_game_size_alignment() {
        let mut args = test_args();
        args.mode = GameMode::TwoVTwo;
        args.population = Population::Matchmaking;
        args.stages = "3@10s".parse().unwrap();
        args.spawn_rate = 1;

        let config = args.into_config().unwrap();
        assert_eq!(config.sessions_per_group(), 1);
        assert_eq!(config.expected_games(3), 0);
    }

    #[test]
    fn open_loop_admission_is_limited_to_idle_probes() {
        let mut args = test_args();
        args.open_loop_admission = true;
        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::OpenLoopAdmissionRequiresIdle
        );

        let mut args = test_args();
        args.population = Population::Idle;
        args.open_loop_admission = true;
        let config = args.into_config().unwrap();
        assert!(config.open_loop_admission);
    }

    #[test]
    fn clap_exposes_open_loop_idle_admission() {
        let args =
            Args::try_parse_from(["loadtest", "--population", "idle", "--open-loop-admission"])
                .unwrap();
        assert_eq!(args.population, Population::Idle);
        assert!(args.open_loop_admission);
    }

    #[test]
    fn rejects_non_increasing_stage_targets() {
        let mut args = test_args();
        args.stages = "4@10s,4@10s".parse().unwrap();

        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::StagesNotIncreasing {
                stage_index: 1,
                previous_target: 4,
                target_concurrency: 4,
            }
        );
    }

    #[test]
    fn production_target_requires_confirmation() {
        let mut args = test_args();
        args.target = "https://snaketron.io".to_string();

        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::ProductionConfirmationRequired
        );
    }

    #[test]
    fn production_subdomain_requires_confirmation() {
        let mut args = test_args();
        args.ws_url = Some("wss://use1.snaketron.io/ws".to_string());

        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::ProductionConfirmationRequired
        );
    }

    #[test]
    fn explicit_confirmation_allows_production_target() {
        let mut args = test_args();
        args.target = "https://api.snaketron.io".to_string();
        args.confirm_production = true;

        let config = args.into_config().unwrap();
        assert!(config.is_production());
        assert!(config.production_confirmed);
    }

    #[test]
    fn similarly_named_non_production_host_does_not_require_confirmation() {
        let mut args = test_args();
        args.target = "https://snaketron.io.example.com".to_string();

        assert!(!args.into_config().unwrap().is_production());
    }

    #[test]
    fn fully_qualified_production_host_requires_confirmation() {
        let mut args = test_args();
        args.target = "https://snaketron.io.".to_string();

        assert_eq!(
            args.into_config().unwrap_err(),
            ConfigError::ProductionConfirmationRequired
        );
    }

    #[test]
    fn validates_target_and_websocket_schemes() {
        let mut args = test_args();
        args.target = "wss://example.test/ws".to_string();
        assert!(matches!(
            args.into_config(),
            Err(ConfigError::InvalidTargetUrl(_))
        ));

        let mut args = test_args();
        args.ws_url = Some("https://example.test/ws".to_string());
        assert!(matches!(
            args.into_config(),
            Err(ConfigError::InvalidWebSocketUrl(_))
        ));
    }

    #[test]
    fn normalizes_target_metadata_and_rejects_websocket_query_secrets() {
        let mut args = test_args();
        args.target = "https://staging.example.test/private?token=do-not-log#secret".to_string();
        assert_eq!(
            args.into_config().unwrap().target_url.as_str(),
            "https://staging.example.test/"
        );

        for websocket in [
            "wss://staging.example.test/ws?token=do-not-log",
            "wss://staging.example.test/ws#secret",
        ] {
            let mut args = test_args();
            args.ws_url = Some(websocket.to_string());
            let error = args.into_config().unwrap_err().to_string();
            assert!(error.contains("query strings and fragments are not allowed"));
            assert!(!error.contains("do-not-log"));
            assert!(!error.contains("secret"));
        }
    }

    #[test]
    fn validates_run_id_for_safe_report_paths() {
        let mut args = test_args();
        args.run_id = Some("../unsafe".to_string());

        assert!(matches!(
            args.into_config(),
            Err(ConfigError::InvalidRunId(_))
        ));
    }

    #[test]
    fn rejects_spawn_rate_smaller_than_one_match_group() {
        let mut args = test_args();
        args.mode = GameMode::FreeForAll;
        args.spawn_rate = 3;

        assert!(matches!(
            args.into_config(),
            Err(ConfigError::SpawnRateBelowMatchSize { .. })
        ));
    }

    #[test]
    fn rejects_total_session_limit_below_peak_concurrency() {
        let mut args = test_args();
        args.max_total_sessions = 128;

        assert!(matches!(
            args.into_config(),
            Err(ConfigError::SessionLimitBelowConcurrency { .. })
        ));
    }

    #[test]
    fn clap_accepts_2v2_and_command_profile() {
        let args = Args::try_parse_from([
            "snaketron-loadtest",
            "--mode",
            "2v2",
            "--command-profile",
            "every-tick",
        ])
        .unwrap();

        assert_eq!(args.mode, GameMode::TwoVTwo);
        assert_eq!(args.command_profile, CommandProfile::EveryTick);
    }

    #[test]
    fn clap_exposes_the_fail_closed_same_origin_gate() {
        let config = Args::try_parse_from(["snaketron-loadtest", "--require-same-origin"])
            .unwrap()
            .into_config()
            .unwrap();

        assert!(config.require_same_origin);
    }

    #[test]
    fn clap_accepts_the_untimed_play_window_and_rejects_zero() {
        let args = Args::try_parse_from(["snaketron-loadtest", "--untimed-play-duration", "3m30s"])
            .unwrap();
        assert_eq!(args.untimed_play_duration, Duration::from_secs(210));

        assert!(
            Args::try_parse_from(["snaketron-loadtest", "--untimed-play-duration", "0s",]).is_err()
        );
    }
}
