use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use clap::Parser;
use common::{
    ClientCommandIdentityV2, GameCommand, GameEngine, GameEvent, GameEventMessage, GameState,
    GameStatus, GameType, QueueMode, calculate_ai_command,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use server::ads::AdBreakResolution;
use server::lifecycle::WS_PROTOCOL_VERSION as CLIENT_PROTOCOL_VERSION;
use server::ws_server::WSMessage;
use std::pin::Pin;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, Interval, MissedTickBehavior, Sleep};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WebSocketError, Message},
};
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

const GAME_OVER_TIMEOUT: Duration = Duration::from_secs(120);
const MATCHMAKING_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const NORMAL_MOVEMENT_INTERVAL_MS: u64 = 100;

#[derive(Parser, Debug)]
#[command(
    name = "snaketron-bot",
    about = "Run one or more AI bots against a Snaketron server"
)]
struct Args {
    /// Base HTTP URL for the API/WebSocket server (e.g. http://localhost:8080)
    #[arg(long, default_value = "http://localhost:8080")]
    url: String,

    /// Game mode to queue for: duel | 2v2 | solo | ffa
    #[arg(long, default_value = "duel")]
    mode: String,

    /// Number of bots to run concurrently
    #[arg(long, default_value_t = 1)]
    bots: usize,

    /// Number of games each bot should play sequentially
    #[arg(long, default_value_t = 1)]
    games: usize,

    /// Queue mode: quickmatch | competitive
    #[arg(long, default_value = "quickmatch")]
    queue_mode: String,

    /// Server-derived stress admission key. Bots are always placed in the
    /// trusted synthetic pool so their games are never written to the
    /// production replay corpus.
    #[arg(long)]
    stress_test_key: String,
}

struct BotRunConfig {
    idx: usize,
    total_games: usize,
    base_url: Url,
    ws_url: Url,
    game_type: GameType,
    queue_mode: QueueMode,
    http_client: Client,
    stress_test_key: String,
}

#[derive(Debug, Deserialize)]
struct GuestResponse {
    token: String,
    user: GuestUser,
}

#[derive(Debug, Deserialize)]
struct GuestUser {
    id: i32,
    username: String,
}

struct BotCommandSession {
    user_id: u32,
    id: String,
    next_sequence: u64,
}

impl BotCommandSession {
    fn new(user_id: u32) -> Self {
        Self {
            user_id,
            id: Uuid::new_v4().to_string(),
            next_sequence: 1,
        }
    }

    fn next_identity(&mut self, game_id: u32) -> Result<ClientCommandIdentityV2> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .context("bot command sequence exhausted")?;
        Ok(ClientCommandIdentityV2 {
            game_id,
            user_id: self.user_id,
            client_game_session_id: self.id.clone(),
            sequence,
        })
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

    let args = Args::parse();
    let base_url = normalize_base_url(&args.url)?;
    let ws_url = websocket_url(&base_url)?;
    let game_type = parse_game_type(&args.mode);
    let queue_mode = parse_queue_mode(&args.queue_mode)?;
    let http_client = Client::new();

    info!(
        "Starting {} bot(s) targeting {} ({}) in {:?} mode, {} game(s) each",
        args.bots, base_url, ws_url, queue_mode, args.games
    );

    let mut handles = Vec::new();
    for idx in 0..args.bots {
        let base_url = base_url.clone();
        let ws_url = ws_url.clone();
        let game_type = game_type.clone();
        let queue_mode = queue_mode.clone();
        let http_client = http_client.clone();
        let games = args.games;
        let stress_test_key = args.stress_test_key.clone();

        let handle = tokio::spawn(async move {
            if let Err(err) = run_bot(BotRunConfig {
                idx,
                total_games: games,
                base_url,
                ws_url,
                game_type,
                queue_mode,
                http_client,
                stress_test_key,
            })
            .await
            {
                error!("Bot {} failed: {:#}", idx + 1, err);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}

async fn run_bot(config: BotRunConfig) -> Result<()> {
    let BotRunConfig {
        idx,
        total_games,
        base_url,
        ws_url,
        game_type,
        queue_mode,
        http_client,
        stress_test_key,
    } = config;
    let (status_tx, status_rx) = watch::channel::<String>(String::from("starting"));
    let logger = tokio::spawn(log_progress(idx, status_rx));

    // Generate nickname under 20 characters: "bot" + bot number + last 8 chars of UUID
    let uuid_suffix = &Uuid::new_v4().simple().to_string()[24..32];
    let nickname = format!("bot{}-{}", idx + 1, uuid_suffix);
    let guest = create_guest(&http_client, &base_url, &nickname, &stress_test_key).await?;
    let user_id = guest.user.id as u32;
    info!(
        "Bot {} authenticated as {} (user_id {})",
        idx + 1,
        guest.user.username,
        user_id
    );

    for game_idx in 1..=total_games {
        send_status(&status_tx, game_idx, total_games, "starting new game");
        match play_single_game(
            idx,
            game_idx,
            total_games,
            &ws_url,
            &guest.token,
            &game_type,
            &queue_mode,
            user_id,
            &status_tx,
        )
        .await
        {
            Ok(_) => send_status(&status_tx, game_idx, total_games, "completed game"),
            Err(err) => {
                send_status(&status_tx, game_idx, total_games, format!("error: {err}"));
                drop(status_tx);
                let _ = logger.await;
                return Err(err);
            }
        }
    }

    drop(status_tx);
    let _ = logger.await;
    info!("Bot {} finished all {} game(s)", idx + 1, total_games);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn play_single_game(
    idx: usize,
    game_idx: usize,
    total_games: usize,
    ws_url: &Url,
    token: &str,
    game_type: &GameType,
    queue_mode: &QueueMode,
    user_id: u32,
    status_tx: &watch::Sender<String>,
) -> Result<()> {
    send_status(status_tx, game_idx, total_games, "connecting to websocket");
    let (ws_stream, _) = connect_async(ws_url.as_str())
        .await
        .with_context(|| format!("Bot {} failed to connect to websocket {}", idx + 1, ws_url))?;
    let mut ws_stream = ws_stream;

    send_status(
        status_tx,
        game_idx,
        total_games,
        "authenticating and creating lobby",
    );
    let lobby_code =
        prepare_matchmaking_session(&mut ws_stream, token, game_type.clone(), queue_mode.clone())
            .await?;
    info!(
        "Bot {} queued explicit lobby {} for game {}/{}",
        idx + 1,
        lobby_code,
        game_idx,
        total_games
    );
    send_status(status_tx, game_idx, total_games, "queued");
    let (mut ws_writer, mut ws_reader) = ws_stream.split();

    let mut engine: Option<GameEngine> = None;
    let mut snake_id: Option<u32> = None;
    let mut tick_interval: Option<Interval> = None;
    let mut game_id: Option<u32> = None;
    let mut game_started = false;
    let mut game_completed = false;
    let mut command_session = BotCommandSession::new(user_id);
    let hang_timer = tokio::time::sleep(GAME_OVER_TIMEOUT);
    tokio::pin!(hang_timer);

    loop {
        tokio::select! {
            _ = &mut hang_timer, if game_started && !game_completed => {
                let game_label = game_id.map(|id| id.to_string()).unwrap_or_else(|| "unknown".to_string());
                let msg = format!(
                    "Bot {} game {}/{} stalled waiting for game over (game {}) after {:?}",
                    idx + 1,
                    game_idx,
                    total_games,
                    game_label,
                    GAME_OVER_TIMEOUT
                );
                error!("{msg}");
                send_status(status_tx, game_idx, total_games, "stalled waiting for game over");
                return Err(anyhow!(msg));
            }
            msg = ws_reader.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;
                if let Message::Text(text) = msg {
                    if let Ok(ws_msg) = serde_json::from_str::<WSMessage>(&text) {
                        let done = handle_ws_message(
                            idx,
                            ws_msg,
                            &mut ws_writer,
                            &mut engine,
                            &mut snake_id,
                            &mut tick_interval,
                            user_id,
                            &mut game_id,
                            &mut game_started,
                            &mut game_completed,
                            &mut hang_timer,
                            status_tx,
                            game_idx,
                            total_games,
                        )
                        .await?;
                        if done {
                            break;
                        }
                    } else if let Ok(event_msg) = serde_json::from_str::<GameEventMessage>(&text) {
                        let done = handle_game_event(
                            idx,
                            event_msg,
                            &mut engine,
                            &mut snake_id,
                            &mut tick_interval,
                            user_id,
                            &mut game_started,
                            &mut game_completed,
                            &mut hang_timer,
                            status_tx,
                            game_idx,
                            total_games,
                        )
                        .await?;
                        if done {
                            break;
                        }
                    } else {
                        debug!("Bot {} received unparsed message: {}", idx + 1, text);
                    }
                }
            }
            _ = async {
                if let Some(interval) = tick_interval.as_mut() {
                    interval.tick().await;
                }
            }, if tick_interval.is_some() => {
                if let (Some(engine), Some(snake_id), Some(game_id)) = (engine.as_mut(), snake_id, game_id) {
                    let tick = engine.get_committed_state().current_tick();
                    drive_bot(
                        idx,
                        engine,
                        &mut ws_writer,
                        snake_id,
                        game_id,
                        &mut command_session,
                    )
                    .await?;
                    send_status(status_tx, game_idx, total_games, format!("playing tick {}", tick));
                }
            }
        }
    }

    let _ = ws_writer.send(Message::Close(None)).await;
    if game_started && !game_completed {
        let game_label = game_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let msg = format!(
            "Bot {} game {}/{} ended without a game over event (game {})",
            idx + 1,
            game_idx,
            total_games,
            game_label
        );
        error!("{msg}");
        send_status(status_tx, game_idx, total_games, "ended without game over");
        return Err(anyhow!(msg));
    }

    Ok(())
}

async fn prepare_matchmaking_session<S>(
    socket: &mut S,
    token: &str,
    game_type: GameType,
    queue_mode: QueueMode,
) -> Result<String>
where
    S: Sink<Message, Error = WebSocketError>
        + Stream<Item = std::result::Result<Message, WebSocketError>>
        + Unpin,
{
    send_ws(
        socket,
        WSMessage::Authenticate {
            token: token.to_owned(),
            protocol_version: CLIENT_PROTOCOL_VERSION,
            anon_id: None,
            distribution: None,
        },
    )
    .await?;
    wait_for_setup_response(
        socket,
        "WebSocket authentication",
        "Authenticated current gameplay protocol",
        |message| {
            matches!(
                message,
                WSMessage::Authenticated {
                    protocol_version: CLIENT_PROTOCOL_VERSION,
                    ..
                }
            )
            .then_some(())
        },
    )
    .await?;

    send_ws(socket, WSMessage::CreateLobby).await?;
    let lobby_code = wait_for_setup_response(socket, "CreateLobby", "LobbyCreated", |message| {
        if let WSMessage::LobbyCreated { lobby_code } = message {
            Some(lobby_code.clone())
        } else {
            None
        }
    })
    .await?;

    send_ws(
        socket,
        WSMessage::QueueForMatch {
            game_type,
            queue_mode,
        },
    )
    .await?;
    Ok(lobby_code)
}

async fn wait_for_setup_response<S, T, F>(
    socket: &mut S,
    phase: &str,
    expected: &str,
    mut matcher: F,
) -> Result<T>
where
    S: Stream<Item = std::result::Result<Message, WebSocketError>> + Unpin,
    F: FnMut(&WSMessage) -> Option<T>,
{
    tokio::time::timeout(MATCHMAKING_SETUP_TIMEOUT, async {
        loop {
            let message = next_ws_message(socket, phase).await?;
            if let Some(response) = matcher(&message) {
                return Ok(response);
            }
            match &message {
                WSMessage::AccessDenied { reason } => {
                    return Err(anyhow!("{phase} was denied: {reason}"));
                }
                WSMessage::Drain {
                    task_boot_id,
                    deadline_unix_ms,
                } => {
                    return Err(anyhow!(
                        "task {task_boot_id} requested drain during {phase} by {deadline_unix_ms}"
                    ));
                }
                _ if is_benign_setup_message(&message) => {
                    debug!("Ignored interleaved message during {phase}: {message:?}");
                }
                _ => {
                    return Err(anyhow!("Expected {expected} response, got {message:?}"));
                }
            }
        }
    })
    .await
    .map_err(|_| anyhow!("Timed out waiting for {expected}"))?
}

fn is_benign_setup_message(message: &WSMessage) -> bool {
    matches!(
        message,
        WSMessage::AdConfiguration(_)
            | WSMessage::UserCountUpdate { .. }
            | WSMessage::LobbyUpdate { .. }
            | WSMessage::LobbyChatHistory { .. }
            | WSMessage::Pong { .. }
    )
}

async fn next_ws_message<S>(socket: &mut S, phase: &str) -> Result<WSMessage>
where
    S: Stream<Item = std::result::Result<Message, WebSocketError>> + Unpin,
{
    loop {
        let frame = socket
            .next()
            .await
            .ok_or_else(|| anyhow!("WebSocket ended during {phase}"))?
            .with_context(|| format!("WebSocket failed during {phase}"))?;
        match frame {
            Message::Text(text) => {
                return serde_json::from_str(&text)
                    .with_context(|| format!("Invalid WebSocket message during {phase}"));
            }
            Message::Close(frame) => {
                return Err(anyhow!("WebSocket closed during {phase}: {frame:?}"));
            }
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_ws_message<S>(
    idx: usize,
    ws_msg: WSMessage,
    ws_writer: &mut S,
    engine: &mut Option<GameEngine>,
    snake_id: &mut Option<u32>,
    tick_interval: &mut Option<Interval>,
    user_id: u32,
    game_id: &mut Option<u32>,
    game_started: &mut bool,
    game_completed: &mut bool,
    hang_timer: &mut Pin<&mut Sleep>,
    status_tx: &watch::Sender<String>,
    game_idx: usize,
    total_games: usize,
) -> Result<bool>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    match ws_msg {
        WSMessage::LobbyUpdate {
            state,
            ad_break: Some(ad_break),
            ..
        } if state == "ad_break" => {
            send_ws(
                ws_writer,
                WSMessage::AdBreakResolved {
                    break_id: ad_break.id,
                    resolution: AdBreakResolution::Unavailable,
                },
            )
            .await?;
        }
        WSMessage::JoinGame(id) => {
            info!("Bot {} matched to game {}", idx + 1, id);
            *game_id = Some(id);
            send_ws(ws_writer, WSMessage::JoinGame(id)).await?;
            // A bot has no briefing to read, and a human waiting on it would
            // sit through the whole readiness window for nothing.
            send_ws(ws_writer, WSMessage::PlayerReady { game_id: id }).await?;
            send_status(
                status_tx,
                game_idx,
                total_games,
                format!("joined game {}", id),
            );
        }
        WSMessage::MatchFound { game_id: found_id } => {
            info!("Bot {} received MatchFound {}", idx + 1, found_id);
            *game_id = Some(found_id);
            send_ws(ws_writer, WSMessage::JoinGame(found_id)).await?;
            send_ws(ws_writer, WSMessage::PlayerReady { game_id: found_id }).await?;
            send_status(
                status_tx,
                game_idx,
                total_games,
                format!("match found {}", found_id),
            );
        }
        WSMessage::GameEvent(event_msg) => {
            return handle_game_event(
                idx,
                event_msg,
                engine,
                snake_id,
                tick_interval,
                user_id,
                game_started,
                game_completed,
                hang_timer,
                status_tx,
                game_idx,
                total_games,
            )
            .await;
        }
        WSMessage::QueueUpdate {
            position,
            estimated_wait_seconds,
        } => {
            info!(
                "Bot {} queue position {} ({}s)",
                idx + 1,
                position,
                estimated_wait_seconds
            );
            send_status(
                status_tx,
                game_idx,
                total_games,
                format!("queue position {} ({}s)", position, estimated_wait_seconds),
            );
        }
        WSMessage::AccessDenied { reason } => {
            return Err(anyhow!("Bot {} access denied: {}", idx + 1, reason));
        }
        WSMessage::Drain {
            task_boot_id,
            deadline_unix_ms,
        } => {
            warn!(
                "Bot {} received drain signal from task {} (deadline {})",
                idx + 1,
                task_boot_id,
                deadline_unix_ms
            );
            return Ok(true);
        }
        _ => {
            debug!("Bot {} ignored message: {:?}", idx + 1, ws_msg);
        }
    }
    if *game_started && !*game_completed {
        hang_timer
            .as_mut()
            .reset(Instant::now() + GAME_OVER_TIMEOUT);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
async fn handle_game_event(
    idx: usize,
    event_msg: GameEventMessage,
    engine: &mut Option<GameEngine>,
    snake_id: &mut Option<u32>,
    tick_interval: &mut Option<Interval>,
    user_id: u32,
    game_started: &mut bool,
    game_completed: &mut bool,
    hang_timer: &mut Pin<&mut Sleep>,
    status_tx: &watch::Sender<String>,
    game_idx: usize,
    total_games: usize,
) -> Result<bool> {
    if *game_started && !*game_completed {
        hang_timer
            .as_mut()
            .reset(Instant::now() + GAME_OVER_TIMEOUT);
    }

    match &event_msg.event {
        GameEvent::Snapshot { game_state } => {
            let mut new_engine = GameEngine::new_from_state(event_msg.game_id, game_state.clone());
            new_engine.set_local_player_id(user_id);
            *snake_id = game_state
                .players
                .get(&user_id)
                .map(|player| player.snake_id);

            // A gated snapshot has no simulation epoch yet. Replacing the
            // option unconditionally also clears an interval left over from a
            // pre-gate/resync snapshot, so the bot cannot issue commands while
            // the authoritative engine is intentionally parked.
            *tick_interval = build_interval(game_state);

            info!(
                "Bot {} received snapshot for game {}, tick {}, snake {:?}",
                idx + 1,
                event_msg.game_id,
                game_state.current_tick(),
                snake_id
            );
            if !*game_started {
                *game_started = true;
                hang_timer
                    .as_mut()
                    .reset(Instant::now() + GAME_OVER_TIMEOUT);
                info!(
                    "Bot {} entered game {} (game {}/{})",
                    idx + 1,
                    event_msg.game_id,
                    game_idx,
                    total_games
                );
                send_status(
                    status_tx,
                    game_idx,
                    total_games,
                    format!("entered game {}", event_msg.game_id),
                );
            }

            *engine = Some(new_engine);
            send_status(
                status_tx,
                game_idx,
                total_games,
                format!("playing tick {}", game_state.current_tick()),
            );
        }
        GameEvent::StatusUpdated { status } => {
            if matches!(status, GameStatus::Complete { .. }) {
                info!("Bot {} saw game {} complete", idx + 1, event_msg.game_id);
                *tick_interval = None;
            }
            if let Some(engine) = engine {
                engine.process_server_event(&event_msg)?;
            }
            if matches!(status, GameStatus::Complete { .. }) {
                *game_completed = true;
                send_status(
                    status_tx,
                    game_idx,
                    total_games,
                    format!("completed match {}", event_msg.game_id),
                );
                info!(
                    "Bot {} finished game {} (game {}/{})",
                    idx + 1,
                    event_msg.game_id,
                    game_idx,
                    total_games
                );
                return Ok(true);
            }
        }
        GameEvent::MatchStartScheduled { .. } => {
            if let Some(engine) = engine {
                engine.process_server_event(&event_msg)?;
                // `process_server_event` installs the authoritative epoch in
                // committed state. Rebuild from that state rather than the
                // immutable legacy `start_ms`, which can be many seconds old
                // after the readiness briefing.
                *tick_interval = build_interval(engine.get_committed_state());
            }
        }
        _ => {
            if let Some(engine) = engine {
                engine.process_server_event(&event_msg)?;
            }
        }
    }

    Ok(false)
}

async fn drive_bot<S>(
    idx: usize,
    engine: &mut GameEngine,
    ws_writer: &mut S,
    snake_id: u32,
    game_id: u32,
    command_session: &mut BotCommandSession,
) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    engine.rebuild_predicted_state(Utc::now().timestamp_millis())?;
    let Some(predicted_state) = engine.predicted_state() else {
        return Ok(());
    };

    let Some(snake) = predicted_state.arena.snakes.get(snake_id as usize) else {
        return Ok(());
    };

    if !snake.is_alive {
        return Ok(());
    }

    let command = calculate_ai_command(predicted_state, snake_id)
        .expect("a living snake should always produce a bot command");
    let command_msg = engine.process_local_command(command)?;
    match &command_msg.command {
        GameCommand::Turn { direction, .. } => debug!(
            "Bot {} sending command for tick {} direction {:?}",
            idx + 1,
            command_msg.command_id_client.tick,
            direction
        ),
        GameCommand::ActivateBoost { .. } => debug!(
            "Bot {} activating stored Boost for tick {}",
            idx + 1,
            command_msg.command_id_client.tick
        ),
        _ => {}
    }
    send_ws(
        ws_writer,
        WSMessage::GameCommandV2 {
            command_id: command_session.next_identity(game_id)?,
            command: command_msg,
        },
    )
    .await?;
    Ok(())
}

async fn send_ws<S>(ws_writer: &mut S, msg: WSMessage) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let payload = serde_json::to_string(&msg)?;
    ws_writer.send(Message::Text(payload.into())).await?;
    Ok(())
}

async fn create_guest(
    client: &Client,
    base_url: &Url,
    nickname: &str,
    stress_test_key: &str,
) -> Result<GuestResponse> {
    // Determine the API URL based on the host
    let api_url = if let Some(host) = base_url.host_str() {
        match host {
            "use1.snaketron.io" | "euw1.snaketron.io" => {
                // Production: use api.snaketron.io
                let mut url = base_url.clone();
                url.set_host(Some("api.snaketron.io"))
                    .map_err(|_| anyhow!("Failed to set API host"))?;
                url
            }
            _ => base_url.clone(),
        }
    } else {
        base_url.clone()
    };

    let endpoint = api_url
        .join("/api/auth/guest")
        .context("Failed to build guest auth URL")?;

    debug!("Guest auth endpoint: {}", endpoint);

    let response = client
        .post(endpoint)
        .header(server::api::auth::STRESS_TEST_KEY_HEADER, stress_test_key)
        .json(&serde_json::json!({ "nickname": nickname }))
        .send()
        .await
        .context("Failed to send guest auth request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Guest auth failed with status {}: {}",
            status,
            body
        ));
    }

    response
        .json::<GuestResponse>()
        .await
        .context("Failed to parse guest auth response")
}

fn normalize_base_url(raw: &str) -> Result<Url> {
    let mut url = Url::parse(raw)
        .or_else(|_| Url::parse(&format!("http://{raw}")))
        .context("Invalid base URL")?;

    // Handle production snaketron.io URLs - convert to regional endpoint
    if let Some("snaketron.io" | "www.snaketron.io") = url.host_str() {
        // Default to US region for WebSocket
        url.set_host(Some("use1.snaketron.io"))
            .map_err(|_| anyhow!("Failed to set host"))?;
        info!("Converted main site URL to US region endpoint: {}", url);
    }

    Ok(url)
}

fn websocket_url(base: &Url) -> Result<Url> {
    let mut ws_url = base.clone();
    let scheme = match base.scheme() {
        "https" | "wss" => "wss",
        _ => "ws",
    };
    ws_url
        .set_scheme(scheme)
        .map_err(|_| anyhow!("Failed to set websocket scheme"))?;
    ws_url.set_path("/ws");
    ws_url.set_query(None);
    Ok(ws_url)
}

fn parse_game_type(mode: &str) -> GameType {
    match mode.to_ascii_lowercase().as_str() {
        "solo" => GameType::Solo,
        "ffa" | "free-for-all" => GameType::FreeForAll { max_players: 4 },
        "2v2" | "team" => GameType::TeamMatch { per_team: 2 },
        _ => GameType::TeamMatch { per_team: 1 }, // Duel default
    }
}

fn parse_queue_mode(mode: &str) -> Result<QueueMode> {
    match mode.to_ascii_lowercase().as_str() {
        "competitive" => Ok(QueueMode::Competitive),
        "quickmatch" | "quick" => Ok(QueueMode::Quickmatch),
        other => Err(anyhow!("Unknown queue mode '{}'", other)),
    }
}

fn build_interval(game_state: &GameState) -> Option<Interval> {
    let now_ms = Utc::now().timestamp_millis();
    let (decision_ms, delay_ms) = decision_schedule_ms(game_state, now_ms)?;

    let mut interval = tokio::time::interval_at(
        Instant::now() + Duration::from_millis(delay_ms),
        Duration::from_millis(decision_ms),
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    Some(interval)
}

/// Return the decision period and delay to its next epoch-aligned boundary.
/// A readiness-gated match deliberately has no schedule until the server
/// publishes `MatchStartScheduled`; legacy ungated states fall back to their
/// immutable `start_ms` through `GameState::simulation_start_ms`.
fn decision_schedule_ms(game_state: &GameState, now_ms: i64) -> Option<(u64, u64)> {
    let tick_ms = game_state.properties.tick_duration_ms as u64;
    if tick_ms == 0 {
        return None;
    }
    let start_ms = game_state.simulation_start_ms()?;
    let decision_ms = decision_interval_ms(game_state);
    let elapsed_ms = (now_ms - start_ms).max(0) as u64;
    let decisions_elapsed = elapsed_ms / decision_ms;
    let next_decision_ms = start_ms + ((decisions_elapsed + 1) * decision_ms) as i64;
    let delay_ms = (next_decision_ms - now_ms).max(0) as u64;
    Some((decision_ms, delay_ms))
}

fn decision_interval_ms(game_state: &GameState) -> u64 {
    let tick_ms = game_state.properties.tick_duration_ms as u64;
    if game_state.properties.boost.is_some() {
        tick_ms.max(NORMAL_MOVEMENT_INTERVAL_MS)
    } else {
        tick_ms
    }
}

fn send_status(
    status_tx: &watch::Sender<String>,
    game_idx: usize,
    total_games: usize,
    status: impl Into<String>,
) {
    let _ = status_tx.send(format!(
        "game {}/{}: {}",
        game_idx,
        total_games,
        status.into()
    ));
}

async fn log_progress(idx: usize, mut status_rx: watch::Receiver<String>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let mut last = String::new();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !last.is_empty() {
                    info!("Bot {} status: {}", idx + 1, last);
                }
            }
            changed = status_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                last = status_rx.borrow().clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[test]
    fn boost_decisions_are_bounded_by_normal_movement_opportunities() {
        let mut boost_game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        assert!(boost_game.properties.boost.is_some());
        assert_eq!(boost_game.properties.tick_duration_ms, 50);
        assert_eq!(decision_interval_ms(&boost_game), 100);

        boost_game.properties.tick_duration_ms = 150;
        assert_eq!(decision_interval_ms(&boost_game), 150);

        // Solo and free-for-all carry Boost now, so their decision cadence is
        // also the normal movement interval rather than their (halved) tick.
        for game_type in [GameType::Solo, GameType::FreeForAll { max_players: 4 }] {
            let field_game = GameState::new(40, 40, game_type, QueueMode::Quickmatch, None, 0);
            assert!(field_game.properties.boost.is_some());
            assert_eq!(field_game.properties.tick_duration_ms, 50);
            assert_eq!(decision_interval_ms(&field_game), 100);
        }

        // A Custom game is the only remaining boostless shape: there the bot
        // decides once per tick, whatever the tick is.
        let mut non_boost_game = GameState::new(
            40,
            40,
            GameType::Custom {
                settings: common::CustomGameSettings::default(),
            },
            QueueMode::Quickmatch,
            None,
            0,
        );
        assert!(non_boost_game.properties.boost.is_none());
        non_boost_game.properties.tick_duration_ms = 50;
        assert_eq!(decision_interval_ms(&non_boost_game), 50);
    }

    #[test]
    fn decision_schedule_waits_for_readiness_and_uses_the_released_epoch() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            1_000,
        );

        // States from before the readiness protocol remain anchored to their
        // original start time.
        assert_eq!(decision_schedule_ms(&game, 1_050), Some((100, 50)));

        game.arm_readiness_gate(9_000);
        assert_eq!(decision_schedule_ms(&game, 8_000), None);

        game.apply_event(
            GameEvent::MatchStartScheduled {
                simulation_epoch_ms: 10_000,
            },
            None,
        );
        // The old start was nine seconds earlier. A schedule based on it would
        // fire immediately here; the released epoch correctly waits until the
        // first 100 ms decision boundary after simulation begins.
        assert_eq!(decision_schedule_ms(&game, 9_900), Some((100, 200)));
        assert_eq!(decision_schedule_ms(&game, 10_050), Some((100, 50)));
    }

    #[tokio::test]
    async fn gated_snapshot_clears_the_timer_until_match_start_is_scheduled() {
        let now_ms = Utc::now().timestamp_millis();
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(7),
            now_ms - 10_000,
        );
        let player = state.add_player(7, Some("ready-bot".to_owned())).unwrap();

        // Model a timer installed from an earlier ungated snapshot. Receiving
        // the gated authoritative snapshot must remove it, not leave it live.
        let mut tick_interval = build_interval(&state);
        assert!(tick_interval.is_some());
        state.arm_readiness_gate(now_ms + 15_000);

        let mut engine = None;
        let mut snake_id = None;
        let mut game_started = false;
        let mut game_completed = false;
        let (status_tx, _status_rx) = watch::channel(String::new());
        let hang_timer = tokio::time::sleep(GAME_OVER_TIMEOUT);
        tokio::pin!(hang_timer);

        handle_game_event(
            0,
            GameEventMessage {
                game_id: 42,
                tick: 0,
                sequence: 1,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::Snapshot { game_state: state },
            },
            &mut engine,
            &mut snake_id,
            &mut tick_interval,
            7,
            &mut game_started,
            &mut game_completed,
            &mut hang_timer,
            &status_tx,
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(snake_id, Some(player.snake_id));
        assert!(tick_interval.is_none());
        assert!(
            engine
                .as_ref()
                .unwrap()
                .get_committed_state()
                .is_awaiting_readiness()
        );

        let simulation_epoch_ms = now_ms + 5_000;
        handle_game_event(
            0,
            GameEventMessage {
                game_id: 42,
                tick: 0,
                sequence: 2,
                stream_seq: 2,
                user_id: None,
                event: GameEvent::MatchStartScheduled {
                    simulation_epoch_ms,
                },
            },
            &mut engine,
            &mut snake_id,
            &mut tick_interval,
            7,
            &mut game_started,
            &mut game_completed,
            &mut hang_timer,
            &status_tx,
            1,
            1,
        )
        .await
        .unwrap();

        assert_eq!(
            engine
                .as_ref()
                .unwrap()
                .get_committed_state()
                .simulation_start_ms(),
            Some(simulation_epoch_ms)
        );
        assert_eq!(
            tick_interval.as_ref().map(Interval::period),
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn bot_activates_collected_boost_before_choosing_another_turn() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let player = game.add_player(41, None).unwrap();
        let pad = game.arena.boost_pads[0].clone();
        let snake = &mut game.arena.snakes[player.snake_id as usize];
        snake.body = vec![
            pad.position,
            common::Position {
                x: pad.position.x - 1,
                y: pad.position.y,
            },
        ];
        snake.direction = common::Direction::Right;

        game.tick_forward(true).unwrap();
        assert_eq!(
            game.arena.snakes[player.snake_id as usize]
                .boost()
                .charge_ms,
            pad.charge_ms
        );
        assert!(matches!(
            calculate_ai_command(&game, player.snake_id),
            Some(GameCommand::ActivateBoost { snake_id }) if snake_id == player.snake_id
        ));
    }

    #[tokio::test]
    async fn matchmaking_setup_waits_for_authentication_and_lobby_before_queueing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();

            assert!(matches!(
                next_ws_message(&mut socket, "test authentication request")
                    .await
                    .unwrap(),
                WSMessage::Authenticate {
                    token,
                    protocol_version: CLIENT_PROTOCOL_VERSION,
                    anon_id: None,
                    distribution: None,
                } if token == "test-token"
            ));
            send_ws(
                &mut socket,
                WSMessage::UserCountUpdate {
                    region_counts: HashMap::from([("test".to_owned(), 1)]),
                },
            )
            .await
            .unwrap();
            send_ws(
                &mut socket,
                WSMessage::Authenticated {
                    task_boot_id: "test-task".to_owned(),
                    protocol_version: CLIENT_PROTOCOL_VERSION,
                    capabilities: Vec::new(),
                    socket_generation: 1,
                },
            )
            .await
            .unwrap();

            assert!(matches!(
                next_ws_message(&mut socket, "test lobby creation")
                    .await
                    .unwrap(),
                WSMessage::CreateLobby
            ));
            send_ws(
                &mut socket,
                WSMessage::UserCountUpdate {
                    region_counts: HashMap::from([("test".to_owned(), 1)]),
                },
            )
            .await
            .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(25), socket.next())
                    .await
                    .is_err(),
                "QueueForMatch arrived before LobbyCreated"
            );
            send_ws(
                &mut socket,
                WSMessage::LobbyCreated {
                    lobby_code: "TEST-LOBBY".to_owned(),
                },
            )
            .await
            .unwrap();

            assert!(matches!(
                next_ws_message(&mut socket, "test queue").await.unwrap(),
                WSMessage::QueueForMatch {
                    game_type: GameType::TeamMatch { per_team: 1 },
                    queue_mode: QueueMode::Quickmatch,
                }
            ));
        });

        let ws_url = format!("ws://{address}/ws");
        let (mut socket, _) = connect_async(&ws_url).await.unwrap();
        let lobby_code = prepare_matchmaking_session(
            &mut socket,
            "test-token",
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        )
        .await
        .unwrap();

        assert_eq!(lobby_code, "TEST-LOBBY");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn matchmaking_setup_surfaces_lobby_denial_without_queueing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();

            assert!(matches!(
                next_ws_message(&mut socket, "test authentication request")
                    .await
                    .unwrap(),
                WSMessage::Authenticate {
                    protocol_version: CLIENT_PROTOCOL_VERSION,
                    ..
                }
            ));
            send_ws(
                &mut socket,
                WSMessage::Authenticated {
                    task_boot_id: "test-task".to_owned(),
                    protocol_version: CLIENT_PROTOCOL_VERSION,
                    capabilities: Vec::new(),
                    socket_generation: 1,
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                next_ws_message(&mut socket, "test lobby creation")
                    .await
                    .unwrap(),
                WSMessage::CreateLobby
            ));
            send_ws(
                &mut socket,
                WSMessage::AccessDenied {
                    reason: "lobby unavailable".to_owned(),
                },
            )
            .await
            .unwrap();

            assert!(
                tokio::time::timeout(Duration::from_millis(25), socket.next())
                    .await
                    .is_err(),
                "QueueForMatch was sent after CreateLobby was denied"
            );
        });

        let ws_url = format!("ws://{address}/ws");
        let (mut socket, _) = connect_async(&ws_url).await.unwrap();
        let error = prepare_matchmaking_session(
            &mut socket,
            "test-token",
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("CreateLobby was denied: lobby unavailable")
        );
        server.await.unwrap();
    }
}
