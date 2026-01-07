use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use clap::Parser;
use common::{
    Direction, GameCommand, GameEngine, GameEvent, GameEventMessage, GameState, GameStatus,
    GameType, QueueMode, calculate_ai_move,
};
use futures_util::{Sink, SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use server::ws_server::WSMessage;
use std::pin::Pin;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, Interval, MissedTickBehavior, Sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

const GAME_OVER_TIMEOUT: Duration = Duration::from_secs(120);

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

        let handle = tokio::spawn(async move {
            if let Err(err) = run_bot(
                idx,
                games,
                base_url,
                ws_url,
                game_type,
                queue_mode,
                http_client,
            )
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

async fn run_bot(
    idx: usize,
    total_games: usize,
    base_url: Url,
    ws_url: Url,
    game_type: GameType,
    queue_mode: QueueMode,
    http_client: Client,
) -> Result<()> {
    let (status_tx, status_rx) = watch::channel::<String>(String::from("starting"));
    let logger = tokio::spawn(log_progress(idx, status_rx));

    // Generate nickname under 20 characters: "bot" + bot number + last 8 chars of UUID
    let uuid_suffix = &Uuid::new_v4().simple().to_string()[24..32];
    let nickname = format!("bot{}-{}", idx + 1, uuid_suffix);
    let guest = create_guest(&http_client, &base_url, &nickname).await?;
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
    let (mut ws_writer, mut ws_reader) = ws_stream.split();

    send_ws(&mut ws_writer, WSMessage::Token(token.to_string())).await?;
    send_status(status_tx, game_idx, total_games, "queued");
    send_ws(
        &mut ws_writer,
        WSMessage::QueueForMatch {
            game_type: game_type.clone(),
            queue_mode: queue_mode.clone(),
        },
    )
    .await?;

    let mut engine: Option<GameEngine> = None;
    let mut snake_id: Option<u32> = None;
    let mut tick_interval: Option<Interval> = None;
    let mut game_id: Option<u32> = None;
    let mut game_started = false;
    let mut game_completed = false;
    let mut hang_timer = tokio::time::sleep(GAME_OVER_TIMEOUT);
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
                if let (Some(engine), Some(snake_id)) = (engine.as_mut(), snake_id) {
                    let tick = engine.get_committed_state().current_tick();
                    drive_bot(idx, engine, &mut ws_writer, snake_id).await?;
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
        WSMessage::JoinGame(id) => {
            info!("Bot {} matched to game {}", idx + 1, id);
            *game_id = Some(id);
            send_ws(ws_writer, WSMessage::JoinGame(id)).await?;
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
        WSMessage::Shutdown => {
            warn!("Bot {} received shutdown signal from server", idx + 1);
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

            if let Some(interval) = build_interval(game_state) {
                *tick_interval = Some(interval);
            }

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
) -> Result<()>
where
    S: Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    engine.rebuild_predicted_state(Utc::now().timestamp_millis())?;
    let Some(predicted_state) = (&*engine).predicted_state() else {
        return Ok(());
    };

    let Some(snake) = predicted_state.arena.snakes.get(snake_id as usize) else {
        return Ok(());
    };

    if !snake.is_alive {
        return Ok(());
    }

    let direction =
        calculate_ai_move(predicted_state, snake_id, snake.direction).unwrap_or(snake.direction);

    let command = GameCommand::Turn {
        snake_id,
        direction,
    };
    let command_msg = engine.process_local_command(command)?;
    debug!(
        "Bot {} sending command for tick {} direction {:?}",
        idx + 1,
        command_msg.command_id_client.tick,
        direction
    );
    send_ws(ws_writer, WSMessage::GameCommand(command_msg)).await?;
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

async fn create_guest(client: &Client, base_url: &Url, nickname: &str) -> Result<GuestResponse> {
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
    if let Some(host) = url.host_str() {
        match host {
            "snaketron.io" | "www.snaketron.io" => {
                // Default to US region for WebSocket
                url.set_host(Some("use1.snaketron.io"))
                    .map_err(|_| anyhow!("Failed to set host"))?;
                info!("Converted main site URL to US region endpoint: {}", url);
            }
            _ => {}
        }
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
    let tick_ms = game_state.properties.tick_duration_ms as u64;
    if tick_ms == 0 {
        return None;
    }

    let now_ms = Utc::now().timestamp_millis();
    let start_ms = game_state.start_ms;
    let elapsed_ms = (now_ms - start_ms).max(0) as u64;
    let ticks_elapsed = elapsed_ms / tick_ms;
    let next_tick_ms = start_ms + ((ticks_elapsed + 1) * tick_ms) as i64;
    let delay_ms = (next_tick_ms - now_ms).max(0) as u64;

    let mut interval = tokio::time::interval_at(
        Instant::now() + Duration::from_millis(delay_ms),
        Duration::from_millis(tick_ms),
    );
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    Some(interval)
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
