use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State, ws::WebSocketUpgrade},
    http::{HeaderMap, Request, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, options, post, put},
};
use common::{GAMEPLAY_REPLAY_VERSION, HIGHLIGHT_CLIP_FORMAT_VERSION, HighlightClip};
use serde::Serialize;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use crate::ads::AdsConfig;
use crate::api::admin;
use crate::api::auth::{self, AuthState};
use crate::api::crazygames;
use crate::api::games as public_games;
use crate::api::jwt::JwtManager;
use crate::api::leaderboard::{self, LeaderboardState};
use crate::api::middleware::{AuthMiddlewareState, admin_middleware, auth_middleware};
use crate::api::news::{self, NewsState};
use crate::api::players;
use crate::api::rate_limit::{
    global_rate_limit_middleware, rate_limit_layer, rate_limit_middleware,
};
use crate::api::regions;
use crate::api::skins;
use crate::api::textures;
use crate::api::wallet as wallet_api;
use crate::cluster_membership::ClusterNamespace;
use crate::db::Database;
use crate::db::models::Game;
use crate::game_bus::GameBus;
use crate::lifecycle::TaskLifecycle;
use crate::lobby_manager::LobbyManager;
use crate::redis_keys::RedisKeys;
use crate::region_cache::RegionCache;
use crate::replay_cache::{ReplayCacheConfig, ValkeyReplayCache};
use crate::replay_repository::{ReplayLoadSource, ReplayRepository};
use crate::replay_store::{ReplayStoreConfig, S3ReplayStore};
use crate::replication::GameEventRouter;
use crate::user_cache::UserCache;
use crate::ws_server::{JwtVerifier, handle_websocket};

use crate::redis_utils::RedisConnection;
use redis::AsyncCommands;
use std::sync::atomic::{AtomicUsize, Ordering};

const ACTIVE_SERVER_METRIC_TTL_MS: u64 = 10_000;
const REPLAY_ROUTE_REQUEST_BODY_LIMIT: usize = 1024;
const MAX_REPLAY_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_HIGHLIGHT_RESPONSE_BYTES: usize = 256 * 1024;

/// A dependency-free listener that serves liveness immediately and installs
/// the full application router once Redis-dependent bootstrap has converged.
/// The listener is never rebound, so cold recovery introduces no port gap.
#[derive(Clone)]
pub struct DeferredHttpServer {
    application: Arc<RwLock<Option<Router>>>,
}

#[derive(Clone)]
struct DeferredHttpState {
    lifecycle: TaskLifecycle,
    application: Arc<RwLock<Option<Router>>>,
}

impl DeferredHttpServer {
    pub async fn bind(
        addr: &str,
        lifecycle: TaskLifecycle,
        cancellation: CancellationToken,
    ) -> Result<(Self, JoinHandle<Result<()>>)> {
        let application = Arc::new(RwLock::new(None));
        let state = DeferredHttpState {
            lifecycle: lifecycle.clone(),
            application: application.clone(),
        };
        let app = Router::new()
            .route("/health", get(health_live))
            .route("/health/live", get(health_live))
            .route("/health/ready", get(health_ready))
            .fallback(deferred_application)
            .with_state(state);

        let listener = TcpListener::bind(addr).await?;
        lifecycle.mark_listener_bound();
        info!("HTTP liveness listener bound on {}", addr);
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .tcp_nodelay(true)
                .with_graceful_shutdown(async move {
                    cancellation.cancelled().await;
                    info!("HTTP server received shutdown signal");
                })
                .await
                .map_err(|error| anyhow::anyhow!("HTTP server error: {error}"))
        });
        Ok((Self { application }, task))
    }

    fn install(&self, application: Router) -> Result<()> {
        let mut current = self
            .application
            .write()
            .map_err(|_| anyhow::anyhow!("deferred HTTP router lock poisoned"))?;
        *current = Some(application);
        Ok(())
    }
}

async fn deferred_application(
    State(state): State<DeferredHttpState>,
    request: Request<Body>,
) -> Response {
    let application = match state.application.read() {
        Ok(application) => application.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "application router unavailable",
            )
                .into_response();
        }
    };
    let Some(application) = application else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "task is warming",
        )
            .into_response();
    };
    application
        .oneshot(request)
        .await
        .expect("Axum Router service is infallible")
}

/// Combined HTTP server state containing both API and WebSocket dependencies
#[derive(Clone)]
pub struct HttpServerState {
    /// Database connection
    pub db: Arc<dyn Database>,
    /// JWT manager for API authentication
    pub jwt_manager: Arc<JwtManager>,
    /// JWT verifier for WebSocket authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
    /// Cloneable Redis connection manager
    pub redis: RedisConnection,
    /// Redis URL for creating new connections
    pub redis_url: String,
    /// PubSub manager for loss-tolerant fan-out (chat, lobby, counters)
    pub pubsub_manager: Arc<crate::pubsub_manager::PubSubManager>,
    /// Game-critical message bus (Redis Streams)
    pub game_bus: Arc<GameBus>,
    /// Matchmaking manager for queue operations
    pub matchmaking_manager:
        Arc<tokio::sync::Mutex<crate::matchmaking_manager::MatchmakingManager>>,
    /// Replication manager for game state
    pub event_router: Arc<GameEventRouter>,
    /// Cancellation token for graceful shutdown
    pub cancellation_token: tokio_util::sync::CancellationToken,
    /// Active WebSocket connection count
    pub connection_count: Arc<AtomicUsize>,
    /// Server ID for Redis metrics
    pub server_id: u64,
    /// Region name for Redis metrics
    pub region: String,
    /// Region cache for dynamic region discovery
    pub region_cache: Arc<RegionCache>,
    /// Lobby manager for pre-game grouping
    pub lobby_manager: Arc<LobbyManager>,
    /// User cache for quick user lookups
    pub user_cache: UserCache,
    /// Verified cache-aside access to durable completed-game recordings.
    /// Absent only when replay object storage is intentionally not configured.
    pub replay_repository: Option<Arc<ReplayRepository>>,
    /// Where generated texture pixels live. Absent when this deployment
    /// stores no textures, which serves 503 rather than 404 — the row exists
    /// and its bytes are unreachable, which is a deployment state.
    pub texture_store: Option<Arc<dyn crate::texture_store::TextureStore>>,
    /// Process lifecycle used for truthful readiness and planned drain.
    pub lifecycle: TaskLifecycle,
    /// Region-scoped authoritative recovery namespace.
    pub cluster_namespace: ClusterNamespace,
    /// Deployment advertisement capabilities advertised to every WebSocket session.
    pub ads_config: Arc<AdsConfig>,
}

/// Install the combined API and WebSocket application behind the already-bound
/// liveness listener.
#[allow(clippy::too_many_arguments)]
pub async fn install_http_application(
    deferred: &DeferredHttpServer,
    db: Arc<dyn Database>,
    jwt_manager: Arc<JwtManager>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis: RedisConnection,
    redis_url: String,
    pubsub_manager: Arc<crate::pubsub_manager::PubSubManager>,
    game_bus: Arc<GameBus>,
    matchmaking_manager: Arc<tokio::sync::Mutex<crate::matchmaking_manager::MatchmakingManager>>,
    event_router: Arc<GameEventRouter>,
    cancellation_token: tokio_util::sync::CancellationToken,
    server_id: u64,
    region: String,
    region_cache: Arc<RegionCache>,
    lobby_manager: Arc<LobbyManager>,
    lifecycle: TaskLifecycle,
    cluster_namespace: ClusterNamespace,
    ads_config: Arc<AdsConfig>,
    // Present when analytics is configured. Optional so a deployment without
    // it builds and runs unchanged.
    analytics: Option<crate::api::auth::AnalyticsHandle>,
) -> Result<()> {
    let connection_count = Arc::new(AtomicUsize::new(0));
    let user_cache = UserCache::new(redis.clone(), db.clone());
    // First caller of `texture_store::from_env`, which has existed since the
    // generation pipeline landed and had nothing to construct it: the bytes
    // were stored and served by nothing.
    let texture_store: Option<Arc<dyn crate::texture_store::TextureStore>> =
        crate::texture_store::from_env()
            .await?
            .map(|store| Arc::new(store) as Arc<dyn crate::texture_store::TextureStore>);
    let replay_repository = match ReplayStoreConfig::from_env()? {
        Some(config) => {
            let store = Arc::new(S3ReplayStore::new(config).await?);
            let cache = Arc::new(ValkeyReplayCache::new(
                redis.clone(),
                ReplayCacheConfig::from_env()?,
            )?);
            Some(Arc::new(ReplayRepository::new(store, cache)))
        }
        None => None,
    };

    // Create state for both API and WebSocket handlers
    let state = HttpServerState {
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        jwt_verifier,
        redis: redis.clone(),
        redis_url,
        pubsub_manager,
        game_bus,
        matchmaking_manager,
        event_router,
        cancellation_token: cancellation_token.clone(),
        connection_count: connection_count.clone(),
        server_id,
        region: region.clone(),
        region_cache,
        lobby_manager,
        user_cache,
        replay_repository,
        texture_store,
        lifecycle: lifecycle.clone(),
        cluster_namespace,
        ads_config,
    };

    // Start background task to update user count in Redis every 5 seconds
    spawn_metrics_updater(
        redis.clone(),
        server_id,
        region.clone(),
        connection_count,
        cancellation_token.clone(),
    );

    // Start background task to broadcast user counts to WebSocket clients every 5 seconds
    spawn_user_count_broadcaster(redis.clone(), cancellation_token.clone());
    spawn_texture_worker(
        db.clone(),
        state.texture_store.clone(),
        cancellation_token.clone(),
    );

    // Start background task to broadcast the region's online-player roster.
    spawn_region_roster_broadcaster(redis.clone(), region.clone(), cancellation_token.clone());

    // Create auth state for API routes
    let auth_state = AuthState {
        analytics: analytics.clone(),
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        user_cache: Some(state.user_cache.clone()),
        crazygames_verifier: crazygames::configured_verifier_from_env()?,
        texture_store: state.texture_store.clone(),
        // Read once at install time rather than per request: a merchant
        // account that is half-configured should stop a deployment, not
        // surprise the first player who clicks buy.
        payments: crate::xsolla::Payments::from_env()?.map(Arc::new),
    };
    let auth_middleware_state = AuthMiddlewareState {
        jwt_manager: jwt_manager.clone(),
        db: db.clone(),
    };

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Create rate limiter for username check endpoint
    let username_check_limiter = rate_limit_layer(1000, 60);
    let crazygames_exchange_limiter = rate_limit_layer(60, 60);
    // Replay reads are public by product policy but can trigger object-store
    // I/O, decompression, parsing, and deterministic hash verification. A
    // process-global backstop cannot be evaded with spoofed proxy headers;
    // cache-aside still makes ordinary repeat views cheap.
    let replay_read_limiter = rate_limit_layer(600, 60);
    // Public match pages are cheap single-item reads and are the surface a
    // crawler or a viral link actually hits, so they get their own budget
    // rather than sharing the replay backstop.
    let public_game_read_limiter = rate_limit_layer(3000, 60);
    // An invite link is followed once per click, but the endpoint answers
    // "does this account exist" to anyone. Per-IP, and tight enough that it is
    // a poor way to enumerate usernames.
    let player_lobby_limiter = rate_limit_layer(120, 60);
    // Generation is the one route where a request turns into money, so it is
    // throttled as well as quota'd and circuit-broken inside the handler.
    let generation_limiter = rate_limit_layer(20, 60);
    // Uploads are cheaper than generations — no model, no bill — but they are
    // still four megabytes of pixels each, so they get their own allowance
    // rather than sharing one with the route that spends money.
    let upload_limiter = rate_limit_layer(30, 60);

    // Build protected API routes
    let protected_routes = Router::new()
        .route("/api/auth/me", get(auth::get_current_user))
        .route(
            "/api/factory/capabilities",
            get(auth::get_factory_capabilities),
        )
        .route("/api/history", get(admin::get_user_history))
        .route(
            "/api/auth/crazygames/preferences",
            put(crazygames::save_preferences)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024)),
        )
        // Two skin references and nothing else; a body larger than this is not
        // an equip request that got long, it is something else entirely.
        .route(
            "/api/users/me/equipped",
            put(skins::set_equipment).layer(axum::extract::DefaultBodyLimit::max(4 * 1024)),
        )
        // A document is capped at 32 KB; the request limit leaves room for the
        // envelope around it and nothing like enough for anything else.
        .route(
            "/api/skins",
            post(skins::create_skin).layer(axum::extract::DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/api/skins/:skin_id",
            put(skins::update_skin).layer(axum::extract::DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/api/skins/:skin_id/publish-request",
            post(skins::request_publication)
                .delete(skins::cancel_publication_request)
                .layer(axum::extract::DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/api/skins/:skin_id/report",
            post(skins::report_skin).layer(axum::extract::DefaultBodyLimit::max(8 * 1024)),
        )
        .route("/api/wallet", get(wallet_api::get_wallet))
        .route("/api/wallet/packs", get(wallet_api::list_packs))
        .route("/api/textures", get(textures::list_mine))
        .route(
            "/api/textures/:texture_id",
            put(textures::update_texture).layer(axum::extract::DefaultBodyLimit::max(2 * 1024)),
        )
        // Generation is slow and costs money per attempt, so the route that
        // starts one is rate limited as well as quota'd inside the handler.
        .route(
            "/api/textures/generate",
            post(textures::generate)
                .layer(axum::extract::DefaultBodyLimit::max(8 * 1024))
                .layer(middleware::from_fn_with_state(
                    generation_limiter,
                    rate_limit_middleware,
                )),
        )
        // A PNG plus its metadata. The 4 MB cap is the PRD's, and it is the
        // largest body this API takes: the handler checks the magic bytes and
        // the declared dimensions, and everything that costs CPU happens in
        // the worker.
        .route(
            "/api/textures",
            post(textures::upload)
                .layer(axum::extract::DefaultBodyLimit::max(4 * 1024 * 1024))
                .layer(middleware::from_fn_with_state(
                    upload_limiter.clone(),
                    rate_limit_middleware,
                )),
        )
        .route(
            "/api/textures/forge",
            post(textures::ingest_forge_manifest)
                .layer(axum::extract::DefaultBodyLimit::max(12 * 1024 * 1024))
                .layer(middleware::from_fn_with_state(
                    upload_limiter,
                    rate_limit_middleware,
                )),
        )
        .route("/api/generation-jobs/:job_id", get(textures::get_job))
        .route(
            "/api/wallet/xsolla/checkout-token",
            post(wallet_api::xsolla_checkout_token)
                .layer(axum::extract::DefaultBodyLimit::max(2 * 1024)),
        )
        .route(
            "/api/skins/:skin_id/purchase",
            post(wallet_api::purchase_skin).layer(axum::extract::DefaultBodyLimit::max(4 * 1024)),
        )
        .layer(middleware::from_fn_with_state(
            auth_middleware_state.clone(),
            auth_middleware,
        ))
        .with_state(auth_state.clone());

    let admin_routes = Router::new()
        .route("/api/admin/history", get(admin::get_admin_history))
        .route(
            "/api/admin/factory-credentials",
            post(auth::create_factory_credential),
        )
        .route(
            "/api/admin/factory-credentials/:credential_id/rotate",
            post(auth::rotate_factory_credential),
        )
        .route(
            "/api/admin/factory-credentials/:credential_id",
            delete(auth::revoke_factory_credential),
        )
        .route("/api/admin/skins", get(skins::admin_review_queue))
        .route(
            "/api/admin/skins/:skin_id/status",
            put(skins::admin_set_status).layer(axum::extract::DefaultBodyLimit::max(8 * 1024)),
        )
        .route(
            "/api/admin/config",
            get(admin::get_admin_config)
                .put(admin::update_admin_config)
                .layer(axum::extract::DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/api/admin/config/audit", get(admin::get_config_audit))
        .layer(middleware::from_fn(admin_middleware))
        // Authentication runs first and installs the DB-derived AuthUser used
        // by the inner administrator authorization layer.
        .layer(middleware::from_fn_with_state(
            auth_middleware_state.clone(),
            auth_middleware,
        ))
        .with_state(auth_state.clone());

    // Build region routes with HttpServerState (for Redis access)
    let region_routes = Router::new()
        .route("/api/regions", get(regions::list_regions))
        .route("/api/regions/user-counts", get(regions::get_user_counts))
        .route(
            "/api/regions/server-counts",
            get(regions::get_server_counts),
        )
        .with_state(state.clone());

    // Resolves `/play/<username>` invite links to a joinable lobby. Anonymous
    // like the link itself, and rate limited because it is an unauthenticated
    // probe against account names.
    let player_routes = Router::new()
        .route(
            "/api/players/:username/lobby",
            get(players::get_player_lobby).layer(middleware::from_fn_with_state(
                player_lobby_limiter,
                rate_limit_middleware,
            )),
        )
        .with_state(state.clone());

    // Debug/observability routes: clients upload their sync trace (flight
    // recorder ring buffer) here when they detect a desync. Body capped at
    // 10 MB; records are validated as TraceRecords before being persisted
    // next to the server-side traces.
    let debug_routes = Router::new()
        .route("/api/debug/client-trace", post(upload_client_trace))
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024));

    // Build leaderboard routes with LeaderboardState
    let leaderboard_state = LeaderboardState { db: db.clone() };
    let leaderboard_routes = Router::new()
        .route("/api/leaderboard", get(leaderboard::get_leaderboard))
        .route(
            "/api/leaderboard/users/:user_id",
            get(leaderboard::get_user_ranking_by_id),
        )
        .route("/api/seasons", get(leaderboard::list_seasons))
        .with_state(leaderboard_state.clone());

    // Build the public arena-news feed separately so its process-wide cache
    // coalesces leaderboard and recent-game reads across all home-page loads.
    let news_routes = Router::new()
        .route("/api/news", get(news::get_news))
        .with_state(NewsState::new(db.clone()));

    // Browsing the catalogue needs no account: the Skins page is a shop window
    // as much as a picker, and a logged-out visitor should see what is on offer.
    let skin_catalog_routes = Router::new()
        .route("/api/skins", get(skins::browse))
        .route("/api/skins/catalog", get(skins::browse));

    // Reading a skin document needs no account: a spectator or a replay viewer
    // holding a reference out of a snapshot has to be able to draw it.
    let payment_routes = Router::new()
        .route(
            "/api/wallet/xsolla/webhook",
            post(wallet_api::xsolla_webhook).layer(axum::extract::DefaultBodyLimit::max(32 * 1024)),
        )
        .with_state(auth_state.clone());

    // Optional auth, not none: every route here serves an anonymous caller,
    // and three of them serve a signed-in one *more* — your own skins, your own
    // skin, and the document behind a draft only you can see. Without this the
    // extension is never installed, `filter=mine` can only answer 401, and a
    // skin you have made but not published is invisible on your own Skins page.
    let skin_document_routes = Router::new()
        .route(
            "/api/skins/by-ref/:content_ref",
            get(skins::get_document_by_ref),
        )
        .route("/api/skins/browse", get(skins::list_skins))
        .route(
            "/api/textures/by-ref/:content_ref/manifest",
            get(textures::get_manifest),
        )
        .route("/api/skins/:skin_id", get(skins::get_skin))
        .layer(middleware::from_fn_with_state(
            auth_middleware_state.clone(),
            crate::api::middleware::optional_auth_middleware,
        ))
        .with_state(auth_state.clone());

    // Replay and highlight reads are intentionally anonymous. At launch all
    // runtime games, including custom games, are public; no username or lobby
    // membership ACL is applied here.
    let replay_routes = build_replay_routes(db.clone(), state.replay_repository.clone()).layer(
        middleware::from_fn_with_state(replay_read_limiter, global_rate_limit_middleware),
    );

    // Permanent public match pages. A link posted anywhere on the internet has
    // to keep resolving, so these read the canonical TTL-free summary and are
    // anonymous for the same reason the replay routes are.
    let public_game_routes = public_games::build_public_game_routes(db.clone()).layer(
        middleware::from_fn_with_state(public_game_read_limiter, global_rate_limit_middleware),
    );
    // Texture pixels, anonymous for the same reason replays are: these are the
    // bytes a client needs to render somebody *else's* snake, so gating them
    // behind a session would stop a match drawing its own players.
    //
    // The budget is far above the replay limiter's because the traffic shape is
    // different: eight players, up to four textures each, all cold at the same
    // instant when a match starts. A per-minute allowance sized for replays
    // would black out arenas rather than shed load.
    let texture_read_limiter = rate_limit_layer(6_000, 60);
    let texture_byte_routes =
        textures::build_texture_byte_routes(db.clone(), state.texture_store.clone()).layer(
            middleware::from_fn_with_state(texture_read_limiter, global_rate_limit_middleware),
        );

    // Build protected leaderboard routes (requires authentication)
    let protected_leaderboard_routes = Router::new()
        .route("/api/leaderboard/me", get(leaderboard::get_my_ranking))
        .layer(middleware::from_fn_with_state(
            auth_middleware_state,
            auth_middleware,
        ))
        .with_state(leaderboard_state);

    // Build API routes with AuthState
    let api_routes = Router::new()
        .route("/api/health", get(regions::health_check_json))
        .route("/api/config", get(admin::get_public_config))
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/guest", post(auth::create_guest))
        .route(
            "/api/auth/crazygames/exchange",
            post(crazygames::exchange)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
                .layer(middleware::from_fn_with_state(
                    crazygames_exchange_limiter,
                    rate_limit_middleware,
                )),
        )
        .route(
            "/api/auth/check-username",
            post(auth::check_username).layer(middleware::from_fn_with_state(
                username_check_limiter,
                rate_limit_middleware,
            )),
        )
        // Catch-all preflight for all API routes to avoid 500s on OPTIONS
        .route("/api/*path", options(|| async { StatusCode::NO_CONTENT }))
        .merge(protected_routes)
        .merge(admin_routes)
        .merge(region_routes)
        .merge(player_routes)
        .merge(leaderboard_routes)
        .merge(news_routes)
        .merge(skin_catalog_routes)
        .merge(skin_document_routes)
        .merge(payment_routes)
        .merge(replay_routes)
        .merge(public_game_routes)
        .merge(texture_byte_routes)
        .merge(protected_leaderboard_routes)
        .merge(debug_routes)
        .with_state(auth_state);

    // Health routes live in the dependency-free outer router so liveness
    // remains available before this application is installed.
    let app = Router::new()
        // WebSocket endpoint
        .route("/ws", get(websocket_handler))
        // Nest API routes
        .nest("/", api_routes)
        .layer(cors)
        .layer(middleware::from_fn(observe_application_request))
        .with_state(state);

    deferred.install(app)?;
    info!("HTTP API and WebSocket routes installed");
    Ok(())
}

#[async_trait]
trait ReplayGameReader: Send + Sync {
    async fn get_game(&self, game_id: i32) -> Result<Option<Game>>;
}

struct DatabaseReplayGameReader {
    db: Arc<dyn Database>,
}

#[async_trait]
impl ReplayGameReader for DatabaseReplayGameReader {
    async fn get_game(&self, game_id: i32) -> Result<Option<Game>> {
        self.db.get_game_by_id(game_id).await
    }
}

#[derive(Clone)]
struct ReplayApiState {
    games: Arc<dyn ReplayGameReader>,
    repository: Option<Arc<ReplayRepository>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum HighlightApiResponse {
    Pending,
    Ready {
        play_of_the_game: Box<HighlightClip>,
    },
    Unavailable,
}

#[derive(Serialize)]
struct ReplayApiError {
    error: &'static str,
}

fn build_replay_routes(
    db: Arc<dyn Database>,
    repository: Option<Arc<ReplayRepository>>,
) -> Router<AuthState> {
    let state = ReplayApiState {
        games: Arc::new(DatabaseReplayGameReader { db }),
        repository,
    };
    replay_route_template().with_state::<AuthState>(state)
}

fn replay_route_template() -> Router<ReplayApiState> {
    Router::new()
        .route("/api/games/:game_id/highlight", get(get_game_highlight))
        .route("/api/games/:game_id/replay", get(get_game_replay))
        .layer(axum::extract::DefaultBodyLimit::max(
            REPLAY_ROUTE_REQUEST_BODY_LIMIT,
        ))
}

fn parse_public_game_id(raw: &str) -> Option<i32> {
    if raw.is_empty() || raw.len() > 10 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    raw.parse::<i32>().ok().filter(|game_id| *game_id > 0)
}

fn parse_replay_byte_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let spec = value.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (start, inclusive_end) = spec.split_once('-')?;
    if start.is_empty() {
        let suffix = inclusive_end.parse::<u64>().ok()?.min(total);
        if suffix == 0 {
            return None;
        }
        return Some((total - suffix, total));
    }
    let start = start.parse::<u64>().ok()?;
    let end = if inclusive_end.is_empty() {
        total
    } else {
        inclusive_end
            .parse::<u64>()
            .ok()?
            .checked_add(1)?
            .min(total)
    };
    (start < end && end <= total).then_some((start, end))
}

fn json_api_error(status: StatusCode, error: &'static str) -> Response {
    (status, Json(ReplayApiError { error })).into_response()
}

fn highlight_api_response(payload: HighlightApiResponse) -> Response {
    let pending = matches!(payload, HighlightApiResponse::Pending);
    let mut response = Json(payload).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static(if pending {
            "no-store"
        } else {
            "public, max-age=300"
        }),
    );
    if pending {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
    }
    response
}

async fn get_game_highlight(
    State(state): State<ReplayApiState>,
    Path(raw_game_id): Path<String>,
) -> Response {
    let Some(game_id) = parse_public_game_id(&raw_game_id) else {
        return json_api_error(StatusCode::BAD_REQUEST, "invalid game id");
    };
    let game = match state.games.get_game(game_id).await {
        Ok(Some(game)) => game,
        // The terminal snapshot is committed to Valkey before PersistGame
        // creates the DynamoDB META row. A first post-match poll therefore
        // commonly races this read. Keep a missing row pending; the bounded
        // client poll will settle to unavailable if the game truly does not
        // exist or persistence never completes.
        Ok(None) => return highlight_api_response(HighlightApiResponse::Pending),
        Err(error) => {
            warn!(game_id, %error, "Failed to load game highlight metadata");
            return json_api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "highlight metadata unavailable",
            );
        }
    };
    if game.status != "complete" {
        return highlight_api_response(HighlightApiResponse::Pending);
    }
    let Some(clip) = game.play_of_the_game else {
        return highlight_api_response(HighlightApiResponse::Unavailable);
    };
    let shape_is_compatible = clip.game_id == game_id as u32
        && clip.clip_format_version == HIGHLIGHT_CLIP_FORMAT_VERSION
        && clip.gameplay_version == GAMEPLAY_REPLAY_VERSION
        && clip.anchor.tick <= clip.window.start_tick
        && clip.window.start_tick <= clip.window.focus_tick
        && clip.window.focus_tick <= clip.window.end_tick
        && clip
            .messages
            .windows(2)
            .all(|pair| (pair[0].tick, pair[0].sequence) < (pair[1].tick, pair[1].sequence))
        && clip.messages.iter().all(|message| {
            message.tick >= clip.anchor.tick && message.tick <= clip.window.end_tick
        });
    if !shape_is_compatible {
        warn!(
            game_id,
            clip_game_id = clip.game_id,
            "Rejecting incompatible highlight metadata"
        );
        return highlight_api_response(HighlightApiResponse::Unavailable);
    }
    // PersistGame only writes clips that were replay-verified before the
    // immutable completion commit. Re-simulating on every anonymous GET would
    // turn this public endpoint into CPU amplification; the browser performs
    // the final end-hash assertion before it presents the clip.
    let ready = HighlightApiResponse::Ready {
        play_of_the_game: Box::new(clip),
    };
    match serde_json::to_vec(&ready) {
        Ok(bytes) if bytes.len() <= MAX_HIGHLIGHT_RESPONSE_BYTES => highlight_api_response(ready),
        Ok(bytes) => {
            warn!(
                game_id,
                response_bytes = bytes.len(),
                "Rejecting oversized highlight response"
            );
            highlight_api_response(HighlightApiResponse::Unavailable)
        }
        Err(error) => {
            warn!(game_id, %error, "Failed to serialize highlight response");
            highlight_api_response(HighlightApiResponse::Unavailable)
        }
    }
}

async fn get_game_replay(
    State(state): State<ReplayApiState>,
    Path(raw_game_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(game_id) = parse_public_game_id(&raw_game_id) else {
        return json_api_error(StatusCode::BAD_REQUEST, "invalid game id");
    };
    let game = match state.games.get_game(game_id).await {
        Ok(Some(game)) => game,
        Ok(None) => return json_api_error(StatusCode::NOT_FOUND, "replay unavailable"),
        Err(error) => {
            warn!(game_id, %error, "Failed to load game replay metadata");
            return json_api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "replay metadata unavailable",
            );
        }
    };
    if game.status != "complete" {
        return json_api_error(StatusCode::NOT_FOUND, "replay unavailable");
    }
    let Some(metadata) = game.replay_object else {
        return json_api_error(StatusCode::NOT_FOUND, "replay unavailable");
    };
    let Some(repository) = state.repository else {
        warn!(
            game_id,
            "Replay metadata exists but replay storage is disabled"
        );
        return json_api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "replay storage unavailable",
        );
    };
    let total_bytes = match repository.recording_length(&metadata).await {
        Ok(Some(total)) => total,
        Ok(None) => return json_api_error(StatusCode::NOT_FOUND, "replay unavailable"),
        Err(error) => {
            warn!(game_id, %error, "Failed to load verified replay length");
            return json_api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "replay storage unavailable",
            );
        }
    };

    if let Some(range_header) = headers.get(header::RANGE) {
        let Some((start, end)) = range_header
            .to_str()
            .ok()
            .and_then(|value| parse_replay_byte_range(value, total_bytes))
        else {
            return json_api_error(StatusCode::RANGE_NOT_SATISFIABLE, "invalid replay range");
        };
        if end - start > MAX_REPLAY_RESPONSE_BYTES as u64 {
            return json_api_error(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "replay range exceeds response limit",
            );
        }
        let loaded = match repository.get_recording_range(&metadata, start, end).await {
            Ok(Some(loaded)) => loaded,
            Ok(None) => return json_api_error(StatusCode::NOT_FOUND, "replay unavailable"),
            Err(error) => {
                warn!(game_id, %error, "Verified replay range lookup failed");
                return json_api_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "replay storage unavailable",
                );
            }
        };
        let source = match loaded.source {
            ReplayLoadSource::Cache => "cache",
            ReplayLoadSource::ObjectStore => "object_store",
        };
        let mut response = Response::new(Body::from(loaded.bytes));
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/octet-stream"),
        );
        response.headers_mut().insert(
            header::ACCEPT_RANGES,
            axum::http::HeaderValue::from_static("bytes"),
        );
        if let Ok(value) = axum::http::HeaderValue::from_str(&format!(
            "bytes {}-{}/{}",
            loaded.start,
            loaded.end - 1,
            loaded.total_bytes
        )) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("x-snaketron-replay-source"),
            axum::http::HeaderValue::from_static(source),
        );
        return response;
    }

    if total_bytes > MAX_REPLAY_RESPONSE_BYTES as u64 {
        warn!(
            game_id,
            response_bytes = total_bytes,
            "Replay requires a bounded byte-range request"
        );
        let mut response = json_api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "replay requires a byte range",
        );
        response.headers_mut().insert(
            header::ACCEPT_RANGES,
            axum::http::HeaderValue::from_static("bytes"),
        );
        if let Ok(value) = axum::http::HeaderValue::from_str(&format!("bytes */{total_bytes}")) {
            response.headers_mut().insert(header::CONTENT_RANGE, value);
        }
        return response;
    }
    let loaded = match repository.get_recording(&metadata).await {
        Ok(Some(loaded)) => loaded,
        Ok(None) => return json_api_error(StatusCode::NOT_FOUND, "replay unavailable"),
        Err(error) => {
            warn!(game_id, %error, "Verified replay lookup failed");
            return json_api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "replay storage unavailable",
            );
        }
    };
    if loaded.recording.bytes.len() > MAX_REPLAY_RESPONSE_BYTES {
        warn!(
            game_id,
            response_bytes = loaded.recording.bytes.len(),
            "Rejecting oversized replay response"
        );
        return json_api_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "replay exceeds response limit",
        );
    }

    let source = match loaded.source {
        ReplayLoadSource::Cache => "cache",
        ReplayLoadSource::ObjectStore => "object_store",
    };
    let content_length = loaded.recording.bytes.len().to_string();
    let mut response = Response::new(Body::from(loaded.recording.bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    if let Ok(value) = axum::http::HeaderValue::from_str(&content_length) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("public, max-age=300"),
    );
    response.headers_mut().insert(
        header::ACCEPT_RANGES,
        axum::http::HeaderValue::from_static("bytes"),
    );
    response.headers_mut().insert(
        axum::http::HeaderName::from_static("x-snaketron-replay-source"),
        axum::http::HeaderValue::from_static(source),
    );
    response
}

async fn observe_application_request(request: Request<Body>, next: middleware::Next) -> Response {
    let started_at = Instant::now();
    let response = next.run(request).await;
    crate::resilience_metrics::record_http_request(
        response.status().as_u16(),
        started_at.elapsed(),
    );
    response
}

#[derive(serde::Deserialize)]
struct ClientTraceUpload {
    game_id: u32,
    user_id: u32,
    records: Vec<serde_json::Value>,
}

/// Persist a client-side sync trace next to the server traces so a desync
/// can be analyzed from both perspectives (see DEBUGGING.md).
async fn upload_client_trace(
    axum::Json(upload): axum::Json<ClientTraceUpload>,
) -> impl IntoResponse {
    // Validate every record parses as a TraceRecord; a malformed trace is
    // rejected rather than persisted half-usable.
    let mut records = Vec::with_capacity(upload.records.len());
    for (idx, value) in upload.records.into_iter().enumerate() {
        match serde_json::from_value::<common::trace::TraceRecord>(value) {
            Ok(record) => records.push(record),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("record {} is not a valid TraceRecord: {}", idx, e),
                )
                    .into_response();
            }
        }
    }

    let config = crate::sync_trace::TraceConfig::from_env();
    let game_id = upload.game_id;
    let user_id = upload.user_id;
    // File I/O is small (<=10MB) but still blocking; keep it off the reactor.
    let result = tokio::task::spawn_blocking(move || {
        crate::sync_trace::write_client_trace(&config, game_id, user_id, &records)
    })
    .await;

    match result {
        Ok(Ok(path)) => {
            info!(
                "Stored client sync trace for game {} user {} at {:?}",
                game_id, user_id, path
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Err(e)) => {
            tracing::warn!(
                "Failed to store client trace for game {} user {}: {}",
                game_id,
                user_id,
                e
            );
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
        Err(e) => {
            tracing::error!("Client trace writer task panicked: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// Authentication and gameplay commands are small JSON messages. Bound frames
// before Axum buffers and deserializes them so oversized unauthenticated input
// cannot allocate tungstenite's much larger default allowance.
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 64 * 1024;

/// WebSocket upgrade handler
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<HttpServerState>,
) -> axum::response::Response {
    if !state.lifecycle.is_ready() {
        crate::resilience_metrics::record_websocket_rejected_upgrade(1);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "task is not ready for new WebSocket sessions",
        )
            .into_response();
    }

    let connection_count = state.connection_count.clone();
    let lifecycle = state.lifecycle.clone();

    ws.max_message_size(MAX_WEBSOCKET_MESSAGE_BYTES)
        .max_frame_size(MAX_WEBSOCKET_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            // Count only upgrades that actually became WebSockets. Incrementing in
            // the HTTP handler leaks the drain counter when a client disappears
            // after the 101 response is prepared but before Axum runs this future.
            let count = connection_count.fetch_add(1, Ordering::Relaxed) + 1;
            lifecycle.websocket_opened();
            crate::resilience_metrics::record_websocket_opened(1);
            tracing::debug!("WebSocket connection opened, total connections: {}", count);

            // Handle the WebSocket connection
            let session_started_at = Instant::now();
            handle_websocket(
                socket,
                state.db,
                state.user_cache,
                state.jwt_verifier,
                state.redis,
                state.redis_url,
                state.pubsub_manager,
                state.game_bus,
                state.matchmaking_manager,
                state.event_router,
                state.cancellation_token,
                state.lobby_manager,
                state.region,
                lifecycle.clone(),
                state.cluster_namespace,
                state.ads_config,
            )
            .await;
            let session_duration = session_started_at.elapsed();
            // The resilience metric stays here. It is a transport fact with no
            // identity in it, and it belongs beside the opened/closed counters
            // and the connection-count bookkeeping in this same closure, which
            // are its only peers. The ANALYTICS session events moved down into
            // `handle_websocket`, where the connection context still exists —
            // emitting them from here is what left them unattributed, because
            // `handle_websocket` returns `()` and nothing about the user
            // survives the call.
            crate::resilience_metrics::record_websocket_session(session_duration);

            // Decrement connection count when connection closes
            let count = connection_count.fetch_sub(1, Ordering::Relaxed) - 1;
            lifecycle.websocket_closed();
            crate::resilience_metrics::record_websocket_closed(1);
            tracing::debug!("WebSocket connection closed, total connections: {}", count);
        })
        .into_response()
}

/// ECS liveness: a Valkey outage must not create a task replacement storm.
async fn health_live(State(state): State<DeferredHttpState>) -> impl IntoResponse {
    debug_assert!(state.lifecycle.is_live());
    (StatusCode::OK, "OK")
}

/// Traefik readiness: only admit new users after local dependencies have
/// converged, and withdraw the task immediately when drain begins.
async fn health_ready(State(state): State<DeferredHttpState>) -> impl IntoResponse {
    let application_installed = state
        .application
        .read()
        .is_ok_and(|application| application.is_some());
    if application_installed && state.lifecycle.is_ready() {
        (StatusCode::OK, "READY")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT_READY")
    }
}

/// Background task to update Redis metrics every 5 seconds
fn spawn_metrics_updater(
    redis: RedisConnection,
    server_id: u64,
    region: String,
    connection_count: Arc<AtomicUsize>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Metrics updater shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let count = connection_count.load(Ordering::Relaxed);

                    if let Err(e) = update_redis_metrics(redis.clone(), server_id, &region, count).await {
                        tracing::error!("Failed to update Redis metrics: {}", e);
                    } else {
                        tracing::trace!("Updated Redis metrics: server_id={}, region={}, count={}", server_id, region, count);
                    }
                }
            }
        }
    });
}

/// Update server metrics in Redis
async fn update_redis_metrics(
    mut redis: RedisConnection,
    server_id: u64,
    region: &str,
    count: usize,
) -> Result<()> {
    let metric = regions::ActiveServerMetric {
        region: region.to_string(),
        user_count: u32::try_from(count).unwrap_or(u32::MAX),
    };
    let payload = serde_json::to_string(&metric).context("Failed to serialize server metric")?;
    let result: i32 = redis::Script::new(
        r#"
        local function key_type(key)
            local response = redis.call('TYPE', key)
            if type(response) == 'table' then return response['ok'] end
            return response
        end
        local metrics_type = key_type(KEYS[1])
        local expiry_type = key_type(KEYS[2])
        if metrics_type ~= 'none' and metrics_type ~= 'hash' then
            return redis.error_reply('active server metrics key has wrong type')
        end
        if expiry_type ~= 'none' and expiry_type ~= 'zset' then
            return redis.error_reply('active server expiry key has wrong type')
        end
        local now = redis.call('TIME')
        local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
        -- Write the expiry first. If a later command fails, an orphaned index
        -- member is harmless and self-pruning; a hash field without an expiry
        -- would be counted forever.
        redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[3]), ARGV[1])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return 1
        "#,
    )
    .key(RedisKeys::active_server_metrics())
    .key(RedisKeys::active_server_metrics_expiry())
    .arg(server_id)
    .arg(payload)
    .arg(ACTIVE_SERVER_METRIC_TTL_MS)
    .invoke_async(&mut redis)
    .await
    .context("Failed to refresh active server metric")?;
    if result != 1 {
        anyhow::bail!("active server metric refresh returned {result}");
    }

    Ok(())
}

/// Broadcast the region's online-player roster whenever it changes.
///
/// Every task in a region runs this, which would be N duplicate frames per
/// tick to every socket. `publish_roster_if_changed` collapses that with a
/// Redis-side digest compare, so an idle region publishes nothing at all and a
/// change publishes once cluster-wide.
fn spawn_region_roster_broadcaster(
    redis: RedisConnection,
    region: String,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let registry = crate::presence::PresenceRegistry::new(redis, region.clone());
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Region roster broadcaster shutting down");
                    break;
                }
                _ = interval.tick() => {
                    match registry.roster().await {
                        Ok(roster) => {
                            if let Err(error) = registry.publish_roster_if_changed(&roster).await {
                                tracing::debug!(%region, %error, "failed to publish the region roster");
                            }
                        }
                        Err(error) => {
                            tracing::debug!(%region, %error, "failed to read the region roster");
                        }
                    }
                }
            }
        }
    });
}

/// Background task to broadcast user count updates every 5 seconds
/// Start the loop that drains the texture queue, when there is anything for it
/// to do.
///
/// It needs somewhere to put pixels and something to ask for them; without
/// either, a worker would claim jobs only to fail them, which is worse than
/// leaving them queued for a deployment that can finish them. So the absence
/// of a store or of every provider key means no worker rather than a broken
/// one, and the log says which.
fn spawn_texture_worker(
    db: Arc<dyn crate::db::Database>,
    store: Option<Arc<dyn crate::texture_store::TextureStore>>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    let Some(store) = store else {
        tracing::info!("no texture store configured; the texture worker will not run");
        return;
    };
    let providers = crate::generation_providers::configured_providers();
    if providers.is_empty() {
        // Uploads need no provider, so this is worth running anyway — it just
        // cannot generate.
        tracing::info!("no image provider configured; the texture worker will handle uploads only");
    }

    let worker = crate::texture_worker::Worker {
        db,
        store,
        providers,
        budget: crate::generation::Budget::default(),
        name: format!(
            "{}-{}",
            gethostname::gethostname().to_string_lossy(),
            std::process::id()
        ),
    };
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        cancellation_token.cancelled().await;
        let _ = tx.send(true);
    });
    tokio::spawn(worker.run_forever(rx));
}

fn spawn_user_count_broadcaster(
    redis: RedisConnection,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("User count broadcaster shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = broadcast_user_counts(redis.clone()).await {
                        tracing::error!("Failed to broadcast user counts: {}", e);
                    }
                }
            }
        }
    });
}

/// Aggregate user counts from Redis and broadcast to all WebSocket clients
async fn broadcast_user_counts(mut redis: RedisConnection) -> Result<()> {
    use std::collections::HashMap;

    let metrics = regions::load_active_server_metrics(&mut redis)
        .await
        .context("Failed to query active server metrics")?;

    let mut region_counts: HashMap<String, u32> = HashMap::new();
    for metric in metrics {
        let regional_count = region_counts.entry(metric.region).or_insert(0_u32);
        *regional_count = regional_count.saturating_add(metric.user_count);
    }

    // Serialize and publish to Redis channel
    let message =
        serde_json::to_string(&region_counts).context("Failed to serialize user counts")?;

    let _: () = redis
        .publish("user_count_updates", message)
        .await
        .context("Failed to publish user counts")?;

    tracing::trace!("Broadcasted user counts: {:?}", region_counts);
    Ok(())
}

#[cfg(test)]
mod deferred_http_tests {
    use super::*;
    use crate::replay_cache::InMemoryReplayCache;
    use crate::replay_store::{InMemoryReplayStore, ReplayStore};
    use axum::routing::get;
    use chrono::Utc;
    use common::{
        GAME_RECORDING_FORMAT_VERSION, GAMEPLAY_REPLAY_VERSION, GameRecordingV1, GameState,
        GameStatus, GameType, HighlightConfig, HighlightPresentation, HighlightReason,
        HighlightScoreBreakdown, HighlightWindow, QueueMode, ReplayAnchor, ReplayVisibility,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    static ACTIVE_SERVER_METRICS_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    struct StaticReplayGameReader {
        games: HashMap<i32, Game>,
    }

    #[async_trait::async_trait]
    impl ReplayGameReader for StaticReplayGameReader {
        async fn get_game(&self, game_id: i32) -> Result<Option<Game>> {
            Ok(self.games.get(&game_id).cloned())
        }
    }

    fn replay_test_recording(game_id: u32) -> GameRecordingV1 {
        let mut state = GameState::new(
            20,
            20,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(11),
            1_000,
        );
        state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id,
            visibility: ReplayVisibility::Public,
            anchors: vec![ReplayAnchor {
                tick: state.tick,
                sequence: 0,
                state: state.clone(),
            }],
            messages: Vec::new(),
            end_tick: state.tick,
            end_sync_hash: state.sync_hash(),
        }
    }

    fn replay_test_highlight(recording: &GameRecordingV1) -> HighlightClip {
        let anchor = recording.anchors[0].state.clone();
        HighlightClip {
            clip_format_version: common::HIGHLIGHT_CLIP_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id: recording.game_id,
            star_user_id: 1,
            star_snake_id: 1,
            star_name: "Replay Tester".into(),
            reason: HighlightReason::ComboFrenzy { max_chain: 2 },
            score: 150,
            breakdown: HighlightScoreBreakdown::default(),
            window: HighlightWindow {
                start_tick: anchor.tick,
                end_tick: anchor.tick,
                focus_tick: anchor.tick,
            },
            anchor,
            messages: Vec::new(),
            end_sync_hash: recording.end_sync_hash,
            presentation: HighlightPresentation {
                rotation: 0,
                follow_snake_id: 1,
                segments: Vec::new(),
            },
            config: HighlightConfig::default(),
        }
    }

    fn replay_test_game(id: i32, status: &str) -> Game {
        let now = Utc::now();
        Game {
            id,
            server_id: Some(1),
            season: Some(1),
            game_type: serde_json::json!({"Solo": {}}),
            game_state: None,
            status: status.into(),
            ended_at: (status == "complete").then_some(now),
            last_activity: now,
            created_at: now,
            game_mode: "matchmaking".into(),
            is_private: false,
            game_code: None,
            replay_object: None,
            play_of_the_game: None,
            news_eligible: false,
        }
    }

    fn replay_test_app(
        games: HashMap<i32, Game>,
        repository: Option<Arc<ReplayRepository>>,
    ) -> Router {
        replay_route_template().with_state(ReplayApiState {
            games: Arc::new(StaticReplayGameReader { games }),
            repository,
        })
    }

    async fn response_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), MAX_HIGHLIGHT_RESPONSE_BYTES)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn get_path(app: Router, path: &str) -> Response {
        app.oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn get_path_with_range(app: Router, path: &str, range: &str) -> Response {
        app.oneshot(
            Request::builder()
                .uri(path)
                .header(header::RANGE, range)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[test]
    fn public_game_ids_are_positive_decimal_database_ids() {
        assert_eq!(parse_public_game_id("1"), Some(1));
        assert_eq!(parse_public_game_id("2147483647"), Some(i32::MAX));
        for invalid in ["", "0", "-1", "+1", " 1", "1 ", "1.0", "2147483648"] {
            assert_eq!(parse_public_game_id(invalid), None, "accepted {invalid:?}");
        }
    }

    #[tokio::test]
    async fn anonymous_highlight_route_has_bounded_pending_ready_and_unavailable_states() {
        let recording = replay_test_recording(3);
        let mut waiting = replay_test_game(1, "playing");
        waiting.is_private = true;
        waiting.game_code = Some("PUBLIC-NOW".into());
        let complete_without_clip = replay_test_game(2, "complete");
        let mut ready = replay_test_game(3, "complete");
        ready.is_private = true;
        ready.game_code = Some("PUBLIC-NOW".into());
        ready.play_of_the_game = Some(replay_test_highlight(&recording));
        let app = replay_test_app(
            [(1, waiting), (2, complete_without_clip), (3, ready)]
                .into_iter()
                .collect(),
            None,
        );

        let pending = get_path(app.clone(), "/api/games/1/highlight").await;
        assert_eq!(pending.status(), StatusCode::OK);
        assert_eq!(pending.headers()[header::RETRY_AFTER], "1");
        assert_eq!(response_json(pending).await["status"], "pending");

        let unavailable = get_path(app.clone(), "/api/games/2/highlight").await;
        assert_eq!(unavailable.status(), StatusCode::OK);
        assert_eq!(response_json(unavailable).await["status"], "unavailable");

        let missing = get_path(app.clone(), "/api/games/999/highlight").await;
        assert_eq!(missing.status(), StatusCode::OK);
        assert_eq!(missing.headers()[header::RETRY_AFTER], "1");
        assert_eq!(response_json(missing).await["status"], "pending");

        let mut participant_payloads = Vec::new();
        for _ in 0..4 {
            let ready = get_path(app.clone(), "/api/games/3/highlight").await;
            assert_eq!(ready.status(), StatusCode::OK);
            let ready_json = response_json(ready).await;
            assert_eq!(ready_json["status"], "ready");
            assert_eq!(ready_json["play_of_the_game"]["game_id"], 3);
            participant_payloads.push(ready_json);
        }
        assert!(
            participant_payloads
                .windows(2)
                .all(|pair| pair[0] == pair[1]),
            "all four clients must receive the same immutable selected clip"
        );

        let invalid = get_path(app, "/api/games/-1/highlight").await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn highlight_route_uses_precommit_verification_without_request_path_resimulation() {
        let recording = replay_test_recording(4);
        let mut clip = replay_test_highlight(&recording);
        // Persistence only accepts replay-verified clips. Deliberately corrupt
        // this fixture's end hash to prove an anonymous GET does not perform a
        // CPU-amplifying deterministic simulation; the browser still verifies
        // the immutable payload before presenting it.
        clip.end_sync_hash ^= 1;
        let mut game = replay_test_game(4, "complete");
        game.play_of_the_game = Some(clip);
        let app = replay_test_app([(4, game)].into_iter().collect(), None);

        let response = get_path(app, "/api/games/4/highlight").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["status"], "ready");
    }

    #[tokio::test]
    async fn anonymous_replay_route_returns_verified_json_through_cache_aside() {
        let recording = replay_test_recording(7);
        let bytes = crate::completion::canonical_json_bytes(&recording).unwrap();
        let store = Arc::new(InMemoryReplayStore::new());
        let metadata = store.put_recording(7, &bytes).await.unwrap();
        let repository = Arc::new(ReplayRepository::new(
            store,
            Arc::new(InMemoryReplayCache::new()),
        ));
        let mut game = replay_test_game(7, "complete");
        game.is_private = true;
        game.game_code = Some("CUSTOM-PUBLIC".into());
        game.replay_object = Some(metadata);
        let app = replay_test_app([(7, game)].into_iter().collect(), Some(repository));

        let first = get_path(app.clone(), "/api/games/7/replay").await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(first.headers()["x-snaketron-replay-source"], "object_store");
        let returned: GameRecordingV1 = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), MAX_REPLAY_RESPONSE_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(returned.game_id, 7);

        let cached = get_path(app, "/api/games/7/replay").await;
        assert_eq!(cached.status(), StatusCode::OK);
        assert_eq!(cached.headers()["x-snaketron-replay-source"], "cache");
    }

    #[tokio::test]
    async fn replay_route_relies_on_persist_game_verification_without_resimulation() {
        let store = Arc::new(InMemoryReplayStore::new());
        let metadata = store
            .put_recording(8, br#"{"not":"a recording"}"#)
            .await
            .unwrap();
        let repository = Arc::new(ReplayRepository::new(
            store,
            Arc::new(InMemoryReplayCache::new()),
        ));
        let mut game = replay_test_game(8, "complete");
        game.replay_object = Some(metadata);
        let app = replay_test_app([(8, game)].into_iter().collect(), Some(repository));

        let response = get_path(app, "/api/games/8/replay").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), MAX_REPLAY_RESPONSE_BYTES)
                .await
                .unwrap(),
            br#"{"not":"a recording"}"#.as_slice()
        );
    }

    #[tokio::test]
    async fn large_replay_requires_and_serves_bounded_byte_ranges() {
        let mut bytes = vec![0u8; 9 * 1024 * 1024 + 99];
        for (index, chunk) in bytes
            .chunks_mut(crate::replay_store::REPLAY_CHUNK_UNCOMPRESSED_BYTES)
            .enumerate()
        {
            chunk.fill(index as u8);
        }
        let store = Arc::new(InMemoryReplayStore::new());
        let metadata = store.put_recording(9, &bytes).await.unwrap();
        let cache = Arc::new(InMemoryReplayCache::new());
        let repository = Arc::new(ReplayRepository::new(store, cache.clone()));
        let mut game = replay_test_game(9, "complete");
        game.replay_object = Some(metadata);
        let app = replay_test_app([(9, game)].into_iter().collect(), Some(repository));

        let full = get_path(app.clone(), "/api/games/9/replay").await;
        assert_eq!(full.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(full.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(
            cache.object_count().await,
            1,
            "preflight loads only manifest"
        );

        let start = crate::replay_store::REPLAY_CHUNK_UNCOMPRESSED_BYTES - 5;
        let end = start + 31;
        let partial = get_path_with_range(
            app,
            "/api/games/9/replay",
            &format!("bytes={start}-{}", end - 1),
        )
        .await;
        assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            partial.headers()[header::CONTENT_RANGE],
            format!("bytes {start}-{}/{}", end - 1, bytes.len())
        );
        let returned = axum::body::to_bytes(partial.into_body(), MAX_REPLAY_RESPONSE_BYTES)
            .await
            .unwrap();
        assert_eq!(returned.as_ref(), &bytes[start..end]);
    }

    #[tokio::test]
    async fn active_server_registry_refreshes_and_prunes_expired_tasks() -> Result<()> {
        let _guard = ACTIVE_SERVER_METRICS_TEST_LOCK.lock().await;
        let client = redis::Client::open("redis://127.0.0.1:6379/15?protocol=resp3")?;
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let mut manager = crate::redis_utils::create_connection_manager(client, push_tx).await?;
        let keys = [
            RedisKeys::active_server_metrics(),
            RedisKeys::active_server_metrics_expiry(),
        ];
        let _: () = manager.del(&keys).await?;

        update_redis_metrics(manager.clone().into(), 101, "use1", 2).await?;
        update_redis_metrics(manager.clone().into(), 202, "euw1", 3).await?;
        let mut connection: RedisConnection = manager.clone().into();
        let mut metrics = regions::load_active_server_metrics(&mut connection).await?;
        metrics.sort_by(|left, right| left.region.cmp(&right.region));
        assert_eq!(
            metrics,
            vec![
                regions::ActiveServerMetric {
                    region: "euw1".into(),
                    user_count: 3,
                },
                regions::ActiveServerMetric {
                    region: "use1".into(),
                    user_count: 2,
                },
            ]
        );

        let _: () = manager
            .zadd(RedisKeys::active_server_metrics_expiry(), 101, 0_i64)
            .await?;
        let metrics = regions::load_active_server_metrics(&mut connection).await?;
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].region, "euw1");
        assert!(
            !manager
                .hexists::<_, _, bool>(RedisKeys::active_server_metrics(), 101)
                .await?
        );

        let _: () = manager.del(&keys).await?;
        Ok(())
    }

    #[tokio::test]
    async fn cold_boot_is_live_unready_then_installs_application_without_rebind() -> Result<()> {
        let port = crate::game_server::get_available_port();
        let address = format!("127.0.0.1:{port}");
        let lifecycle = TaskLifecycle::new("cold-boot-test");
        let cancellation = CancellationToken::new();
        let (deferred, task) =
            DeferredHttpServer::bind(&address, lifecycle.clone(), cancellation.clone()).await?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()?;

        let live = client
            .get(format!("http://{address}/health/live"))
            .send()
            .await?;
        assert_eq!(live.status(), reqwest::StatusCode::OK);
        let ready = client
            .get(format!("http://{address}/health/ready"))
            .send()
            .await?;
        assert_eq!(ready.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let warming = client.get(format!("http://{address}/probe")).send().await?;
        assert_eq!(warming.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert!(!task.is_finished());

        lifecycle.mark_event_readers_ready(true);
        lifecycle.mark_assignment_ready(true);
        lifecycle.mark_membership_ready(true);
        lifecycle.mark_redis_success_now();
        lifecycle.activate();
        let no_application = client
            .get(format!("http://{address}/health/ready"))
            .send()
            .await?;
        assert_eq!(
            no_application.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        );

        deferred
            .install(Router::new().route("/probe", get(|| async { StatusCode::NO_CONTENT })))?;

        let ready = client
            .get(format!("http://{address}/health/ready"))
            .send()
            .await?;
        assert_eq!(ready.status(), reqwest::StatusCode::OK);
        let installed = client.get(format!("http://{address}/probe")).send().await?;
        assert_eq!(installed.status(), reqwest::StatusCode::NO_CONTENT);
        assert!(!task.is_finished());

        cancellation.cancel();
        task.await??;
        Ok(())
    }
}
