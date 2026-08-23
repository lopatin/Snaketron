use anyhow::Result;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{Duration, Utc};
use common::{GameType, QueueMode};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::{Value, json};
use server::{
    api::{
        auth::{AuthState, LoginRequest, RegisterRequest, get_current_user, login, register},
        jwt::{Claims, JwtManager},
        middleware::{AuthMiddlewareState, auth_middleware},
    },
    db::{Database, dynamodb::DynamoDatabase},
    matchmaking_pool::MatchmakingPool,
};
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

const JWT_SECRET: &str = "test_secret_key_for_testing";

// Each test changes the process-wide DynamoDB prefix, so this integration
// binary must serialize its setup and database lifetime.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn isolated_state() -> Result<(AuthState, Arc<dyn Database>, Arc<JwtManager>)> {
    let prefix = format!("test_auth_upgrade_{}", Uuid::new_v4().simple());
    // SAFETY: every test in this binary holds TEST_LOCK for its full lifetime.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", prefix) };

    let db = Arc::new(DynamoDatabase::new().await?) as Arc<dyn Database>;
    let jwt_manager = Arc::new(JwtManager::new(JWT_SECRET));
    let state = AuthState {
        analytics: None,
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        user_cache: None,
        crazygames_verifier: None,
        texture_store: None,
        // These tests exercise accounts, not the shop.
        payments: None,
    };
    Ok((state, db, jwt_manager))
}

async fn response_json(response: Response) -> Result<(StatusCode, Value)> {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

async fn register_response(
    state: AuthState,
    bearer_token: Option<&str>,
    username: &str,
    password: &str,
) -> Result<(StatusCode, Value)> {
    let mut headers = HeaderMap::new();
    if let Some(token) = bearer_token {
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))?,
        );
    }
    let response = match register(
        State(state),
        headers,
        Json(RegisterRequest {
            username: username.to_string(),
            password: password.to_string(),
        }),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    response_json(response).await
}

async fn login_response(
    state: AuthState,
    username: &str,
    password: &str,
) -> Result<(StatusCode, Value)> {
    let response = match login(
        State(state),
        axum::http::HeaderMap::new(),
        Json(LoginRequest {
            username: username.to_string(),
            password: password.to_string(),
        }),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => error.into_response(),
    };
    response_json(response).await
}

fn guest_token(jwt: &JwtManager, user_id: i32, username: &str) -> Result<String> {
    jwt.generate_token_with_guest_and_pool(user_id, username, true, MatchmakingPool::Public)
}

#[tokio::test]
async fn guest_upgrade_preserves_id_progress_and_owned_records() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let password = "careful-password";
    let guest = db
        .create_guest_user("KeepMySnake", "guest-record-token", 1_000, false)
        .await?;
    let original_created_at = guest.created_at;
    let old_token = guest_token(&jwt_manager, guest.id, &guest.username)?;

    db.update_user_mmr(guest.id, 1_234).await?;
    assert_eq!(
        db.update_user_mmr_by_mode(guest.id, 125, &QueueMode::Competitive)
            .await?,
        1_125
    );
    assert_eq!(
        db.update_user_mmr_by_mode(guest.id, -50, &QueueMode::Quickmatch)
            .await?,
        950
    );
    assert_eq!(db.add_user_xp(guest.id, 77).await?, 77);

    let ranked_game_type = GameType::TeamMatch { per_team: 1 };
    db.upsert_ranking(
        guest.id,
        &guest.username,
        1_125,
        &QueueMode::Competitive,
        &ranked_game_type,
        "test-region",
        0,
        true,
    )
    .await?;
    db.insert_high_score(
        "guest-upgrade-score",
        guest.id,
        &guest.username,
        42,
        &GameType::Solo,
        "test-region",
        0,
    )
    .await?;
    let game_id = db
        .create_game(
            1,
            &serde_json::to_value(GameType::Solo)?,
            "quickmatch",
            false,
            None,
        )
        .await?;
    db.add_player_to_game(game_id, guest.id, 0).await?;

    let (status, body) =
        register_response(state.clone(), Some(&old_token), &guest.username, password).await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["user"]["id"], json!(guest.id));
    assert_eq!(body["user"]["isGuest"], json!(false));
    assert_eq!(body["user"]["mmr"], json!(1_234));

    let replacement_token = body["token"].as_str().expect("replacement token");
    let replacement_claims = jwt_manager.verify_token(replacement_token)?;
    assert_eq!(replacement_claims.sub, guest.id.to_string());
    assert_eq!(replacement_claims.username, guest.username);
    assert!(!replacement_claims.is_guest);
    assert_eq!(replacement_claims.matchmaking_pool, MatchmakingPool::Public);

    let upgraded = db
        .get_user_by_id(guest.id)
        .await?
        .expect("upgraded user remains present");
    assert_eq!(upgraded.id, guest.id);
    assert_eq!(upgraded.created_at, original_created_at);
    assert_eq!(upgraded.mmr, 1_234);
    assert_eq!(upgraded.ranked_mmr, 1_125);
    assert_eq!(upgraded.casual_mmr, 950);
    assert_eq!(upgraded.xp, 77);
    assert!(!upgraded.is_guest);
    assert!(upgraded.guest_token.is_none());
    assert!(verify(password, &upgraded.password_hash)?);

    // A guest socket that was already open before the HTTP conversion still
    // carries guest metadata. The database boundary must reject its late
    // nickname mutation rather than desynchronizing the username index.
    assert!(
        db.update_guest_username(guest.id, "StaleSocketRename")
            .await
            .is_err()
    );
    assert_eq!(
        db.get_user_by_id(guest.id)
            .await?
            .expect("converted user remains present")
            .username,
        upgraded.username
    );

    let by_username = db
        .get_user_by_username(&upgraded.username)
        .await?
        .expect("username index points to converted user");
    assert_eq!(by_username.id, guest.id);
    assert_eq!(by_username.ranked_mmr, 1_125);
    assert_eq!(by_username.casual_mmr, 950);
    assert_eq!(by_username.xp, 77);

    let ranking = db
        .get_user_ranking(
            guest.id,
            &QueueMode::Competitive,
            &ranked_game_type,
            "test-region",
            0,
        )
        .await?
        .expect("ranking remains attached to the same ID");
    assert_eq!(ranking.user_id, guest.id);
    assert_eq!(ranking.mmr, 1_125);
    assert_eq!(ranking.games_played, 1);
    assert_eq!(ranking.wins, 1);

    let high_scores = db
        .get_high_scores(&GameType::Solo, Some("test-region"), 0, 10)
        .await?;
    let score = high_scores
        .iter()
        .find(|score| score.user_id == guest.id)
        .expect("high score remains attached to the same ID");
    assert_eq!(score.score, 42);
    assert!(
        db.get_game_players(game_id)
            .await?
            .iter()
            .any(|player| player.user_id == guest.id)
    );

    let (login_status, login_body) =
        login_response(state.clone(), &upgraded.username, password).await?;
    assert_eq!(login_status, StatusCode::OK, "{login_body}");
    assert_eq!(login_body["user"]["id"], json!(guest.id));

    // A lost-success retry can recover with the newly established password.
    let (retry_status, retry_body) = register_response(
        state.clone(),
        Some(&old_token),
        &upgraded.username,
        password,
    )
    .await?;
    assert_eq!(retry_status, StatusCode::OK, "{retry_body}");
    assert_eq!(retry_body["user"]["id"], json!(guest.id));

    // Exercise the real authentication middleware boundary: the old guest
    // claim is revoked by the canonical account transition, while the
    // replacement token remains valid for the same user ID.
    let protected_auth = Router::new()
        .route("/api/auth/me", get(get_current_user))
        .layer(middleware::from_fn_with_state(
            AuthMiddlewareState {
                jwt_manager: jwt_manager.clone(),
                db: db.clone(),
            },
            auth_middleware,
        ))
        .with_state(state);
    let stale_response = protected_auth
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/auth/me")
                .header(header::AUTHORIZATION, format!("Bearer {old_token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(stale_response.status(), StatusCode::UNAUTHORIZED);

    let replacement_response = protected_auth
        .oneshot(
            Request::builder()
                .uri("/api/auth/me")
                .header(header::AUTHORIZATION, format!("Bearer {replacement_token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(replacement_response.status(), StatusCode::OK);

    Ok(())
}

#[tokio::test]
async fn taken_username_leaves_guest_unchanged() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let owner_hash = hash("owner-password", DEFAULT_COST)?;
    let owner = db.create_user("AlreadyOwned", &owner_hash, 1_000).await?;
    let guest = db
        .create_guest_user("StillAGuest", "guest-conflict-token", 1_000, false)
        .await?;
    db.add_user_xp(guest.id, 19).await?;
    let token = guest_token(&jwt_manager, guest.id, &guest.username)?;

    let (status, body) =
        register_response(state, Some(&token), &owner.username, "new-password").await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], json!("Username already exists"));

    let unchanged = db
        .get_user_by_id(guest.id)
        .await?
        .expect("guest remains present");
    assert!(unchanged.is_guest);
    assert_eq!(unchanged.username, guest.username);
    assert_eq!(unchanged.password_hash, guest.password_hash);
    assert_eq!(unchanged.guest_token, guest.guest_token);
    assert_eq!(unchanged.xp, 19);
    assert_eq!(
        db.get_user_by_username(&owner.username)
            .await?
            .expect("owner remains present")
            .id,
        owner.id
    );
    assert!(db.get_user_by_username(&guest.username).await?.is_none());

    Ok(())
}

#[tokio::test]
async fn stress_guest_cannot_be_upgraded() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let guest = db
        .create_guest_user("StressGuest", "stress-guest-token", 1_000, true)
        .await?;
    let stress_token = jwt_manager.generate_token_with_guest_and_pool(
        guest.id,
        &guest.username,
        true,
        MatchmakingPool::Stress,
    )?;
    // The database check is a second boundary: even a public-pool claim for a
    // stress record must not convert that synthetic identity into a real one.
    let public_token = guest_token(&jwt_manager, guest.id, &guest.username)?;

    for (token, username) in [
        (stress_token.as_str(), "StressGuest"),
        (public_token.as_str(), "StressPublicClaim"),
    ] {
        let (status, body) =
            register_response(state.clone(), Some(token), username, "strong-password").await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(
            body["error"],
            json!("Stress-test guest accounts cannot be upgraded")
        );
        assert!(db.get_user_by_username(username).await?.is_none());
    }

    let unchanged = db
        .get_user_by_id(guest.id)
        .await?
        .expect("stress guest remains present");
    assert!(unchanged.is_guest);
    assert!(unchanged.is_stress_test);
    assert_eq!(unchanged.username, guest.username);
    assert_eq!(unchanged.password_hash, guest.password_hash);
    assert_eq!(unchanged.guest_token, guest.guest_token);

    Ok(())
}

#[tokio::test]
async fn invalid_guest_tokens_fail_closed_and_signed_out_registration_still_works() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let now = Utc::now();
    let expired_token = encode(
        &Header::new(Algorithm::HS256),
        &Claims {
            sub: "999".to_string(),
            username: "ExpiredGuest".to_string(),
            exp: (now - Duration::hours(1)).timestamp(),
            iat: (now - Duration::hours(2)).timestamp(),
            is_guest: true,
            matchmaking_pool: MatchmakingPool::Public,
        },
        &EncodingKey::from_secret(JWT_SECRET.as_bytes()),
    )?;

    for (token, username) in [
        ("not-a-jwt", "InvalidBearer"),
        (expired_token.as_str(), "ExpiredBearer"),
    ] {
        let (status, body) =
            register_response(state.clone(), Some(token), username, "strong-password").await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        assert_eq!(body["error"], json!("Invalid or expired guest session"));
        assert!(db.get_user_by_username(username).await?.is_none());
    }

    let (status, body) =
        register_response(state.clone(), None, "OrdinaryAccount", "strong-password").await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ordinary_id = body["user"]["id"].as_i64().expect("ordinary user ID") as i32;
    assert!(!body["user"]["isGuest"].as_bool().unwrap());

    let ordinary_token = body["token"].as_str().expect("ordinary token");
    let claims = jwt_manager.verify_token(ordinary_token)?;
    assert_eq!(claims.sub, ordinary_id.to_string());
    assert!(!claims.is_guest);

    let (full_user_status, full_user_body) = register_response(
        state,
        Some(ordinary_token),
        "SecondAccount",
        "strong-password",
    )
    .await?;
    assert_eq!(full_user_status, StatusCode::CONFLICT, "{full_user_body}");
    assert_eq!(
        full_user_body["error"],
        json!("Account is already registered")
    );
    assert!(db.get_user_by_username("SecondAccount").await?.is_none());

    Ok(())
}

#[tokio::test]
async fn concurrent_guest_claims_create_exactly_one_account() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let guest = db
        .create_guest_user("RaceGuest", "guest-race-token", 1_000, false)
        .await?;
    let token = guest_token(&jwt_manager, guest.id, &guest.username)?;

    let first = register_response(
        state.clone(),
        Some(&token),
        "RaceWinnerA",
        "strong-password-a",
    );
    let second = register_response(
        state.clone(),
        Some(&token),
        "RaceWinnerB",
        "strong-password-b",
    );
    let (first_result, second_result) = tokio::join!(first, second);
    let (first_status, first_body) = first_result?;
    let (second_status, second_body) = second_result?;

    let (winner_name, winner_password, winner_body, loser_name, loser_body) =
        match (first_status, second_status) {
            (StatusCode::OK, StatusCode::UNAUTHORIZED) => (
                "RaceWinnerA",
                "strong-password-a",
                &first_body,
                "RaceWinnerB",
                &second_body,
            ),
            (StatusCode::UNAUTHORIZED, StatusCode::OK) => (
                "RaceWinnerB",
                "strong-password-b",
                &second_body,
                "RaceWinnerA",
                &first_body,
            ),
            statuses => panic!(
                "expected one success and one rejected stale claim, got {statuses:?}; \
                 first={first_body}, second={second_body}"
            ),
        };
    assert_eq!(winner_body["user"]["id"], json!(guest.id));
    assert_eq!(winner_body["user"]["isGuest"], json!(false));
    assert_eq!(
        loser_body["error"],
        json!("Invalid or expired guest session")
    );

    let converted = db
        .get_user_by_id(guest.id)
        .await?
        .expect("guest record remains present");
    assert!(!converted.is_guest);
    assert_eq!(converted.id, guest.id);
    assert_eq!(converted.username, winner_name);
    assert!(converted.guest_token.is_none());
    assert!(verify(winner_password, &converted.password_hash)?);

    let winner = db
        .get_user_by_username(winner_name)
        .await?
        .expect("winning username mapping");
    assert_eq!(winner.id, guest.id);
    assert_eq!(winner.username, winner_name);
    assert!(db.get_user_by_username(loser_name).await?.is_none());

    let (login_status, login_body) =
        login_response(state.clone(), winner_name, winner_password).await?;
    assert_eq!(login_status, StatusCode::OK, "{login_body}");
    assert_eq!(login_body["user"]["id"], json!(guest.id));
    let (loser_login_status, loser_login_body) =
        login_response(state, loser_name, "strong-password-a").await?;
    assert_eq!(
        loser_login_status,
        StatusCode::UNAUTHORIZED,
        "{loser_login_body}"
    );

    Ok(())
}

#[tokio::test]
async fn two_guests_racing_for_one_username_leave_one_guest_unclaimed() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let (state, db, jwt_manager) = isolated_state().await?;
    let first_guest = db
        .create_guest_user("FirstGuest", "first-guest-token", 1_000, false)
        .await?;
    let second_guest = db
        .create_guest_user("SecondGuest", "second-guest-token", 1_000, false)
        .await?;
    let first_token = guest_token(&jwt_manager, first_guest.id, &first_guest.username)?;
    let second_token = guest_token(&jwt_manager, second_guest.id, &second_guest.username)?;
    let target_username = "SharedClaim";

    let first = register_response(
        state.clone(),
        Some(&first_token),
        target_username,
        "first-password",
    );
    let second = register_response(
        state.clone(),
        Some(&second_token),
        target_username,
        "second-password",
    );
    let (first_result, second_result) = tokio::join!(first, second);
    let (first_status, first_body) = first_result?;
    let (second_status, second_body) = second_result?;

    let (winner, winner_password, winner_body, loser, loser_body) =
        match (first_status, second_status) {
            (StatusCode::OK, StatusCode::CONFLICT) => (
                &first_guest,
                "first-password",
                &first_body,
                &second_guest,
                &second_body,
            ),
            (StatusCode::CONFLICT, StatusCode::OK) => (
                &second_guest,
                "second-password",
                &second_body,
                &first_guest,
                &first_body,
            ),
            statuses => panic!(
                "expected one success and one username conflict, got {statuses:?}; \
                 first={first_body}, second={second_body}"
            ),
        };
    assert_eq!(winner_body["user"]["id"], json!(winner.id));
    assert_eq!(loser_body["error"], json!("Username already exists"));

    let converted = db
        .get_user_by_id(winner.id)
        .await?
        .expect("winning guest remains present");
    assert!(!converted.is_guest);
    assert_eq!(converted.username, target_username);
    assert!(converted.guest_token.is_none());
    assert!(verify(winner_password, &converted.password_hash)?);

    let unchanged = db
        .get_user_by_id(loser.id)
        .await?
        .expect("losing guest remains present");
    assert!(unchanged.is_guest);
    assert_eq!(unchanged.username, loser.username);
    assert_eq!(unchanged.password_hash, loser.password_hash);
    assert_eq!(unchanged.guest_token, loser.guest_token);

    assert_eq!(
        db.get_user_by_username(target_username)
            .await?
            .expect("target username has one owner")
            .id,
        winner.id
    );
    assert!(db.get_user_by_username(&loser.username).await?.is_none());

    let (login_status, login_body) =
        login_response(state.clone(), target_username, winner_password).await?;
    assert_eq!(login_status, StatusCode::OK, "{login_body}");
    assert_eq!(login_body["user"]["id"], json!(winner.id));
    let losing_password = if loser.id == first_guest.id {
        "first-password"
    } else {
        "second-password"
    };
    let (loser_login_status, loser_login_body) =
        login_response(state, target_username, losing_password).await?;
    assert_eq!(
        loser_login_status,
        StatusCode::UNAUTHORIZED,
        "{loser_login_body}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_progress_writes_survive_in_place_upgrade() -> Result<()> {
    const STEPS: i32 = 4;

    let _guard = TEST_LOCK.lock().await;
    let (state, db, _jwt_manager) = isolated_state().await?;
    let password = "progress-password";
    let password_hash = hash(password, DEFAULT_COST)?;
    let guest = db
        .create_guest_user("ProgressGuest", "progress-guest-token", 1_000, false)
        .await?;
    let guest_id = guest.id;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    let upgrade_db = db.clone();
    let upgrade_barrier = barrier.clone();
    let upgrade_username = guest.username.clone();
    let upgrade_task = tokio::spawn(async move {
        upgrade_barrier.wait().await;
        upgrade_db
            .upgrade_guest_to_account(guest_id, &upgrade_username, &password_hash)
            .await
    });

    let progress_db = db.clone();
    let progress_barrier = barrier.clone();
    let progress_task = tokio::spawn(async move {
        progress_barrier.wait().await;
        for _ in 0..STEPS {
            progress_db.add_user_xp(guest_id, 1).await?;
            progress_db
                .update_user_mmr_by_mode(guest_id, 3, &QueueMode::Competitive)
                .await?;
            progress_db
                .update_user_mmr_by_mode(guest_id, -2, &QueueMode::Quickmatch)
                .await?;
            tokio::task::yield_now().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    barrier.wait().await;
    let (upgrade_result, progress_result) = tokio::join!(upgrade_task, progress_task);
    let upgraded = upgrade_result??;
    progress_result??;
    assert_eq!(upgraded.id, guest.id);

    let persisted = db
        .get_user_by_id(guest.id)
        .await?
        .expect("converted user remains present after concurrent progress writes");
    assert!(!persisted.is_guest);
    assert_eq!(persisted.id, guest.id);
    assert_eq!(persisted.username, guest.username);
    assert_eq!(persisted.xp, STEPS);
    assert_eq!(persisted.ranked_mmr, 1_000 + (STEPS * 3));
    assert_eq!(persisted.casual_mmr, 1_000 - (STEPS * 2));
    assert!(persisted.guest_token.is_none());
    assert!(verify(password, &persisted.password_hash)?);
    assert_eq!(
        db.get_user_by_username(&guest.username)
            .await?
            .expect("converted username resolves after concurrent progress writes")
            .id,
        guest.id
    );

    let (login_status, login_body) = login_response(state, &guest.username, password).await?;
    assert_eq!(login_status, StatusCode::OK, "{login_body}");
    assert_eq!(login_body["user"]["id"], json!(guest.id));

    Ok(())
}
