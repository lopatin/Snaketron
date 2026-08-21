use anyhow::Result;
use axum::{
    Extension, Json,
    body::to_bytes,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use common::{GameType, QueueMode};
use serde_json::{Value, json};
use server::api::{
    auth::{AuthState, RegisterRequest},
    jwt::JwtManager,
    leaderboard::{
        LeaderboardEntry, LeaderboardQuery, LeaderboardState, UserRankingResponse, get_leaderboard,
        get_my_ranking,
    },
    middleware::AuthUser,
};
use server::db::{Database, dynamodb::DynamoDatabase, models::User};
use server::matchmaking_pool::MatchmakingPool as Pool;
use std::sync::Arc;
use uuid::Uuid;

const JWT_SECRET: &str = "test_secret_key_for_leaderboard_tests";

// Every test mutates process-wide environment (table prefix and the server's
// own AWS region), so this binary serializes its cases.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DUEL: GameType = GameType::TeamMatch { per_team: 1 };

async fn isolated_db() -> Result<Arc<dyn Database>> {
    let prefix = format!("test_leaderboard_rank_{}", Uuid::new_v4().simple());
    // SAFETY: every test in this binary holds TEST_LOCK for its full lifetime.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", prefix) };
    Ok(Arc::new(DynamoDatabase::new().await?) as Arc<dyn Database>)
}

/// Stand in for "which region's server answered this request". Only
/// `SNAKETRON_AWS_REGION` is touched: it wins over every other source in
/// `season::get_region`, and `AWS_REGION` still has to point the SDK at the
/// local DynamoDB endpoint.
fn set_server_region(region: &str) {
    // SAFETY: every test in this binary holds TEST_LOCK for its full lifetime.
    unsafe {
        std::env::set_var("SNAKETRON_AWS_REGION", region);
        std::env::remove_var("REGION");
        std::env::remove_var("SNAKETRON_REGION");
    }
}

/// A database plus the registration handler's state, for the tests that need
/// to exercise the real guest-upgrade branch rather than the database alone.
async fn isolated_auth_state() -> Result<(Arc<dyn Database>, AuthState, Arc<JwtManager>)> {
    let db = isolated_db().await?;
    let jwt_manager = Arc::new(JwtManager::new(JWT_SECRET));
    let auth_state = AuthState {
        // Analytics is optional by construction; tests run without it.
        analytics: None,
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        user_cache: None,
        crazygames_verifier: None,
    };
    Ok((db, auth_state, jwt_manager))
}

async fn response_json(response: Response) -> Result<(StatusCode, Value)> {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    Ok((status, serde_json::from_slice(&bytes)?))
}

/// Call the real registration endpoint, optionally presenting the bearer token
/// a browser would already be holding for its guest session.
async fn register(
    state: &AuthState,
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
    let response = match server::api::auth::register(
        State(state.clone()),
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

fn query(region: Option<&str>) -> LeaderboardQuery {
    LeaderboardQuery {
        queue_mode: "competitive".to_string(),
        game_type: "duel".to_string(),
        season: Some(0),
        limit: Some(25),
        offset: Some(0),
        region: region.map(str::to_string),
    }
}

fn auth_user(user: &User) -> AuthUser {
    AuthUser {
        user_id: user.id,
        username: user.username.clone(),
        is_guest: user.is_guest,
        is_admin: false,
    }
}

async fn my_ranking(
    db: &Arc<dyn Database>,
    user: &User,
    region: Option<&str>,
) -> Result<UserRankingResponse> {
    let Json(response) = get_my_ranking(
        Extension(auth_user(user)),
        State(LeaderboardState { db: db.clone() }),
        Query(query(region)),
    )
    .await
    .map_err(|status| anyhow::anyhow!("my ranking failed: {status}"))?;
    Ok(response)
}

async fn board_user_ids(db: &Arc<dyn Database>, region: Option<&str>) -> Vec<i32> {
    let Json(response) = get_leaderboard(
        State(LeaderboardState { db: db.clone() }),
        Query(query(region)),
    )
    .await;
    response
        .entries
        .into_iter()
        .map(|entry| match entry {
            LeaderboardEntry::Ranking(row) => row.user_id,
            LeaderboardEntry::HighScore(row) => row.user_id,
        })
        .collect()
}

async fn board_rows(db: &Arc<dyn Database>, region: Option<&str>) -> Vec<(String, i32)> {
    let Json(response) = get_leaderboard(
        State(LeaderboardState { db: db.clone() }),
        Query(query(region)),
    )
    .await;
    response
        .entries
        .into_iter()
        .filter_map(|entry| match entry {
            LeaderboardEntry::Ranking(row) => Some((row.username, row.mmr)),
            LeaderboardEntry::HighScore(_) => None,
        })
        .collect()
}

/// The two endpoints are deliberately asymmetric when the client selects
/// Global: the board spans every region, while a player's own standing is
/// always reported for one region — the one the client names (the web client
/// sends the region its websocket is connected to), or else the region of the
/// server that answered. This pins that contract, including the part that
/// looks like a discrepancy: the badge may show a different rating than the
/// player's own row on the global board.
#[tokio::test]
async fn global_board_spans_regions_while_own_rank_follows_the_connected_region() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let db = isolated_db().await?;

    let player = db.create_user("Troncat", "hash", 1_000).await?;
    let rival = db.create_user("Lopatron", "hash", 1_000).await?;

    // Older games on the US server, then the player's latest games on the EU
    // server, where they climbed to 1430.
    db.upsert_ranking(
        player.id,
        &player.username,
        1_313,
        &QueueMode::Competitive,
        &DUEL,
        "us-east-1",
        0,
        true,
    )
    .await?;
    db.upsert_ranking(
        player.id,
        &player.username,
        1_430,
        &QueueMode::Competitive,
        &DUEL,
        "eu-west-1",
        0,
        true,
    )
    .await?;
    db.upsert_ranking(
        rival.id,
        &rival.username,
        1_346,
        &QueueMode::Competitive,
        &DUEL,
        "us-east-1",
        0,
        true,
    )
    .await?;

    // The Global board is the union of every region, so this player holds the
    // top row with the 1430 they earned in EU.
    let global_board = board_rows(&db, None).await;
    assert_eq!(
        global_board.first().map(|(_, mmr)| *mmr),
        Some(1_430),
        "the global board spans regions: {global_board:?}"
    );

    // A client connected to the US region asks about that region, by logical
    // matchmaking ID, and gets its US rating — deliberately not the 1430
    // sitting above it on the same board.
    let connected_to_us = my_ranking(&db, &player, Some("use1")).await?;
    assert_eq!(
        (
            connected_to_us.mmr,
            connected_to_us.wins,
            connected_to_us.losses
        ),
        (Some(1_313), Some(1), Some(0))
    );

    // Connected to EU instead, the same player's badge follows them there.
    assert_eq!(
        my_ranking(&db, &player, Some("euw1")).await?.mmr,
        Some(1_430)
    );

    // The logical ID is what the client has, and it must not be reinterpreted
    // by the region of the server that happens to answer.
    set_server_region("eu-west-1");
    assert_eq!(
        my_ranking(&db, &player, Some("use1")).await?.mmr,
        Some(1_313),
        "a named region is answered as named, whichever server replies"
    );

    // With no live websocket the client names no region, and the answering
    // server's own region is the documented fallback.
    assert_eq!(my_ranking(&db, &player, None).await?.mmr, Some(1_430));
    set_server_region("us-east-1");
    assert_eq!(my_ranking(&db, &player, None).await?.mmr, Some(1_313));
    set_server_region("ap-southeast-2");
    assert_eq!(
        my_ranking(&db, &player, None).await?.mmr,
        None,
        "no row in the answering server's region reads as unranked"
    );

    Ok(())
}

/// The global board is a ladder of players, not of ranking rows: a player who
/// has played in several regions owns a row in each, and must still occupy a
/// single line. Duplicates also used to consume the page, hiding real
/// competitors below the fold.
#[tokio::test]
async fn global_leaderboard_lists_each_player_once_and_still_fills_the_page() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let db = isolated_db().await?;

    // Three well-travelled players with a row in each region, plus enough
    // single-region players to overflow a page once the duplicates collapse.
    for (index, name) in ["Troncat", "Lopatron", "Tronman"].iter().enumerate() {
        let player = db.create_user(name, "hash", 1_000).await?;
        for (offset, region) in ["us-east-1", "eu-west-1", "ap-southeast-2"]
            .iter()
            .enumerate()
        {
            db.upsert_ranking(
                player.id,
                &player.username,
                2_000 - (index as i32 * 10) - (offset as i32),
                &QueueMode::Competitive,
                &DUEL,
                region,
                0,
                true,
            )
            .await?;
        }
    }
    for index in 0..30 {
        let player = db
            .create_user(&format!("Solo{index}"), "hash", 1_000)
            .await?;
        db.upsert_ranking(
            player.id,
            &player.username,
            1_900 - index,
            &QueueMode::Competitive,
            &DUEL,
            "us-east-1",
            0,
            true,
        )
        .await?;
    }

    let page = board_rows(&db, None).await;
    assert_eq!(page.len(), 25, "a full page of players: {page:?}");

    let mut names: Vec<String> = page.iter().map(|(name, _)| name.clone()).collect();
    names.sort();
    let unique = names.len();
    names.dedup();
    assert_eq!(names.len(), unique, "no player appears twice on the ladder");

    assert_eq!(
        page.iter()
            .filter(|(name, _)| name == "Troncat")
            .map(|(_, mmr)| *mmr)
            .collect::<Vec<_>>(),
        vec![2_000],
        "the surviving row is the player's strongest"
    );

    Ok(())
}

/// Guests are deliberately exempt from the username index, so a guest may take
/// the display name of a registered account. Two rows on the ladder can
/// therefore read "Troncat" while belonging to different accounts — and the
/// badge must follow the signed-in account's ID, never the name.
#[tokio::test]
async fn duplicate_display_names_belong_to_separate_accounts_and_rank_by_id() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let db = isolated_db().await?;

    let registered = db.create_user("Troncat", "hash", 1_000).await?;
    let impostor = db
        .create_guest_user("Guest4821", "guest-token", 1_000, false)
        .await?;
    // Nothing stops a guest from adopting a registered player's name.
    db.update_guest_username(impostor.id, "Troncat").await?;
    assert_ne!(registered.id, impostor.id);
    assert_eq!(
        db.get_user_by_id(impostor.id)
            .await?
            .map(|user| user.username),
        Some("Troncat".to_string()),
    );
    // Registered names stay unique, so this can only ever be guest-vs-account.
    assert!(
        db.create_user("Troncat", "hash", 1_000).await.is_err(),
        "a second registered account must not be able to claim a taken name"
    );
    assert_eq!(
        db.get_user_by_username("Troncat")
            .await?
            .map(|user| user.id),
        Some(registered.id),
        "the username index still resolves to the registered account"
    );

    for (user_id, mmr) in [(impostor.id, 1_430), (registered.id, 1_313)] {
        db.upsert_ranking(
            user_id,
            "Troncat",
            mmr,
            &QueueMode::Competitive,
            &DUEL,
            "us-east-1",
            0,
            true,
        )
        .await?;
    }

    let board = board_rows(&db, None).await;
    assert_eq!(
        board,
        vec![
            ("Troncat".to_string(), 1_430),
            ("Troncat".to_string(), 1_313)
        ],
        "both accounts are listed under the same display name"
    );

    // The signed-in player owns the second row, and that is what they must be
    // told — the higher row is a different account that merely shares a name.
    assert_eq!(my_ranking(&db, &registered, None).await?.mmr, Some(1_313));

    // Rows carry the account they belong to, which is the only thing a client
    // can match on: the names are identical, and the board's rank numbers
    // describe a ladder the badge is not reporting on.
    assert_eq!(
        board_user_ids(&db, None).await,
        vec![impostor.id, registered.id],
        "each row names its account so the client can find the player's own"
    );

    Ok(())
}

/// The upgrade a player expects: play as guest "Troncat", then register that
/// same name from the same browser. The account ID must survive, carrying the
/// ranking row with it, so the ladder shows one Troncat and the badge shows the
/// rating that was earned as a guest.
#[tokio::test]
async fn registering_a_guests_own_nickname_carries_its_rank_without_duplicating_it() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let (db, auth_state, jwt) = isolated_auth_state().await?;

    let guest = db
        .create_guest_user("Troncat", "guest-record-token", 1_000, false)
        .await?;
    // A few ranked wins as a guest.
    db.update_user_mmr_by_mode(guest.id, 430, &QueueMode::Competitive)
        .await?;
    db.upsert_ranking(
        guest.id,
        "Troncat",
        1_430,
        &QueueMode::Competitive,
        &DUEL,
        "us-east-1",
        0,
        true,
    )
    .await?;

    // The browser presents the guest session it is already holding.
    let guest_token =
        jwt.generate_token_with_guest_and_pool(guest.id, &guest.username, true, Pool::Public)?;
    let (status, body) = register(
        &auth_state,
        Some(&guest_token),
        "Troncat",
        "a-real-password",
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["user"]["id"],
        json!(guest.id),
        "the upgrade must keep the account ID, or the earned rank is orphaned"
    );
    assert_eq!(body["user"]["isGuest"], json!(false));

    let upgraded = db
        .get_user_by_id(guest.id)
        .await?
        .expect("the account survives its upgrade");
    assert!(!upgraded.is_guest);
    assert_eq!(upgraded.ranked_mmr, 1_430, "ranked progress carries over");

    let board = board_rows(&db, None).await;
    assert_eq!(
        board,
        vec![("Troncat".to_string(), 1_430)],
        "exactly one Troncat: the upgrade moved no rows and created none"
    );

    let mine = my_ranking(&db, &upgraded, None).await?;
    assert_eq!(
        (mine.mmr, mine.wins, mine.losses),
        (Some(1_430), Some(1), Some(0)),
        "the badge reports the record earned as a guest"
    );

    Ok(())
}

/// The path that does strand a duplicate: registering the same nickname from a
/// browser that is not holding the guest's session (another device, cleared
/// storage, a private window). The server cannot link an account nobody can
/// prove ownership of, so the guest's rank stays behind under the same name.
#[tokio::test]
async fn registering_without_the_guest_session_leaves_the_guests_rank_behind() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let (db, auth_state, _jwt) = isolated_auth_state().await?;

    let guest = db
        .create_guest_user("Troncat", "guest-record-token", 1_000, false)
        .await?;
    db.upsert_ranking(
        guest.id,
        "Troncat",
        1_430,
        &QueueMode::Competitive,
        &DUEL,
        "us-east-1",
        0,
        true,
    )
    .await?;

    // No Authorization header: a signed-out registration.
    let (status, body) = register(&auth_state, None, "Troncat", "a-real-password").await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "the guest nickname never reserved the name, so this succeeds: {body}"
    );
    let new_id = body["user"]["id"].as_i64().expect("new account id") as i32;
    assert_ne!(new_id, guest.id, "a second, unrelated account is created");

    let new_account = db
        .get_user_by_id(new_id)
        .await?
        .expect("the new account exists");
    assert_eq!(
        my_ranking(&db, &new_account, None).await?.mmr,
        None,
        "the new account starts unranked while the guest keeps the 1430 row"
    );
    assert_eq!(
        board_rows(&db, None).await,
        vec![("Troncat".to_string(), 1_430)],
        "the visible 1430 Troncat is the guest, not the account just created"
    );

    // Both accounts are guest-flagged distinctly, which is what lets the board
    // label the stranded row instead of leaving two identical names.
    let flags = db.get_users_are_guests(&[guest.id, new_id]).await?;
    assert_eq!(flags.get(&guest.id), Some(&true));
    assert_eq!(flags.get(&new_id), Some(&false));

    Ok(())
}

/// An explicit region filter still answers about that region only, and stays
/// independent of which server handled the request.
#[tokio::test]
async fn regional_my_ranking_follows_the_requested_region() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    set_server_region("us-east-1");
    let db = isolated_db().await?;

    let player = db.create_user("Troncat", "hash", 1_000).await?;
    db.upsert_ranking(
        player.id,
        &player.username,
        1_313,
        &QueueMode::Competitive,
        &DUEL,
        "us-east-1",
        0,
        true,
    )
    .await?;
    db.upsert_ranking(
        player.id,
        &player.username,
        1_430,
        &QueueMode::Competitive,
        &DUEL,
        "eu-west-1",
        0,
        true,
    )
    .await?;

    assert_eq!(
        my_ranking(&db, &player, Some("us-east-1")).await?.mmr,
        Some(1_313)
    );
    assert_eq!(
        my_ranking(&db, &player, Some("eu-west-1")).await?.mmr,
        Some(1_430)
    );

    // The EU answer must not change just because a US server replied.
    set_server_region("eu-west-1");
    assert_eq!(
        my_ranking(&db, &player, Some("us-east-1")).await?.mmr,
        Some(1_313)
    );

    Ok(())
}
