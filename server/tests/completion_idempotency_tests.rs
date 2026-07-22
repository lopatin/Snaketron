use anyhow::Result;
use chrono::Utc;
use common::{GameState, GameStatus, GameType, QueueMode};
use server::completion::{
    COMPLETION_SCHEMA_VERSION, CompletionEffect, CompletionRecordV1, EffectApplyResult,
    apply_all_effects, materialize_completion,
};
use server::db::{Database, dynamodb::DynamoDatabase, models::User};
use uuid::Uuid;

fn completed_state(
    game_type: GameType,
    queue_mode: QueueMode,
    users: &[&User],
    started_at_ms: i64,
) -> Result<GameState> {
    let mut state = GameState::new(40, 40, game_type, queue_mode, Some(7), started_at_ms);
    for user in users {
        state.add_player(user.id as u32, Some(user.username.clone()))?;
    }
    state.status = GameStatus::Complete {
        winning_snake_id: None,
    };
    Ok(state)
}

fn completion_record(
    game_id: u32,
    final_state: GameState,
    ended_at_ms: i64,
    effects: Vec<CompletionEffect>,
) -> CompletionRecordV1 {
    CompletionRecordV1 {
        schema_version: COMPLETION_SCHEMA_VERSION,
        game_id,
        partition_id: 7,
        revision: Uuid::new_v4(),
        ended_at_ms,
        server_id: 42,
        final_state,
        effects,
    }
}

fn ranked_effects(user: &User, delta: i32, won: bool) -> Vec<CompletionEffect> {
    vec![
        CompletionEffect::PersistGame { id: "game".into() },
        CompletionEffect::AddMmr {
            id: format!("mmr:{}", user.id),
            user_id: user.id as u32,
            username: user.username.clone(),
            delta,
            queue_mode: QueueMode::Competitive,
        },
        CompletionEffect::UpdateRanking {
            id: format!("ranking:{}", user.id),
            user_id: user.id as u32,
            username: user.username.clone(),
            queue_mode: QueueMode::Competitive,
            game_type: GameType::TeamMatch { per_team: 1 },
            region: "test".into(),
            season: 1,
            won,
        },
    ]
}

/// Exercises the durable completion boundary against the same local DynamoDB
/// endpoint as the rest of the server integration suite. One test owns the
/// process-wide prefix for its entire lifetime, avoiding environment races.
#[tokio::test]
async fn durable_completion_effects_survive_replay_and_concurrency() -> Result<()> {
    let prefix = format!("test_completion_{}", Uuid::new_v4().simple());
    // SAFETY: this integration-test binary contains one test, so no sibling
    // thread can observe its process-wide table prefix.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", prefix) };

    let db = DynamoDatabase::new().await?;
    let now = Utc::now().timestamp_millis();

    // A game may finish on a different executor than the server that created
    // its metadata row. Durable ID ownership, not server affinity, proves the
    // metadata-only row is safe to adopt.
    let metadata_game_id = db
        .create_game(
            7,
            &serde_json::to_value(GameType::Solo)?,
            "matchmaking",
            false,
            None,
        )
        .await?;
    let metadata_state = completed_state(GameType::Solo, QueueMode::Quickmatch, &[], now - 1_000)?;
    let metadata_record = completion_record(
        metadata_game_id as u32,
        metadata_state,
        now,
        vec![CompletionEffect::PersistGame { id: "game".into() }],
    );
    db.apply_completion_effect(&metadata_record, &metadata_record.effects[0])
        .await?;
    let adopted = db
        .get_game_by_id(metadata_game_id)
        .await?
        .expect("metadata-only game was adopted by takeover executor");
    assert_eq!(adopted.server_id, Some(42));
    assert_eq!(adopted.status, "complete");

    // Every mutation and its marker commit together. Registered-user mirrors
    // receive exactly the same XP/MMR increments as the main user item.
    let user = db.create_user("completion_user", "hash", 1_000).await?;
    let mut final_state = completed_state(
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Competitive,
        &[&user],
        now - 1_000,
    )?;
    final_state.player_xp.insert(user.id as u32, 15);
    let effects = vec![
        CompletionEffect::PersistGame { id: "game".into() },
        CompletionEffect::AddXp {
            id: format!("xp:{}", user.id),
            user_id: user.id as u32,
            username: user.username.clone(),
            amount: 15,
        },
        CompletionEffect::AddMmr {
            id: format!("mmr:{}", user.id),
            user_id: user.id as u32,
            username: user.username.clone(),
            delta: 25,
            queue_mode: QueueMode::Competitive,
        },
        CompletionEffect::UpdateRanking {
            id: format!("ranking:{}", user.id),
            user_id: user.id as u32,
            username: user.username.clone(),
            queue_mode: QueueMode::Competitive,
            game_type: GameType::TeamMatch { per_team: 1 },
            region: "test".into(),
            season: 1,
            won: true,
        },
    ];
    let record = completion_record(1_000_000_777, final_state, now, effects);
    record.validate()?;

    for effect in &record.effects {
        assert_eq!(
            db.apply_completion_effect(&record, effect).await?,
            EffectApplyResult::Applied
        );
        assert_eq!(
            db.apply_completion_effect(&record, effect).await?,
            EffectApplyResult::AlreadyApplied
        );
    }

    let persisted_user = db
        .get_user_by_id(user.id)
        .await?
        .expect("user remains present");
    let mirrored_user = db
        .get_user_by_username(&user.username)
        .await?
        .expect("registered-user mirror remains present");
    assert_eq!((persisted_user.xp, persisted_user.ranked_mmr), (15, 1_025));
    assert_eq!((mirrored_user.xp, mirrored_user.ranked_mmr), (15, 1_025));

    let ranking = db
        .get_user_ranking(
            user.id,
            &QueueMode::Competitive,
            &GameType::TeamMatch { per_team: 1 },
            "test",
            1,
        )
        .await?
        .expect("ranking effect created one row");
    assert_eq!(
        (ranking.games_played, ranking.wins, ranking.losses),
        (1, 1, 0)
    );
    assert_eq!(ranking.mmr, 1_025);

    // The per-game revision anchor protects every effect, even if a caller
    // attempts another revision before/without applying PersistGame.
    let mut conflicting_completion = record.clone();
    conflicting_completion.revision = Uuid::new_v4();
    assert!(
        db.apply_completion_effect(&conflicting_completion, &conflicting_completion.effects[1])
            .await
            .is_err(),
        "a second completion revision must not apply any external effect"
    );

    // An ambiguous duplicate submission converges through the transactional
    // marker: one caller applies it and one observes AlreadyApplied.
    let mut raced_state = completed_state(GameType::Solo, QueueMode::Quickmatch, &[&user], now)?;
    raced_state.player_xp.insert(user.id as u32, 5);
    let raced_record = completion_record(
        1_000_000_778,
        raced_state,
        now + 1,
        vec![
            CompletionEffect::PersistGame { id: "game".into() },
            CompletionEffect::AddXp {
                id: format!("xp:{}", user.id),
                user_id: user.id as u32,
                username: user.username.clone(),
                amount: 5,
            },
            CompletionEffect::InsertHighScore {
                id: format!("high_score:{}", user.id),
                user_id: user.id as u32,
                username: user.username.clone(),
                score: 0,
                game_type: GameType::Solo,
                region: "test".into(),
                season: 1,
            },
        ],
    );
    assert!(
        db.apply_completion_effect(&raced_record, &raced_record.effects[1])
            .await
            .is_err(),
        "rewards must not commit before PersistGame proves the game identity"
    );
    assert_eq!(
        db.get_user_by_id(user.id)
            .await?
            .expect("user remains present")
            .xp,
        15
    );
    db.apply_completion_effect(&raced_record, &raced_record.effects[0])
        .await?;
    let raced_effect = &raced_record.effects[1];
    let (left, right) = tokio::join!(
        db.apply_completion_effect(&raced_record, raced_effect),
        db.apply_completion_effect(&raced_record, raced_effect),
    );
    let raced_results = [left?, right?];
    assert!(raced_results.contains(&EffectApplyResult::Applied));
    assert!(raced_results.contains(&EffectApplyResult::AlreadyApplied));
    assert_eq!(
        db.get_user_by_id(user.id)
            .await?
            .expect("user remains present")
            .xp,
        20
    );
    assert_eq!(
        db.get_user_by_username(&user.username)
            .await?
            .expect("mirror remains present")
            .xp,
        20
    );

    let mismatched_retry = CompletionEffect::AddXp {
        id: format!("xp:{}", user.id),
        user_id: user.id as u32,
        username: user.username.clone(),
        amount: 999,
    };
    assert!(
        db.apply_completion_effect(&record, &mismatched_retry)
            .await
            .is_err(),
        "one effect identity cannot be reused with another payload"
    );

    // Guest users intentionally have no username-table mirror. Their main
    // mutation remains transactional instead of failing forever on a missing
    // mirror row.
    let guest = db
        .create_guest_user("completion_guest", "guest-token", 1_000)
        .await?;
    let mut guest_state = completed_state(GameType::Solo, QueueMode::Quickmatch, &[&guest], now)?;
    guest_state.player_xp.insert(guest.id as u32, 7);
    let guest_record = completion_record(
        1_000_000_779,
        guest_state,
        now + 2,
        vec![
            CompletionEffect::PersistGame { id: "game".into() },
            CompletionEffect::AddXp {
                id: format!("xp:{}", guest.id),
                user_id: guest.id as u32,
                username: guest.username.clone(),
                amount: 7,
            },
            CompletionEffect::InsertHighScore {
                id: format!("high_score:{}", guest.id),
                user_id: guest.id as u32,
                username: guest.username.clone(),
                score: 0,
                game_type: GameType::Solo,
                region: "test".into(),
                season: 1,
            },
        ],
    );
    apply_all_effects(&db, &guest_record).await?;
    assert_eq!(
        db.get_user_by_id(guest.id)
            .await?
            .expect("guest remains present")
            .xp,
        7
    );

    // Two games for one user may concurrently move the same sorted ranking
    // row. Conditional delete/put plus bounded re-read retries must preserve
    // both game counters and leave exactly one row at the final combined MMR.
    let ranking_user = db.create_user("ranking_user", "hash", 1_000).await?;
    let ranking_state_a = completed_state(
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Competitive,
        &[&ranking_user],
        now,
    )?;
    let ranking_state_b = completed_state(
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Competitive,
        &[&ranking_user],
        now + 1,
    )?;
    let ranking_record_a = completion_record(
        1_000_000_780,
        ranking_state_a,
        now + 3,
        ranked_effects(&ranking_user, 40, true),
    );
    let ranking_record_b = completion_record(
        1_000_000_781,
        ranking_state_b,
        now + 4,
        ranked_effects(&ranking_user, -15, false),
    );
    let (ranking_a, ranking_b) = tokio::join!(
        apply_all_effects(&db, &ranking_record_a),
        apply_all_effects(&db, &ranking_record_b),
    );
    assert!(
        ranking_a?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::Applied)
    );
    assert!(
        ranking_b?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::Applied)
    );

    let ranked_main = db
        .get_user_by_id(ranking_user.id)
        .await?
        .expect("ranking user remains present");
    let ranked_mirror = db
        .get_user_by_username(&ranking_user.username)
        .await?
        .expect("ranking mirror remains present");
    assert_eq!(ranked_main.ranked_mmr, 1_025);
    assert_eq!(ranked_mirror.ranked_mmr, 1_025);
    let concurrent_ranking = db
        .get_user_ranking(
            ranking_user.id,
            &QueueMode::Competitive,
            &GameType::TeamMatch { per_team: 1 },
            "test",
            1,
        )
        .await?
        .expect("concurrent ranking remains present");
    assert_eq!(concurrent_ranking.mmr, 1_025);
    assert_eq!(
        (
            concurrent_ranking.games_played,
            concurrent_ranking.wins,
            concurrent_ranking.losses,
        ),
        (2, 1, 1)
    );
    let leaderboard = db
        .get_leaderboard(
            &QueueMode::Competitive,
            Some(&GameType::TeamMatch { per_team: 1 }),
            Some("test"),
            1,
            100,
        )
        .await?;
    assert_eq!(
        leaderboard
            .iter()
            .filter(|entry| entry.user_id == ranking_user.id)
            .count(),
        1,
        "concurrent sorted-key moves must not leave duplicate ranking rows"
    );
    assert!(
        apply_all_effects(&db, &ranking_record_a)
            .await?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::AlreadyApplied)
    );
    assert!(
        apply_all_effects(&db, &ranking_record_b)
            .await?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::AlreadyApplied)
    );

    // Materialization captures high-score payloads before the authoritative
    // commit. Equal scores from two users in one game remain distinct, and a
    // serialize/reload replay cannot create or overwrite either row.
    let high_user_a = db.create_user("high_user_a", "hash", 1_000).await?;
    let high_user_b = db.create_user("high_user_b", "hash", 1_000).await?;
    let mut solo_state = completed_state(
        GameType::Solo,
        QueueMode::Quickmatch,
        &[&high_user_a, &high_user_b],
        now,
    )?;
    for player in solo_state.players.values() {
        solo_state.scores.insert(player.snake_id, 777);
    }
    let high_record =
        materialize_completion(&db, 1_000_000_782, 7, 42, solo_state, now + 5).await?;
    let (high_region, high_season) = high_record
        .effects
        .iter()
        .find_map(|effect| match effect {
            CompletionEffect::InsertHighScore { region, season, .. } => {
                Some((region.clone(), *season))
            }
            _ => None,
        })
        .expect("materialized solo completion contains high scores");
    assert!(
        apply_all_effects(&db, &high_record)
            .await?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::Applied)
    );
    let reloaded: CompletionRecordV1 = serde_json::from_slice(&serde_json::to_vec(&high_record)?)?;
    assert!(
        apply_all_effects(&db, &reloaded)
            .await?
            .iter()
            .all(|(_, result)| *result == EffectApplyResult::AlreadyApplied)
    );
    let high_scores = db
        .get_high_scores(&GameType::Solo, Some(&high_region), high_season, 100)
        .await?;
    let game_scores: Vec<_> = high_scores
        .iter()
        .filter(|entry| entry.game_id == high_record.game_id.to_string())
        .collect();
    assert_eq!(game_scores.len(), 2);
    assert!(game_scores.iter().all(|entry| entry.score == 777));

    Ok(())
}
