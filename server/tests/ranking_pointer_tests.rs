//! Ranking rows are keyed by rating (`MMR#{inverted}#USER#{id}`), so a user
//! cannot be found by key. A second `USER#{id}` pointer item in the same
//! partition makes "what is this user's standing?" a single keyed read instead
//! of a filtered walk of the whole partition.
//!
//! These tests pin the two properties that arrangement can break: the pointer
//! must never surface as a leaderboard entry, and it must never disagree with
//! the ladder row it mirrors.

use anyhow::Result;
use common::{GameType, QueueMode};
use server::db::{Database, dynamodb::DynamoDatabase};
use std::sync::Arc;
use uuid::Uuid;

// Each test changes the process-wide DynamoDB prefix, so this integration
// binary must serialize its setup and database lifetime.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const REGION: &str = "test-region";
const SEASON: u32 = 0;

async fn isolated_db() -> Result<Arc<dyn Database>> {
    let prefix = format!("test_ranking_pointer_{}", Uuid::new_v4().simple());
    // SAFETY: every test in this binary holds TEST_LOCK for its full lifetime.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", prefix) };
    Ok(Arc::new(DynamoDatabase::new().await?) as Arc<dyn Database>)
}

fn duel() -> GameType {
    GameType::TeamMatch { per_team: 1 }
}

async fn record_match(
    db: &Arc<dyn Database>,
    user_id: i32,
    username: &str,
    mmr: i32,
    won: bool,
) -> Result<()> {
    db.upsert_ranking(
        user_id,
        username,
        mmr,
        &QueueMode::Competitive,
        &duel(),
        REGION,
        SEASON,
        won,
    )
    .await
}

/// The pointer shares every attribute with the ladder row it mirrors, so a
/// leaderboard query that reaches it would parse it as a second, identical
/// player. A ladder shorter than the page size is where that happens: the
/// query runs out of `MMR#` rows and pages straight into the `USER#` pointers.
#[tokio::test]
async fn a_short_ladder_does_not_list_its_players_twice() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_db().await?;

    record_match(&db, 1, "alpha", 1_400, true).await?;
    record_match(&db, 2, "bravo", 1_200, false).await?;
    record_match(&db, 3, "charlie", 1_000, false).await?;

    // A limit far larger than the ladder — the case that pages into pointers.
    let entries = db
        .get_leaderboard(
            &QueueMode::Competitive,
            Some(&duel()),
            Some(REGION),
            SEASON,
            50,
        )
        .await?;

    assert_eq!(
        entries.len(),
        3,
        "expected one entry per player, got {entries:?}"
    );
    let mut user_ids: Vec<i32> = entries.iter().map(|entry| entry.user_id).collect();
    user_ids.sort_unstable();
    assert_eq!(user_ids, vec![1, 2, 3]);
    assert_eq!(
        entries.iter().map(|entry| entry.mmr).collect::<Vec<_>>(),
        vec![1_400, 1_200, 1_000],
        "entries must stay ordered by rating"
    );

    Ok(())
}

/// A rating change moves the row to a new sort key. The retired row, the new
/// row, and the pointer all move in one transaction, so the ladder must never
/// show the same player at two ratings and the pointer must agree with it.
#[tokio::test]
async fn a_rating_change_retires_the_old_row_and_moves_the_pointer() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_db().await?;

    record_match(&db, 7, "delta", 1_000, true).await?;
    record_match(&db, 7, "delta", 1_075, true).await?;
    record_match(&db, 7, "delta", 1_050, false).await?;

    let entries = db
        .get_leaderboard(
            &QueueMode::Competitive,
            Some(&duel()),
            Some(REGION),
            SEASON,
            50,
        )
        .await?;
    assert_eq!(
        entries.len(),
        1,
        "a player holds exactly one ladder row, got {entries:?}"
    );
    assert_eq!(entries[0].mmr, 1_050);

    let pointer = db
        .get_user_ranking(7, &QueueMode::Competitive, &duel(), REGION, SEASON)
        .await?
        .expect("the pointer resolves after the row moved twice");
    assert_eq!(pointer.mmr, entries[0].mmr, "pointer must match the ladder");
    assert_eq!(pointer.games_played, 3);
    assert_eq!(pointer.wins, 2);
    assert_eq!(pointer.losses, 1);

    Ok(())
}

/// An unranked user is the common case on the leaderboard page. The lookup
/// records the absence so the next request is another keyed read rather than
/// another walk of the partition, and that record must not survive the user's
/// first real match.
#[tokio::test]
async fn an_absent_ranking_resolves_and_then_yields_to_a_real_one() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_db().await?;

    record_match(&db, 1, "occupant", 1_300, true).await?;

    for _ in 0..2 {
        assert!(
            db.get_user_ranking(99, &QueueMode::Competitive, &duel(), REGION, SEASON)
                .await?
                .is_none(),
            "a user with no row on this ladder has no ranking"
        );
    }

    record_match(&db, 99, "newcomer", 1_015, true).await?;

    let ranking = db
        .get_user_ranking(99, &QueueMode::Competitive, &duel(), REGION, SEASON)
        .await?
        .expect("the first rated match replaces the recorded absence");
    assert_eq!(ranking.mmr, 1_015);
    assert_eq!(ranking.games_played, 1);
    assert_eq!(ranking.wins, 1);

    // The absence marker must not have leaked into the ladder either.
    let entries = db
        .get_leaderboard(
            &QueueMode::Competitive,
            Some(&duel()),
            Some(REGION),
            SEASON,
            50,
        )
        .await?;
    assert_eq!(entries.len(), 2, "got {entries:?}");

    Ok(())
}
