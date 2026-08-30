//! The durable administrator flag, proved end to end against DynamoDB.
//!
//! The point of these tests is the seam that unit tests cannot cover: an
//! operator writes `isAdmin` onto an account row out of band (with
//! `scripts/set-user-admin.sh` in the deployment repository) and never touches
//! the server, so the only thing that can carry the grant through is
//! `get_user_by_id` actually reading the attribute back off the item.
//!
//! They write the attribute exactly the way that script does — a bare
//! `UpdateItem` with `SET isAdmin = :admin` — rather than through any server
//! API, because there deliberately is no server API for granting admin.

use anyhow::Result;
use aws_sdk_dynamodb::types::AttributeValue;
use server::{
    api::middleware::is_admin_user,
    db::{Database, dynamodb::DynamoDatabase, dynamodb::dynamodb_client},
};
use std::sync::Arc;
use uuid::Uuid;

/// Each test rewrites the process-wide DynamoDB prefix, so this binary has to
/// serialize setup and the database lifetime, exactly as the other DynamoDB
/// integration binaries do.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Fixture {
    db: Arc<dyn Database>,
    main_table: String,
}

async fn isolated_database() -> Result<Fixture> {
    let prefix = format!("test_admin_flag_{}", Uuid::new_v4().simple());
    // SAFETY: every test in this binary holds TEST_LOCK for its full lifetime.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", &prefix) };

    Ok(Fixture {
        db: Arc::new(DynamoDatabase::new().await?) as Arc<dyn Database>,
        main_table: format!("{prefix}-main"),
    })
}

/// Flip `isAdmin` on an account row the way the operator script does: an
/// `UpdateItem` against `USER#<id>` / `META`, touching nothing else.
async fn write_admin_flag(main_table: &str, user_id: i32, is_admin: bool) -> Result<()> {
    dynamodb_client()
        .await
        .update_item()
        .table_name(main_table)
        .key("pk", AttributeValue::S(format!("USER#{user_id}")))
        .key("sk", AttributeValue::S("META".to_string()))
        .update_expression("SET isAdmin = :admin")
        .expression_attribute_values(":admin", AttributeValue::Bool(is_admin))
        .send()
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_new_account_is_not_an_administrator() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let fixture = isolated_database().await?;

    let created = fixture.db.create_user("freshly-made", "hash", 1000).await?;
    assert!(!created.is_admin, "a new account is never born an admin");

    let loaded = fixture
        .db
        .get_user_by_id(created.id)
        .await?
        .expect("the account was just created");
    assert!(!loaded.is_admin);
    assert!(!is_admin_user(&loaded));

    Ok(())
}

/// The whole point of the flag: it survives the round trip through DynamoDB
/// and grants administrative access on the very next read, with no deploy, no
/// restart, and no `SNAKETRON_ADMIN_USER_IDS` entry.
#[tokio::test]
async fn an_out_of_band_grant_is_read_back_and_honoured() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let fixture = isolated_database().await?;

    let created = fixture.db.create_user("operator", "hash", 1000).await?;
    write_admin_flag(&fixture.main_table, created.id, true).await?;

    let promoted = fixture
        .db
        .get_user_by_id(created.id)
        .await?
        .expect("the account still exists");
    assert!(promoted.is_admin, "the flag must survive the round trip");
    assert!(is_admin_user(&promoted));

    // And revoking is just as immediate.
    write_admin_flag(&fixture.main_table, created.id, false).await?;
    let demoted = fixture
        .db
        .get_user_by_id(created.id)
        .await?
        .expect("the account still exists");
    assert!(!demoted.is_admin);
    assert!(!is_admin_user(&demoted));

    Ok(())
}

/// Granting the flag must not disturb anything else on the account row. The
/// script uses `UpdateItem` rather than a full-item `PutItem` precisely so a
/// promotion cannot cost somebody their rating or their progress.
#[tokio::test]
async fn a_grant_leaves_the_rest_of_the_account_alone() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let fixture = isolated_database().await?;

    let created = fixture
        .db
        .create_user("intact", "secret-hash", 1234)
        .await?;
    fixture.db.add_user_xp(created.id, 77).await?;
    let before = fixture
        .db
        .get_user_by_id(created.id)
        .await?
        .expect("the account exists");

    write_admin_flag(&fixture.main_table, created.id, true).await?;

    let after = fixture
        .db
        .get_user_by_id(created.id)
        .await?
        .expect("the account exists");
    assert!(after.is_admin);
    assert_eq!(after.username, before.username);
    assert_eq!(after.password_hash, before.password_hash);
    assert_eq!(after.mmr, before.mmr);
    assert_eq!(after.ranked_mmr, before.ranked_mmr);
    assert_eq!(after.casual_mmr, before.casual_mmr);
    assert_eq!(after.xp, before.xp);
    assert_eq!(after.games_played, before.games_played);
    assert_eq!(after.created_at, before.created_at);
    assert!(!after.is_guest);

    Ok(())
}

/// The exclusion lives on the decision, not on the grant, so even a flag
/// written straight onto a guest row buys nothing. The operator script refuses
/// to write one — this proves the server would not honour it if anything else
/// did.
#[tokio::test]
async fn a_flagged_guest_is_still_refused() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let fixture = isolated_database().await?;

    let guest = fixture
        .db
        .create_guest_user("wandering-guest", "guest-token", 1000, false)
        .await?;
    write_admin_flag(&fixture.main_table, guest.id, true).await?;

    let flagged = fixture
        .db
        .get_user_by_id(guest.id)
        .await?
        .expect("the guest exists");
    assert!(flagged.is_guest);
    assert!(flagged.is_admin, "the attribute is there on the row");
    assert!(
        !is_admin_user(&flagged),
        "but a guest is never an administrator"
    );

    Ok(())
}
