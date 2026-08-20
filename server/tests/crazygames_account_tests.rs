use anyhow::Result;
use common::QueueMode;
use server::db::{
    Database,
    dynamodb::DynamoDatabase,
    models::{
        CrazyGamesAccount, CrazyGamesAccountOutcome, CrazyGamesAccountResolution,
        CrazyGamesBoostInputMode, CrazyGamesGuestPromotion, CrazyGamesLobbyPreferences,
        CrazyGamesPreferences, CrazyGamesProfile,
    },
};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

// DynamoDatabase reads a process-wide table prefix during construction.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn isolated_database() -> Result<Arc<dyn Database>> {
    let prefix = format!("test_crazygames_{}", Uuid::new_v4().simple());
    // SAFETY: every test in this integration binary holds TEST_LOCK while the
    // database is being constructed and for the rest of its lifetime.
    unsafe { std::env::set_var("DYNAMODB_TABLE_PREFIX", prefix) };
    Ok(Arc::new(DynamoDatabase::new().await?))
}

fn profile(provider_user_id: &str, username: &str, iat: i64) -> CrazyGamesProfile {
    CrazyGamesProfile {
        provider_user_id: provider_user_id.to_string(),
        username: username.to_string(),
        avatar_url: format!("https://images.crazygames.com/{username}.png"),
        profile_iat: iat,
    }
}

fn preferences(tutorial: &str, competitive: bool) -> CrazyGamesPreferences {
    CrazyGamesPreferences {
        tutorial_seen: Some(HashMap::from([(tutorial.to_string(), true)])),
        lobby_preferences: Some(CrazyGamesLobbyPreferences {
            selected_modes: vec!["solo".to_string()],
            competitive,
        }),
        boost_input_mode: Some(CrazyGamesBoostInputMode::Hold),
    }
}

fn resolved(outcome: CrazyGamesAccountOutcome) -> CrazyGamesAccount {
    match outcome {
        CrazyGamesAccountOutcome::Resolved(account) => *account,
        CrazyGamesAccountOutcome::GuestLinkConsentRequired => {
            panic!("expected a resolved CrazyGames account")
        }
    }
}

#[tokio::test]
async fn guest_claim_preserves_identity_and_progress_without_username_index() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let guest = db
        .create_guest_user("GuestSnake", "guest-token", 1_000, false)
        .await?;
    let created_at = guest.created_at;
    db.update_user_mmr(guest.id, 1_234).await?;
    db.update_user_mmr_by_mode(guest.id, 125, &QueueMode::Competitive)
        .await?;
    db.update_user_mmr_by_mode(guest.id, -50, &QueueMode::Quickmatch)
        .await?;
    db.add_user_xp(guest.id, 77).await?;

    let initial = preferences("movement", false);
    let account = resolved(
        db.resolve_crazygames_account(
            &profile("cg-guest-claim", "Crazy.Player", 1_000),
            Some(guest.id),
            CrazyGamesGuestPromotion::Allow,
            Some(&initial),
        )
        .await?,
    );
    assert_eq!(
        account.resolution,
        CrazyGamesAccountResolution::GuestClaimed
    );
    assert_eq!(account.user.id, guest.id);
    assert_eq!(account.user.created_at, created_at);
    assert_eq!(account.user.mmr, 1_234);
    assert_eq!(account.user.ranked_mmr, 1_125);
    assert_eq!(account.user.casual_mmr, 950);
    assert_eq!(account.user.xp, 77);
    assert!(!account.user.is_guest);
    assert_eq!(account.user.auth_provider.as_deref(), Some("crazygames"));
    assert_eq!(
        account.user.crazygames_user_id.as_deref(),
        Some("cg-guest-claim")
    );
    assert_eq!(account.preferences, initial);
    assert!(db.get_user_by_username("Crazy.Player").await?.is_none());

    // CrazyGames accounts intentionally have no password username mirror;
    // every progression writer must continue to work without one.
    assert_eq!(
        db.update_user_mmr_by_mode(guest.id, 25, &QueueMode::Competitive)
            .await?,
        1_150
    );
    assert_eq!(db.add_user_xp(guest.id, 3).await?, 80);

    let saved = db
        .save_crazygames_preferences(
            guest.id,
            &CrazyGamesPreferences {
                tutorial_seen: Some(HashMap::from([
                    ("movement".to_string(), false),
                    ("boost".to_string(), true),
                ])),
                boost_input_mode: Some(CrazyGamesBoostInputMode::Toggle),
                ..Default::default()
            },
        )
        .await?;
    let tutorials = saved.tutorial_seen.expect("tutorials");
    assert_eq!(tutorials.get("movement"), Some(&true));
    assert_eq!(tutorials.get("boost"), Some(&true));
    assert_eq!(
        saved.boost_input_mode,
        Some(CrazyGamesBoostInputMode::Toggle)
    );
    Ok(())
}

#[tokio::test]
async fn guest_promotion_check_is_read_only_and_repeatable() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let guest = db
        .create_guest_user("ConsentGuest", "consent-token", 1_000, false)
        .await?;
    db.add_user_xp(guest.id, 29).await?;
    let initial = preferences("movement", true);
    let identity = profile("cg-consent-check", "Consent.Player", 1_500);

    for _ in 0..2 {
        let outcome = db
            .resolve_crazygames_account(
                &identity,
                Some(guest.id),
                CrazyGamesGuestPromotion::Check,
                Some(&initial),
            )
            .await?;
        assert!(matches!(
            outcome,
            CrazyGamesAccountOutcome::GuestLinkConsentRequired
        ));

        let persisted = db
            .get_user_by_id(guest.id)
            .await?
            .expect("guest remains after consent check");
        assert!(persisted.is_guest);
        assert_eq!(persisted.username, "ConsentGuest");
        assert_eq!(persisted.xp, 29);
        assert!(persisted.auth_provider.is_none());
        assert!(persisted.crazygames_user_id.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn guest_promotion_decline_creates_separate_account_without_importing_preferences()
-> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let guest = db
        .create_guest_user("DeclineGuest", "decline-token", 1_000, false)
        .await?;
    db.add_user_xp(guest.id, 37).await?;
    let identity = profile("cg-consent-decline", "Separate.Player", 1_750);
    let initial = preferences("must-not-import", true);

    let created = resolved(
        db.resolve_crazygames_account(
            &identity,
            Some(guest.id),
            CrazyGamesGuestPromotion::Decline,
            Some(&initial),
        )
        .await?,
    );
    assert_eq!(created.resolution, CrazyGamesAccountResolution::Created);
    assert_ne!(created.user.id, guest.id);
    assert_eq!(created.preferences, CrazyGamesPreferences::default());

    let persisted_guest = db
        .get_user_by_id(guest.id)
        .await?
        .expect("declined guest remains");
    assert!(persisted_guest.is_guest);
    assert_eq!(persisted_guest.xp, 37);
    assert!(persisted_guest.auth_provider.is_none());

    // Once the identity exists, even a later check with an eligible guest is
    // a normal returning login and must never prompt or claim that guest.
    let returning = resolved(
        db.resolve_crazygames_account(
            &identity,
            Some(guest.id),
            CrazyGamesGuestPromotion::Check,
            Some(&initial),
        )
        .await?,
    );
    assert_eq!(returning.resolution, CrazyGamesAccountResolution::Returning);
    assert_eq!(returning.user.id, created.user.id);
    assert!(
        db.get_user_by_id(guest.id)
            .await?
            .expect("guest remains after returning login")
            .is_guest
    );
    Ok(())
}

#[tokio::test]
async fn returning_identity_wins_and_only_newer_verified_profile_is_applied() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let original_preferences = preferences("movement", false);
    let created = resolved(
        db.resolve_crazygames_account(
            &profile("cg-returning", "First.Name", 1_000),
            None,
            CrazyGamesGuestPromotion::Decline,
            // Unscoped browser preferences must not seed a newly created
            // provider account on a shared device.
            Some(&original_preferences),
        )
        .await?,
    );
    assert_eq!(created.resolution, CrazyGamesAccountResolution::Created);
    assert_eq!(created.preferences, CrazyGamesPreferences::default());
    let original_preferences = db
        .save_crazygames_preferences(created.user.id, &original_preferences)
        .await?;

    let unrelated_guest = db
        .create_guest_user("UnrelatedGuest", "unrelated-token", 1_000, false)
        .await?;
    db.add_user_xp(unrelated_guest.id, 41).await?;
    let replacement_initial = preferences("should-not-overwrite", true);
    let returning = resolved(
        db.resolve_crazygames_account(
            &profile("cg-returning", "Newer.Name", 2_000),
            Some(unrelated_guest.id),
            CrazyGamesGuestPromotion::Check,
            Some(&replacement_initial),
        )
        .await?,
    );
    assert_eq!(returning.resolution, CrazyGamesAccountResolution::Returning);
    assert_eq!(returning.user.id, created.user.id);
    assert_eq!(returning.profile.username, "Newer.Name");
    assert_eq!(returning.preferences, original_preferences);
    let untouched_guest = db
        .get_user_by_id(unrelated_guest.id)
        .await?
        .expect("unrelated guest remains");
    assert!(untouched_guest.is_guest);
    assert_eq!(untouched_guest.xp, 41);

    let stale = resolved(
        db.resolve_crazygames_account(
            &profile("cg-returning", "Stale.Name", 1_500),
            None,
            CrazyGamesGuestPromotion::Decline,
            None,
        )
        .await?,
    );
    assert_eq!(stale.user.id, created.user.id);
    assert_eq!(stale.profile.username, "Newer.Name");
    assert_eq!(stale.user.username, "Newer.Name");
    Ok(())
}

#[tokio::test]
async fn concurrent_first_launches_converge_on_exactly_one_account() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let identity = profile("cg-concurrent", "Race.Player", 3_000);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let db = db.clone();
        let identity = identity.clone();
        tasks.push(tokio::spawn(async move {
            db.resolve_crazygames_account(&identity, None, CrazyGamesGuestPromotion::Decline, None)
                .await
        }));
    }

    let mut ids = Vec::new();
    let mut created = 0;
    for task in tasks {
        let account = resolved(task.await??);
        ids.push(account.user.id);
        created += usize::from(account.resolution == CrazyGamesAccountResolution::Created);
    }
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(created, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_progress_writes_survive_crazygames_guest_claim() -> Result<()> {
    const STEPS: i32 = 6;

    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let guest = db
        .create_guest_user("ProgressGuest", "progress-token", 1_000, false)
        .await?;
    let guest_id = guest.id;
    let barrier = Arc::new(tokio::sync::Barrier::new(3));

    let claim_db = db.clone();
    let claim_barrier = barrier.clone();
    let claim_task = tokio::spawn(async move {
        claim_barrier.wait().await;
        claim_db
            .resolve_crazygames_account(
                &profile("cg-progress-race", "Progress.Player", 3_500),
                Some(guest_id),
                CrazyGamesGuestPromotion::Allow,
                None,
            )
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
    let (claim_result, progress_result) = tokio::join!(claim_task, progress_task);
    let claimed = resolved(claim_result??);
    progress_result??;
    assert_eq!(claimed.user.id, guest_id);
    assert_eq!(
        claimed.resolution,
        CrazyGamesAccountResolution::GuestClaimed
    );

    let persisted = db
        .get_user_by_id(guest_id)
        .await?
        .expect("claimed user remains");
    assert!(!persisted.is_guest);
    assert_eq!(persisted.auth_provider.as_deref(), Some("crazygames"));
    assert_eq!(persisted.xp, STEPS);
    assert_eq!(persisted.ranked_mmr, 1_000 + (STEPS * 3));
    assert_eq!(persisted.casual_mmr, 1_000 - (STEPS * 2));
    Ok(())
}

#[tokio::test]
async fn stress_guest_is_never_claimed() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let db = isolated_database().await?;
    let stress_guest = db
        .create_guest_user("StressSnake", "stress-token", 1_000, true)
        .await?;
    let account = resolved(
        db.resolve_crazygames_account(
            &profile("cg-no-stress", "Public.Player", 4_000),
            Some(stress_guest.id),
            CrazyGamesGuestPromotion::Allow,
            None,
        )
        .await?,
    );
    assert_eq!(account.resolution, CrazyGamesAccountResolution::Created);
    assert_ne!(account.user.id, stress_guest.id);
    assert!(
        db.get_user_by_id(stress_guest.id)
            .await?
            .expect("stress guest remains")
            .is_guest
    );
    Ok(())
}
