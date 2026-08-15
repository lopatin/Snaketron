use anyhow::{Context, Result};
use common::{
    GAME_RECORDING_FORMAT_VERSION, GAMEPLAY_REPLAY_VERSION, GameRecordingV1, GameState, GameStatus,
    GameType, HighlightClip, HighlightConfig, HighlightPresentation, HighlightReason,
    HighlightScoreBreakdown, HighlightWindow, QueueMode, ReplayAnchor, ReplayVisibility,
};
use server::completion::{
    COMPLETION_SCHEMA_VERSION, CompletionEffect, CompletionRecordV1, EffectApplyResult,
    canonical_json_bytes,
};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::redis_utils::RedisClient;
use server::replay_cache::{ReplayCacheConfig, ValkeyReplayCache};
use server::replay_repository::{ReplayLoadSource, ReplayRepository};
use server::replay_store::{ReplayStoreConfig, S3ReplayStore};
use std::sync::Arc;
use uuid::Uuid;

fn recorded_completion(game_id: u32) -> CompletionRecordV1 {
    let ended_at_ms = chrono::Utc::now().timestamp_millis();
    let mut final_state = GameState::new(
        20,
        20,
        GameType::Solo,
        QueueMode::Quickmatch,
        Some(91),
        ended_at_ms - 1_000,
    );
    final_state.status = GameStatus::Complete {
        winning_snake_id: None,
    };
    let recording = GameRecordingV1 {
        format_version: GAME_RECORDING_FORMAT_VERSION,
        gameplay_version: GAMEPLAY_REPLAY_VERSION,
        game_id,
        visibility: ReplayVisibility::Public,
        anchors: vec![ReplayAnchor {
            tick: final_state.tick,
            sequence: 0,
            state: final_state.clone(),
        }],
        messages: Vec::new(),
        end_tick: final_state.tick,
        end_sync_hash: final_state.sync_hash(),
    };
    let play_of_the_game = HighlightClip {
        clip_format_version: common::HIGHLIGHT_CLIP_FORMAT_VERSION,
        gameplay_version: GAMEPLAY_REPLAY_VERSION,
        game_id,
        star_user_id: 1,
        star_snake_id: 1,
        star_name: "Replay Tester".into(),
        reason: HighlightReason::ComboFrenzy { max_chain: 2 },
        score: 150,
        breakdown: HighlightScoreBreakdown::default(),
        window: HighlightWindow {
            start_tick: final_state.tick,
            end_tick: final_state.tick,
            focus_tick: final_state.tick,
        },
        anchor: final_state.clone(),
        messages: Vec::new(),
        end_sync_hash: final_state.sync_hash(),
        presentation: HighlightPresentation {
            rotation: 0,
            follow_snake_id: 1,
            segments: Vec::new(),
        },
        config: HighlightConfig::default(),
    };
    CompletionRecordV1 {
        schema_version: COMPLETION_SCHEMA_VERSION,
        game_id,
        partition_id: 1,
        revision: Uuid::new_v4(),
        ended_at_ms,
        server_id: 1,
        season: Some(1),
        recording: Some(recording),
        recording_canonical_bytes: None,
        recording_journal: None,
        play_of_the_game: Some(play_of_the_game),
        final_state,
        effects: vec![CompletionEffect::PersistGame { id: "game".into() }],
    }
}

/// Full local infrastructure exercise. Start `docker compose up -d localstack
/// redis` before running this ignored test.
#[tokio::test]
#[ignore = "requires LocalStack DynamoDB/S3 and local Valkey"]
async fn completion_persists_s3_reference_and_reloads_through_valkey_cache() -> Result<()> {
    let unique = Uuid::new_v4().simple().to_string();
    // SAFETY: this integration-test binary contains one test, so no sibling
    // test can observe these process-wide configuration values.
    unsafe {
        std::env::set_var("DYNAMODB_TABLE_PREFIX", format!("replay_e2e_{unique}"));
        std::env::set_var("SNAKETRON_REPLAY_S3_BUCKET", "snaketron-replays-dev");
        std::env::set_var("SNAKETRON_REPLAY_S3_PREFIX", format!("e2e-{unique}"));
        std::env::set_var("SNAKETRON_REPLAY_S3_FORCE_PATH_STYLE", "true");
        std::env::set_var("SNAKETRON_REPLAY_S3_STORAGE_CLASS", "STANDARD");
        std::env::set_var(
            "SNAKETRON_REPLAY_CACHE_PREFIX",
            format!("snaketron:replay-e2e:{unique}"),
        );
    }

    let db = DynamoDatabase::new().await?;
    let game_id = 900_000_001;
    let mut completion = recorded_completion(game_id);
    // Force final_state + highlight beyond the conservative META-row budget
    // while keeping the clip below the public-response cap. The same atomic
    // completion effect must write a split HIGHLIGHT item, and get_game_by_id
    // must hydrate it transparently.
    completion
        .play_of_the_game
        .as_mut()
        .expect("fixture has a highlight")
        .star_name = "S".repeat(205_000);
    completion.validate()?;
    let effect = &completion.effects[0];

    assert_eq!(
        db.apply_completion_effect(&completion, effect).await?,
        EffectApplyResult::Applied
    );
    assert_eq!(
        db.apply_completion_effect(&completion, effect).await?,
        EffectApplyResult::AlreadyApplied
    );

    let game = db
        .get_game_by_id(game_id as i32)
        .await?
        .context("completed game metadata row was not persisted")?;
    let metadata = game
        .replay_object
        .context("completed game row has no replay object reference")?;
    assert_eq!(metadata.game_id, game_id);
    assert_eq!(
        game.play_of_the_game
            .context("completed game row has no Play-of-the-Game clip")?
            .game_id,
        game_id
    );

    let store_config = ReplayStoreConfig::from_env()?.context("replay S3 config missing")?;
    let store = Arc::new(S3ReplayStore::new(store_config).await?);
    let redis_url =
        std::env::var("SNAKETRON_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let redis = RedisClient::open(&redis_url, None)?
        .get_managed_connection()
        .await?;
    let cache = Arc::new(ValkeyReplayCache::new(
        redis,
        ReplayCacheConfig::from_env()?,
    )?);
    let repository = ReplayRepository::new(store, cache);

    let first = repository
        .get_recording(&metadata)
        .await?
        .context("S3 replay object was not found")?;
    assert_eq!(first.source, ReplayLoadSource::ObjectStore);
    assert_eq!(
        first.recording.bytes,
        canonical_json_bytes(completion.recording.as_ref().unwrap())?
    );

    let cached = repository
        .get_recording(&metadata)
        .await?
        .context("cached replay object was not found")?;
    assert_eq!(cached.source, ReplayLoadSource::Cache);
    Ok(())
}
