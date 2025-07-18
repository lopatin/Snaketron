use super::*;
use tokio::sync::broadcast;
use common::GameEvent;
use std::fs;

pub struct ReplayListener {
    recorders: Arc<RwLock<HashMap<u32, GameReplayRecorder>>>,
    output_dir: PathBuf,
}

impl ReplayListener {
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            recorders: Arc::new(RwLock::new(HashMap::new())),
            output_dir,
        }
    }


    async fn handle_game_event(&self, game_id: u32, event: GameEventMessage) -> Result<()> {
        // Check if this is a completion event before acquiring the lock
        let is_completion = matches!(&event.event, GameEvent::StatusUpdated { status } if matches!(status, GameStatus::Complete { .. }));
        let completion_status = if is_completion {
            if let GameEvent::StatusUpdated { status } = &event.event {
                Some(status.clone())
            } else {
                None
            }
        } else {
            None
        };
        
        // Handle the event recording
        {
            let mut recorders = self.recorders.write().await;
            
            if let Some(recorder) = recorders.get_mut(&game_id) {
                // Handle special case where this is a Snapshot event (initial state)
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    if let GameEvent::Snapshot { game_state } = &event.event {
                        recorder.set_initial_state(game_state.clone());
                    }
                }
                
                recorder.record_event(event.clone());
                
                // If this is a completion event, handle it inline to avoid deadlock
                if let Some(status) = completion_status {
                    recorder.set_final_status(status.clone());
                    let path = recorder.save().await?;
                    recorders.remove(&game_id);
                    tracing::info!("Game {} completed, replay saved to {:?}", game_id, path);
                }
            } else {
                // If we receive an event for a game we're not tracking, create a recorder
                // This can happen if the replay listener starts after games are already running
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    if let GameEvent::Snapshot { game_state } = &event.event {
                        let mut recorder = GameReplayRecorder::new(game_id, self.output_dir.clone());
                        recorder.set_initial_state(game_state.clone());
                        
                        for (user_id, player) in &game_state.players {
                            recorder.add_player(*user_id, player.snake_id, format!("Player_{}", user_id));
                        }
                        
                        recorder.record_event(event.clone());
                        recorders.insert(game_id, recorder);
                        tracing::debug!("Created replay recorder for existing game {}", game_id);
                    }
                }
            }
        } // Lock is released here
        
        Ok(())
    }


    pub async fn finalize_game(&self, game_id: u32) -> Result<PathBuf> {
        let mut recorders = self.recorders.write().await;
        
        let recorder = recorders.remove(&game_id)
            .context("No replay recorder found for game")?;
        
        recorder.save().await
    }

    pub async fn get_replay_path(&self, game_id: u32) -> Result<PathBuf> {
        // Check if the game is still being recorded
        let recorders = self.recorders.read().await;
        if recorders.contains_key(&game_id) {
            return Err(anyhow::anyhow!("Game {} is still in progress", game_id));
        }
        
        // Look for the replay file
        let pattern = format!("game_{}_*.replay", game_id);
        let entries = fs::read_dir(&self.output_dir)?;
        
        for entry in entries {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();
            
            if filename_str.contains(&game_id.to_string()) && filename_str.ends_with(".replay") {
                return Ok(entry.path());
            }
        }
        
        Err(anyhow::anyhow!("No replay file found for game {}", game_id))
    }
}