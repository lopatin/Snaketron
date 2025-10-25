use super::*;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::fs;
use std::io::Write;
use tracing::info;

pub struct GameReplayRecorder {
    game_id: u32,
    output_dir: PathBuf,
    events: Vec<TimestampedEvent>,
    initial_state: Option<GameState>,
    metadata: ReplayMetadata,
}

impl GameReplayRecorder {
    pub fn new(game_id: u32, output_dir: PathBuf) -> Self {
        Self {
            game_id,
            output_dir,
            events: Vec::new(),
            initial_state: None,
            metadata: ReplayMetadata {
                players: Vec::new(),
                start_time: SystemTime::now(),
                end_time: SystemTime::now(),
                final_status: GameStatus::Stopped,
            },
        }
    }

    pub fn set_initial_state(&mut self, state: GameState) {
        self.initial_state = Some(state);
    }

    pub fn add_player(&mut self, user_id: u32, snake_id: u32, username: String) {
        self.metadata.players.push(PlayerInfo {
            user_id,
            snake_id,
            username,
        });
    }

    pub fn record_event(&mut self, event: GameEventMessage) {
        tracing::info!("Got game event message: {:?}", event);
        let timestamped = TimestampedEvent {
            tick: event.tick,
            timestamp: SystemTime::now(),
            event,
        };
        self.events.push(timestamped);
    }

    pub fn set_final_status(&mut self, status: GameStatus) {
        self.metadata.final_status = status;
        self.metadata.end_time = SystemTime::now();
    }

    pub async fn save(&self) -> Result<PathBuf> {
        // Ensure output directory exists
        fs::create_dir_all(&self.output_dir).context("Failed to create replay output directory")?;

        let initial_state = self
            .initial_state
            .clone()
            .context("Cannot save replay without initial state")?;

        let replay = GameReplay {
            version: 1,
            game_id: self.game_id,
            initial_state,
            events: self.events.clone(),
            metadata: self.metadata.clone(),
        };

        // Generate filename with timestamp
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let filename = format!("game_{}_{}.replay", self.game_id, timestamp);
        let filepath = self.output_dir.join(&filename);

        // Save as newline-delimited JSON, compressed with gzip
        let file = fs::File::create(&filepath).context("Failed to create replay file")?;
        let mut encoder = GzEncoder::new(file, Compression::default());

        // Write metadata as first line
        let metadata_json = serde_json::to_string(&replay.metadata)?;
        writeln!(encoder, "{}", metadata_json)?;

        // Write initial state as second line
        let initial_state_json = serde_json::to_string(&replay.initial_state)?;
        writeln!(encoder, "{}", initial_state_json)?;

        // Write each event as a separate line
        for event in &replay.events {
            let event_json = serde_json::to_string(event)?;
            writeln!(encoder, "{}", event_json)?;
        }

        encoder.finish()?;

        tracing::info!("Saved replay for game {} to {:?}", self.game_id, filepath);
        Ok(filepath)
    }
}
