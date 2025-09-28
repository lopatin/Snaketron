use super::*;

pub struct ReplayPlayer {
    pub replay: ReplayData,
    pub current_state: GameState,
    pub current_tick: u32,
    pub current_event_index: usize,
    pub is_playing: bool,
    pub play_speed: f32,
}

impl ReplayPlayer {
    pub fn new(replay: ReplayData) -> Self {
        let initial_state = replay.initial_state.clone();
        Self {
            replay,
            current_state: initial_state,
            current_tick: 0,
            current_event_index: 0,
            is_playing: false,
            play_speed: 1.0,
        }
    }
    
    /// Move forward by n ticks
    pub fn step_forward(&mut self, ticks: u32) {
        let target_tick = self.current_tick + ticks;
        
        // Process ticks one by one to maintain game logic
        while self.current_tick < target_tick {
            // Check if the game is already complete
            if matches!(self.current_state.status, GameStatus::Complete { .. }) {
                break;
            }
            // First, apply all events that should happen before or at this tick
            // This includes CommandScheduled events that enqueue commands
            while self.current_event_index < self.replay.events.len() {
                let event = self.replay.events[self.current_event_index].clone();
                if event.tick > self.current_tick {
                    break; // This event is for a future tick
                }
                
                // Apply the event (this may enqueue commands or update state)
                self.apply_event(&event.event);
                self.current_event_index += 1;
            }
            
            // Now tick the game forward - this processes queued commands and advances the simulation
            // This is the same method the actual game engine uses
            if let Err(e) = self.current_state.tick_forward(false) {
                eprintln!("Error during tick_forward: {:?}", e);
            }
            
            // The tick counter is incremented by tick_forward, so we sync our tracking
            self.current_tick = self.current_state.tick;
        }
    }
    
    /// Rewind by n ticks (rebuilds state from beginning)
    pub fn step_backward(&mut self, ticks: u32) {
        let target_tick = self.current_tick.saturating_sub(ticks);
        self.seek_to_tick(target_tick);
    }
    
    /// Seek to specific tick (always rebuilds from start)
    pub fn seek_to_tick(&mut self, target_tick: u32) {
        // Reset to initial state
        self.current_state = self.replay.initial_state.clone();
        self.current_tick = 0;
        self.current_event_index = 0;
        
        // Fast-forward to target tick
        if target_tick > 0 {
            self.step_forward(target_tick);
        }
    }
    
    /// Toggle play/pause
    pub fn toggle_play(&mut self) {
        // Don't allow playing if the game is complete
        if matches!(self.current_state.status, GameStatus::Complete { .. }) {
            self.is_playing = false;
        } else {
            self.is_playing = !self.is_playing;
        }
    }
    
    /// Get the maximum tick available in the replay
    pub fn max_tick(&self) -> u32 {
        self.replay.events.last()
            .map(|e| e.tick)
            .unwrap_or(0)
    }
    
    /// Get the current tick
    pub fn current_tick(&self) -> u32 {
        self.current_tick
    }
    
    /// Get the current game state
    pub fn current_state(&self) -> &GameState {
        &self.current_state
    }
    
    fn apply_event(&mut self, event_msg: &GameEventMessage) {
        let event = &event_msg.event;
        
        // Use the GameState's apply_event method for consistency
        // This ensures we handle events exactly as the game engine does
        self.current_state.apply_event(event.clone(), None);
    }
}