use super::*;
use common::{GameEvent};

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
            self.current_tick += 1;
            self.current_state.tick = self.current_tick;
            
            // Step all alive snakes forward
            for snake in self.current_state.arena.snakes.iter_mut() {
                if snake.is_alive {
                    snake.step_forward();
                }
            }
            
            // Apply events at this tick
            while self.current_event_index < self.replay.events.len() {
                let event = self.replay.events[self.current_event_index].clone();
                if event.tick == self.current_tick {
                    self.apply_event(&event.event);
                    self.current_event_index += 1;
                } else if event.tick > self.current_tick {
                    break;
                } else {
                    // Skip past events (shouldn't happen in normal playback)
                    self.current_event_index += 1;
                }
            }
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
        self.is_playing = !self.is_playing;
    }
    
    /// Get the maximum tick available in the replay
    pub fn max_tick(&self) -> u32 {
        self.replay.events.last()
            .map(|e| e.tick)
            .unwrap_or(0)
    }
    
    fn apply_event(&mut self, event_msg: &GameEventMessage) {
        let event = &event_msg.event;
        
        match event {
            GameEvent::SnakeTurned { snake_id, direction } => {
                if let Some(snake) = self.current_state.arena.snakes.get_mut(*snake_id as usize) {
                    snake.direction = *direction;
                }
            }
            GameEvent::SnakeDied { snake_id } => {
                if let Some(snake) = self.current_state.arena.snakes.get_mut(*snake_id as usize) {
                    snake.is_alive = false;
                }
            }
            GameEvent::FoodEaten { snake_id, position } => {
                if let Some(snake) = self.current_state.arena.snakes.get_mut(*snake_id as usize) {
                    snake.food += 1;
                }
                // Remove food from arena
                self.current_state.arena.food.retain(|&pos| pos != *position);
            }
            GameEvent::FoodSpawned { position } => {
                self.current_state.arena.food.push(*position);
            }
            GameEvent::CommandScheduled { command_message } => {
                // Add command to the queue
                self.current_state.command_queue.push(command_message.clone());
            }
            GameEvent::StatusUpdated { status } => {
                self.current_state.status = status.clone();
            }
            GameEvent::Snapshot { game_state } => {
                // Snapshot events update the entire state
                self.current_state = game_state.clone();
            }
        }
    }
}