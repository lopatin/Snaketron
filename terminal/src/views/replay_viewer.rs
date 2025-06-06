use super::View;
use crate::app::AppCommand;
use crate::replay::{ReplayData, player::ReplayPlayer};
use crate::render::snake::SnakeRenderer;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect, Margin},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};
use std::time::{Duration, Instant};
use std::cell::{RefCell, Cell};
use common::{GameStatus};

pub struct ReplayViewerState {
    player: ReplayPlayer,
    last_update: Instant,
    playback_accumulator: f32,
    event_log_scroll: u16,
    event_log_total_lines: Cell<u16>,
    event_log_scrollbar_state: RefCell<ScrollbarState>,
}

impl ReplayViewerState {
    pub fn new(replay_data: ReplayData) -> Self {
        Self {
            player: ReplayPlayer::new(replay_data),
            last_update: Instant::now(),
            playback_accumulator: 0.0,
            event_log_scroll: 0,
            event_log_total_lines: Cell::new(0),
            event_log_scrollbar_state: RefCell::new(ScrollbarState::default()),
        }
    }
}

impl View for ReplayViewerState {
    fn handle_input(&mut self, key: KeyEvent) -> Option<AppCommand> {
        match key.code {
            KeyCode::Char(' ') => {
                self.player.toggle_play();
                self.playback_accumulator = 0.0;
                None
            }
            KeyCode::Char('h') => {
                self.player.is_playing = false;
                self.player.step_backward(1);
                None
            }
            KeyCode::Char('l') => {
                self.player.is_playing = false;
                self.player.step_forward(1);
                None
            }
            KeyCode::Char('j') => {
                self.player.is_playing = false;
                self.player.step_backward(10);
                None
            }
            KeyCode::Char('k') => {
                self.player.is_playing = false;
                self.player.step_forward(10);
                None
            }
            KeyCode::Char('q') | KeyCode::Esc => {
                Some(AppCommand::BackToSelector)
            }
            KeyCode::PageUp => {
                self.event_log_scroll = self.event_log_scroll.saturating_sub(5);
                let mut scrollbar_state = self.event_log_scrollbar_state.borrow_mut();
                *scrollbar_state = scrollbar_state.position(self.event_log_scroll as usize);
                None
            }
            KeyCode::PageDown => {
                let visible_height = 10; // Approximate visible lines in event log
                let max_scroll = self.event_log_total_lines.get().saturating_sub(visible_height);
                self.event_log_scroll = (self.event_log_scroll + 5).min(max_scroll);
                let mut scrollbar_state = self.event_log_scrollbar_state.borrow_mut();
                *scrollbar_state = scrollbar_state.position(self.event_log_scroll as usize);
                None
            }
            _ => None,
        }
    }
    
    fn update(&mut self, dt: Duration) {
        if self.player.is_playing {
            // Accumulate time for smooth playback
            self.playback_accumulator += dt.as_secs_f32() * self.player.play_speed;
            
            // Step forward when we've accumulated enough time for a tick
            // Assuming 3 ticks per second as standard game speed
            const SECONDS_PER_TICK: f32 = 1.0 / 3.0;
            while self.playback_accumulator >= SECONDS_PER_TICK {
                self.player.step_forward(1);
                self.playback_accumulator -= SECONDS_PER_TICK;
                
                // Stop playing if we've reached the end
                if self.player.current_tick >= self.player.max_tick() {
                    self.player.is_playing = false;
                    self.playback_accumulator = 0.0;
                    break;
                }
            }
        }
    }
    
    fn render(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),  // Header
                Constraint::Min(10),    // Game area
                Constraint::Length(4),  // Status
                Constraint::Length(3),  // Controls
                Constraint::Min(5),     // Event log at bottom
            ])
            .split(frame.area());
        
        // Header with game info
        let header = self.render_header();
        frame.render_widget(header, chunks[0]);
        
        // Game arena
        self.render_arena(frame, chunks[1]);
        
        // Status info
        let status = self.render_status();
        frame.render_widget(status, chunks[2]);
        
        // Controls help
        let controls = self.render_controls();
        frame.render_widget(controls, chunks[3]);
        
        // Event log at bottom
        self.render_event_log(frame, chunks[4]);
    }
}

impl ReplayViewerState {
    fn render_header(&self) -> Paragraph {
        let title = format!(
            "Tick: {} / {} | Speed: {}x | {}",
            self.player.current_tick,
            self.player.max_tick(),
            self.player.play_speed,
            if self.player.is_playing { "▶ Playing" } else { "⏸ Paused" }
        );
        
        Paragraph::new(title)
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL))
    }
    
    fn render_arena(&self, frame: &mut Frame, area: Rect) {
        let arena = &self.player.current_state.arena;
        let block = Block::default()
            .title("Arena")
            .borders(Borders::ALL);
        
        let inner = block.inner(area);
        frame.render_widget(block, area);
        
        // Calculate cell size based on available space
        let _cell_width = inner.width as f64 / arena.width as f64;
        let _cell_height = inner.height as f64 / arena.height as f64;
        
        // Use a simple character-based rendering for now
        let mut grid = vec![vec![' '; arena.width as usize]; arena.height as usize];
        
        // Render food
        for food in &arena.food {
            if food.x >= 0 && food.x < arena.width as i16 && 
               food.y >= 0 && food.y < arena.height as i16 {
                grid[food.y as usize][food.x as usize] = '●';
            }
        }
        
        // Render snakes
        for (snake_id, snake) in arena.snakes.iter().enumerate() {
            let snake_id = snake_id as u32;
            if snake.is_alive {
                let positions = SnakeRenderer::expand_snake_body(snake);
                let snake_char = match snake_id % 4 {
                    0 => '█',
                    1 => '▓',
                    2 => '▒',
                    _ => '░',
                };
                
                for pos in positions {
                    if pos.x >= 0 && pos.x < arena.width as i16 && 
                       pos.y >= 0 && pos.y < arena.height as i16 {
                        grid[pos.y as usize][pos.x as usize] = snake_char;
                    }
                }
                
                // Mark the head with a different character
                if let Ok(head) = snake.head() {
                    if head.x >= 0 && head.x < arena.width as i16 && 
                       head.y >= 0 && head.y < arena.height as i16 {
                        grid[head.y as usize][head.x as usize] = match snake.direction {
                            common::Direction::Up => '▲',
                            common::Direction::Down => '▼',
                            common::Direction::Left => '◄',
                            common::Direction::Right => '►',
                        };
                    }
                }
            }
        }
        
        // Convert grid to lines
        let lines: Vec<Line> = grid.into_iter()
            .map(|row| {
                let text: String = row.into_iter().collect();
                Line::from(text)
            })
            .collect();
        
        let game_view = Paragraph::new(lines)
            .style(Style::default().fg(Color::White));
        
        frame.render_widget(game_view, inner);
    }
    
    fn render_status(&self) -> Paragraph {
        let mut lines = vec![];
        
        // Game status
        let status_text = match &self.player.current_state.status {
            GameStatus::Stopped => "Stopped".to_string(),
            GameStatus::Started { .. } => "In progress".to_string(),
            GameStatus::Complete { winning_snake_id } => {
                if let Some(winner_id) = winning_snake_id {
                    // Find the player who owns this snake
                    let winner_name = self.player.current_state.players.iter()
                        .find(|(_, player)| player.snake_id == *winner_id)
                        .and_then(|(user_id, _)| {
                            self.player.replay.metadata.players.iter()
                                .find(|p| p.user_id == *user_id)
                                .map(|p| &p.username)
                        });
                    if let Some(name) = winner_name {
                        format!("Complete - Winner: {}", name)
                    } else {
                        "Complete - Winner: Unknown".to_string()
                    }
                } else {
                    "Complete - Draw".to_string()
                }
            }
        };
        
        lines.push(Line::from(vec![
            Span::raw("Status: "),
            Span::styled(status_text, Style::default().fg(Color::Yellow)),
        ]));
        
        // Player info
        let alive_count = self.player.current_state.arena.snakes.iter()
            .filter(|s| s.is_alive)
            .count();
        lines.push(Line::from(format!("Alive snakes: {} / {}", 
            alive_count, 
            self.player.current_state.arena.snakes.len()
        )));
        
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL))
    }
    
    fn render_controls(&self) -> Paragraph {
        let help_text = "Space: Play/Pause | h/l: ±1 tick | j/k: ±10 ticks | PageUp/Down: Scroll event log | q: Back to menu";
        
        Paragraph::new(help_text)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL))
    }
    
    fn render_event_log(&self, frame: &mut Frame, area: Rect) {
        // Get all events up to current tick
        let current_tick = self.player.current_tick;
        let events_to_show: Vec<&crate::replay::TimestampedEvent> = self.player.replay.events
            .iter()
            .filter(|e| e.tick <= current_tick)
            .collect();
        
        // Create text lines for each event showing raw JSON
        let mut lines = Vec::new();
        
        for event in events_to_show.iter().rev() {
            // Add tick header
            lines.push(Line::from(vec![
                Span::styled(format!("=== Tick {} ===", event.tick), 
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            ]));
            
            // Serialize event to JSON
            if let Ok(json) = serde_json::to_string_pretty(&event.event) {
                // Split JSON into lines and wrap long lines
                for json_line in json.lines() {
                    // Simple line wrapping at 60 chars for the event log area
                    let max_width = area.width.saturating_sub(4) as usize; // Account for borders and padding
                    
                    if json_line.len() <= max_width {
                        lines.push(Line::from(json_line.to_string()));
                    } else {
                        // Wrap long lines
                        let mut remaining = json_line;
                        while !remaining.is_empty() {
                            let chunk_len = remaining.len().min(max_width);
                            let chunk = &remaining[..chunk_len];
                            lines.push(Line::from(chunk.to_string()));
                            remaining = &remaining[chunk_len..];
                        }
                    }
                }
            } else {
                lines.push(Line::from(format!("Failed to serialize event")));
            }
            
            // Add empty line between events
            lines.push(Line::from(""));
        }
        
        // Update total lines count
        let total_lines = lines.len();
        self.event_log_total_lines.set(total_lines as u16);
        
        // Update scrollbar state with content length
        let mut scrollbar_state = self.event_log_scrollbar_state.borrow_mut();
        *scrollbar_state = scrollbar_state
            .content_length(total_lines)
            .position(self.event_log_scroll as usize);
        
        // Create scrollable paragraph with user-controlled scroll position
        let event_log = Paragraph::new(lines)
            .block(Block::default()
                .title(format!("Raw Event Log ({} events) - PageUp/PageDown to scroll", events_to_show.len()))
                .borders(Borders::ALL))
            .style(Style::default().fg(Color::White))
            .scroll((self.event_log_scroll, 0));
        
        frame.render_widget(event_log, area);
        
        // Create and render scrollbar
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"));
        
        // Render the scrollbar inside the block borders
        frame.render_stateful_widget(
            scrollbar,
            area.inner(Margin {
                vertical: 1,
                horizontal: 0,
            }),
            &mut *scrollbar_state,
        );
    }
}