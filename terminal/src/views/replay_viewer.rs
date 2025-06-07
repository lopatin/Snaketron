use super::View;
use crate::app::AppCommand;
use crate::replay::{ReplayData, player::ReplayPlayer};
use crate::render::arena::ArenaRenderer;
use crate::render::standard_renderer::StandardRenderer;
use crate::render::types::{RenderConfig, CharDimensions};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum LayoutMode {
    SingleColumn,  // Tall/narrow screens
    TwoColumn,     // Wide screens
}

impl LayoutMode {
    fn from_dimensions(width: u16, height: u16) -> Self {
        // Aspect ratio and minimum width thresholds
        let aspect_ratio = width as f32 / height as f32;
        const MIN_WIDTH_FOR_TWO_COLUMN: u16 = 100;  // Reduced from 120
        const ASPECT_RATIO_THRESHOLD: f32 = 1.8;    // Increased from 1.5 for better layout
        
        if width >= MIN_WIDTH_FOR_TWO_COLUMN && aspect_ratio >= ASPECT_RATIO_THRESHOLD {
            LayoutMode::TwoColumn
        } else {
            LayoutMode::SingleColumn
        }
    }
}

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
                self.player.step_backward(5);
                None
            }
            KeyCode::Char('l') => {
                self.player.is_playing = false;
                self.player.step_forward(5);
                None
            }
            KeyCode::Char('j') => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+J: Scroll event log down
                    self.scroll_event_log_down(1);
                } else {
                    // Regular j: Step forward 1 tick
                    self.player.is_playing = false;
                    self.player.step_forward(1);
                }
                None
            }
            KeyCode::Char('k') => {
                if key.modifiers.contains(KeyModifiers::SHIFT) {
                    // Shift+K: Scroll event log up
                    self.scroll_event_log_up(1);
                } else {
                    // Regular k: Step backward 1 tick
                    self.player.is_playing = false;
                    self.player.step_backward(1);
                }
                None
            }
            KeyCode::Char('J') => {
                // Uppercase J (Shift+J): Scroll event log down
                self.scroll_event_log_down(1);
                None
            }
            KeyCode::Char('K') => {
                // Uppercase K (Shift+K): Scroll event log up
                self.scroll_event_log_up(1);
                None
            }
            KeyCode::Char('q') | KeyCode::Esc => {
                Some(AppCommand::BackToSelector)
            }
            KeyCode::PageUp => {
                self.scroll_event_log_up(5);
                None
            }
            KeyCode::PageDown => {
                self.scroll_event_log_down(5);
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
                
                // Stop playing if the game is complete
                if matches!(self.player.current_state.status, GameStatus::Complete { .. }) {
                    self.player.is_playing = false;
                    self.playback_accumulator = 0.0;
                    break;
                }
            }
        }
    }
    
    fn render(&self, frame: &mut Frame) {
        let layout_mode = LayoutMode::from_dimensions(frame.area().width, frame.area().height);
        
        match layout_mode {
            LayoutMode::SingleColumn => self.render_single_column(frame),
            LayoutMode::TwoColumn => self.render_two_column(frame),
        }
    }
}

impl ReplayViewerState {
    fn scroll_event_log_up(&mut self, lines: u16) {
        self.event_log_scroll = self.event_log_scroll.saturating_sub(lines);
        let mut scrollbar_state = self.event_log_scrollbar_state.borrow_mut();
        *scrollbar_state = scrollbar_state.position(self.event_log_scroll as usize);
    }
    
    fn scroll_event_log_down(&mut self, lines: u16) {
        // We'll calculate visible height later when we know the actual rendered area
        let visible_height = 10; // Default estimate, will be improved
        let max_scroll = self.event_log_total_lines.get().saturating_sub(visible_height);
        self.event_log_scroll = (self.event_log_scroll + lines).min(max_scroll);
        let mut scrollbar_state = self.event_log_scrollbar_state.borrow_mut();
        *scrollbar_state = scrollbar_state.position(self.event_log_scroll as usize);
    }
    
    fn render_single_column(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Min(20),    // Game arena at top
                Constraint::Length(12), // Event log (taller)
                Constraint::Length(3),  // Header (tick counter)
                Constraint::Length(4),  // Status
                Constraint::Length(4),  // Controls help at bottom (increased for 2 lines)
            ])
            .split(frame.area());
        
        // Game arena at top
        self.render_arena(frame, chunks[0]);
        
        // Event log
        self.render_event_log(frame, chunks[1]);
        
        // Header with game info
        let header = self.render_header();
        frame.render_widget(header, chunks[2]);
        
        // Status info
        let status = self.render_status();
        frame.render_widget(status, chunks[3]);
        
        // Controls help at bottom
        let controls = self.render_controls();
        frame.render_widget(controls, chunks[4]);
    }
    
    fn render_two_column(&self, frame: &mut Frame) {
        // Split into left (arena) and right (everything else) columns
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .margin(1)
            .constraints([
                Constraint::Percentage(40),  // Arena column
                Constraint::Percentage(60),  // Info column
            ])
            .split(frame.area());
        
        // Render arena in left column
        self.render_arena(frame, main_chunks[0]);
        
        // Calculate dynamic constraints for right column
        let available_height = main_chunks[1].height;
        let constraints = self.calculate_info_column_constraints(available_height);
        
        // Split right column for other components
        let info_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(main_chunks[1]);
        
        // Render components in right column
        self.render_event_log(frame, info_chunks[0]);
        frame.render_widget(self.render_header(), info_chunks[1]);
        frame.render_widget(self.render_status(), info_chunks[2]);
        frame.render_widget(self.render_controls(), info_chunks[3]);
    }
    
    fn calculate_info_column_constraints(&self, available_height: u16) -> Vec<Constraint> {
        const HEADER_HEIGHT: u16 = 3;
        const STATUS_HEIGHT: u16 = 4;
        const CONTROLS_HEIGHT: u16 = 4;  // Increased for 2 lines
        const MIN_EVENT_LOG_HEIGHT: u16 = 10;
        
        let fixed_height = HEADER_HEIGHT + STATUS_HEIGHT + CONTROLS_HEIGHT;
        
        if available_height > fixed_height + MIN_EVENT_LOG_HEIGHT {
            // Enough space - event log takes remaining space
            vec![
                Constraint::Min(MIN_EVENT_LOG_HEIGHT),  // Event log expands
                Constraint::Length(HEADER_HEIGHT),      // Header
                Constraint::Length(STATUS_HEIGHT),      // Status
                Constraint::Length(CONTROLS_HEIGHT),    // Controls
            ]
        } else {
            // Limited space - use percentages
            vec![
                Constraint::Percentage(58),  // Event log gets majority (slightly reduced)
                Constraint::Percentage(12),  // Header
                Constraint::Percentage(16),  // Status
                Constraint::Percentage(14),  // Controls (increased)
            ]
        }
    }
    
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
        
        // Create renderer with 2x1 characters per point
        let char_dims = CharDimensions::new(2, 1);
        let renderer = StandardRenderer::new(char_dims);
        let arena_renderer = ArenaRenderer::new(renderer);
        let config = RenderConfig { chars_per_point: char_dims };
        
        // Render the arena to a character grid
        let char_grid = arena_renderer.render(arena, &config);
        
        // Calculate arena dimensions including borders
        let arena_width = (arena.width as usize * char_dims.horizontal) + 2; // +2 for left and right borders
        let arena_height = (arena.height as usize * char_dims.vertical) + 2; // +2 for top and bottom borders
        
        // Calculate centering offsets
        let x_offset = inner.width.saturating_sub(arena_width as u16) / 2;
        let y_offset = inner.height.saturating_sub(arena_height as u16) / 2;
        
        // Create lines with borders and proper positioning
        let mut final_lines: Vec<Line> = Vec::new();
        
        // Add vertical spacing for centering
        for _ in 0..y_offset {
            final_lines.push(Line::from(""));
        }
        
        // Top border
        let mut top_border = " ".repeat(x_offset as usize);
        top_border.push('┌');
        for _ in 0..(arena.width as usize * char_dims.horizontal) {
            top_border.push('─');
        }
        top_border.push('┐');
        final_lines.push(Line::from(vec![
            Span::styled(top_border, Style::default().fg(Color::DarkGray))
        ]));
        
        // Convert grid to styled lines with side borders
        let grid_lines = char_grid.into_styled_lines();
        for (chars, styles) in grid_lines {
            let mut line_spans = Vec::new();
            
            // Left padding and border
            if x_offset > 0 {
                line_spans.push(Span::raw(" ".repeat(x_offset as usize)));
            }
            line_spans.push(Span::styled("│", Style::default().fg(Color::DarkGray)));
            
            // Arena content
            for (ch, style) in chars.into_iter().zip(styles.into_iter()) {
                line_spans.push(Span::styled(ch.to_string(), style));
            }
            
            // Right border
            line_spans.push(Span::styled("│", Style::default().fg(Color::DarkGray)));
            
            final_lines.push(Line::from(line_spans));
        }
        
        // Bottom border
        let mut bottom_border = " ".repeat(x_offset as usize);
        bottom_border.push('└');
        for _ in 0..(arena.width as usize * char_dims.horizontal) {
            bottom_border.push('─');
        }
        bottom_border.push('┘');
        final_lines.push(Line::from(vec![
            Span::styled(bottom_border, Style::default().fg(Color::DarkGray))
        ]));
        
        let game_view = Paragraph::new(final_lines);
        
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
        let lines = vec![
            Line::from("Space: Play/Pause | j/k: ±1 tick | h/l: ±5 ticks | q: Back to menu"),
            Line::from("Shift+J/K: Scroll event log | PageUp/Down: Scroll event log (5 lines)"),
        ];
        
        Paragraph::new(lines)
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
                .title(format!("Raw Event Log ({} events)", events_to_show.len()))
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