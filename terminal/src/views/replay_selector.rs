use super::View;
use crate::app::AppCommand;
use crate::replay::reader::ReplayReader;
use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};
use std::path::PathBuf;
use std::time::Duration;

pub struct ReplaySelectorState {
    replay_files: Vec<PathBuf>,
    selected_index: usize,
    scroll_offset: usize,
}

impl ReplaySelectorState {
    pub fn new(replay_dir: PathBuf) -> Result<Self> {
        let replay_files = ReplayReader::list_replays(&replay_dir)?;
        Ok(Self {
            replay_files,
            selected_index: 0,
            scroll_offset: 0,
        })
    }
    
    fn move_selection_up(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
            
            // Adjust scroll if needed
            if self.selected_index < self.scroll_offset {
                self.scroll_offset = self.selected_index;
            }
        }
    }
    
    fn move_selection_down(&mut self) {
        if self.selected_index < self.replay_files.len().saturating_sub(1) {
            self.selected_index += 1;
        }
    }
    
    fn update_scroll(&mut self) {
        // This will be called after selection changes to update scroll
        // The actual adjustment happens in render based on visible height
    }
}

impl View for ReplaySelectorState {
    fn handle_input(&mut self, key: KeyEvent) -> Option<AppCommand> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => Some(AppCommand::Quit),
            KeyCode::Char('j') | KeyCode::Down => {
                self.move_selection_down();
                self.update_scroll();
                None
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.move_selection_up();
                self.update_scroll();
                None
            }
            KeyCode::Enter => {
                if self.selected_index < self.replay_files.len() {
                    let path = self.replay_files[self.selected_index].clone();
                    Some(AppCommand::OpenReplay(path))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    fn update(&mut self, _dt: Duration) {
        // No time-based updates needed for selector
    }
    
    fn render(&self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(3),
            ])
            .split(frame.area());
        
        // Title
        let title = Paragraph::new("SnakeTron Replay Viewer")
            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL));
        frame.render_widget(title, chunks[0]);
        
        // File list
        let list_area = chunks[1];
        let visible_height = list_area.height.saturating_sub(2) as usize; // Account for borders
        
        // Calculate scroll offset based on current selection
        let scroll_offset = if self.selected_index >= self.scroll_offset + visible_height {
            self.selected_index.saturating_sub(visible_height - 1)
        } else if self.selected_index < self.scroll_offset {
            self.selected_index
        } else {
            self.scroll_offset
        };
        
        let items: Vec<ListItem> = self.replay_files
            .iter()
            .enumerate()
            .skip(scroll_offset)
            .take(visible_height)
            .map(|(i, path)| {
                let filename = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("Unknown");
                
                // Get file size and modification time
                let metadata_info = if let Ok(metadata) = path.metadata() {
                    let size = metadata.len();
                    let size_str = if size < 1024 {
                        format!("{} B", size)
                    } else if size < 1024 * 1024 {
                        format!("{:.1} KB", size as f64 / 1024.0)
                    } else {
                        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
                    };
                    
                    let modified = metadata.modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| {
                            let secs = d.as_secs();
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            let age = now - secs;
                            if age < 60 {
                                "just now".to_string()
                            } else if age < 3600 {
                                format!("{}m ago", age / 60)
                            } else if age < 86400 {
                                format!("{}h ago", age / 3600)
                            } else {
                                format!("{}d ago", age / 86400)
                            }
                        })
                        .unwrap_or_else(|| "unknown".to_string());
                    
                    format!(" ({}, {})", size_str, modified)
                } else {
                    String::new()
                };
                
                let style = if i == self.selected_index {
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{:>3} ", i + 1), Style::default().fg(Color::DarkGray)),
                    Span::styled(filename, style),
                    Span::styled(metadata_info, Style::default().fg(Color::DarkGray)),
                ]))
            })
            .collect();
        
        let list = List::new(items)
            .block(Block::default()
                .title("Select Replay")
                .borders(Borders::ALL))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD));
        
        frame.render_widget(list, list_area);
        
        // Help text
        let help_text = if self.replay_files.is_empty() {
            "No replay files found. Press 'q' to quit."
        } else {
            "↑/k: Up | ↓/j: Down | Enter: Open | q: Quit"
        };
        
        let help = Paragraph::new(help_text)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL));
        frame.render_widget(help, chunks[2]);
    }
}