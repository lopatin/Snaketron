pub mod replay_selector;
pub mod replay_viewer;

pub use replay_selector::ReplaySelectorState;
pub use replay_viewer::ReplayViewerState;

use crate::app::AppCommand;
use crossterm::event::KeyEvent;
use ratatui::Frame;
use std::time::Duration;

pub trait View {
    fn handle_input(&mut self, key: KeyEvent) -> Option<AppCommand>;
    fn update(&mut self, dt: Duration);
    fn render(&self, frame: &mut Frame);
}
