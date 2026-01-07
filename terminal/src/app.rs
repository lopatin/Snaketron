use anyhow::Result;
use crossterm::event::KeyEvent;
use ratatui::Frame;
use std::path::PathBuf;
use std::time::Duration;

use crate::replay::reader::ReplayReader;
use crate::views::{ReplaySelectorState, ReplayViewerState, View};

#[derive(Debug)]
pub enum AppCommand {
    Quit,
    BackToSelector,
    OpenReplay(PathBuf),
}

pub enum AppState {
    ReplaySelector(Box<ReplaySelectorState>),
    ReplayViewer(Box<ReplayViewerState>),
}

pub struct App {
    pub state: AppState,
    pub replay_dir: PathBuf,
}

impl App {
    pub fn new(replay_dir: PathBuf) -> Result<Self> {
        let selector = ReplaySelectorState::new(replay_dir.clone())?;
        Ok(Self {
            state: AppState::ReplaySelector(Box::new(selector)),
            replay_dir,
        })
    }

    pub fn handle_input(&mut self, key: KeyEvent) -> Option<AppCommand> {
        match &mut self.state {
            AppState::ReplaySelector(selector) => selector.handle_input(key),
            AppState::ReplayViewer(viewer) => viewer.handle_input(key),
        }
    }

    pub fn update(&mut self, dt: Duration) {
        match &mut self.state {
            AppState::ReplaySelector(selector) => selector.update(dt),
            AppState::ReplayViewer(viewer) => viewer.update(dt),
        }
    }

    pub fn render(&self, frame: &mut Frame) {
        match &self.state {
            AppState::ReplaySelector(selector) => selector.render(frame),
            AppState::ReplayViewer(viewer) => viewer.render(frame),
        }
    }

    pub fn handle_command(&mut self, command: AppCommand) -> Result<()> {
        match command {
            AppCommand::OpenReplay(path) => {
                // Load the replay
                let replay_data = ReplayReader::load_replay(&path)?;
                let viewer = ReplayViewerState::new(replay_data);
                self.state = AppState::ReplayViewer(Box::new(viewer));
            }
            AppCommand::BackToSelector => {
                let selector = ReplaySelectorState::new(self.replay_dir.clone())?;
                self.state = AppState::ReplaySelector(Box::new(selector));
            }
            AppCommand::Quit => {
                // Handled in main loop
            }
        }
        Ok(())
    }
}
