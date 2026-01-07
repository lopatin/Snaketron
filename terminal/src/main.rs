use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    Terminal,
};
use std::io;
use std::path::PathBuf;
use std::time::Duration;

mod app;
mod render;
mod replay;
mod views;

use app::{App, AppCommand};

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Removed debug code

    // Get replay directory from args or use centralized default
    let replay_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/snaketron_replays"));

    println!("Looking for replays in: {:?}", replay_dir);

    // Check if directory exists
    if !replay_dir.exists() {
        eprintln!("Replay directory does not exist: {:?}", replay_dir);
        eprintln!("Creating directory...");
        std::fs::create_dir_all(&replay_dir)?;
    }

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app
    let mut app = App::new(replay_dir)?;

    // Run app
    let res = run_app(&mut terminal, &mut app);

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        eprintln!("Error: {:?}", err);
    }

    Ok(())
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    let mut last_update = std::time::Instant::now();

    loop {
        // Calculate delta time
        let now = std::time::Instant::now();
        let dt = now.duration_since(last_update);
        last_update = now;

        // Update app state
        app.update(dt);

        // Draw
        terminal.draw(|f| app.render(f))?;

        // Handle input
        if event::poll(Duration::from_millis(16))? {
            if let Event::Key(key) = event::read()? {
                if let Some(command) = app.handle_input(key) {
                    match command {
                        AppCommand::Quit => return Ok(()),
                        _ => app.handle_command(command)?,
                    }
                }
            }
        }
    }
}
