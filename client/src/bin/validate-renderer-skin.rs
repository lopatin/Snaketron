//! Validate a SkinDoc through the exact compiler used by the game client.
//!
//! ```text
//! cargo run -p client --bin validate-renderer-skin -- path/to/name.skin.json
//! ```

use std::process::ExitCode;

fn main() -> ExitCode {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: validate-renderer-skin <file.skin.json> [more.skin.json ...]");
        return ExitCode::from(2);
    }

    let mut failed = false;
    for path in &paths {
        let json = match std::fs::read_to_string(path) {
            Ok(json) => json,
            Err(error) => {
                eprintln!("{path}: cannot read: {error}");
                failed = true;
                continue;
            }
        };

        match client::validate_skin_document_for_renderer(&json) {
            Ok(()) => println!("{path}: ok — client renderer compiled"),
            Err(error) => {
                eprintln!("{path}: client renderer refused the document\n{error}");
                failed = true;
            }
        }
    }

    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
