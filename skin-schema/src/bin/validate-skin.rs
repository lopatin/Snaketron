//! Validate a skin document.
//!
//! ```text
//! cargo run -p skin-schema --bin validate-skin -- path/to/name.skin.json
//! ```
//!
//! Deliberately a binary rather than a test: a test filter that matches nothing
//! exits successfully, which would let a typo'd filename look like a pass.

use std::process::ExitCode;

fn main() -> ExitCode {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: validate-skin <file.skin.json> [more.skin.json ...]");
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

        match skin_schema::load(&json) {
            Ok(doc) => println!("{path}: ok — {} ({})", doc.name, doc.id),
            Err(errors) => {
                failed = true;
                eprintln!("{path}: {} problem(s)", errors.len());
                for error in errors {
                    eprintln!("  - {error}");
                }
            }
        }
    }

    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
