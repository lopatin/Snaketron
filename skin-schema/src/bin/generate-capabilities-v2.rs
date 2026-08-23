use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rendered = serde_json::to_string_pretty(&skin_schema::capabilities::capabilities_v2())?;
    rendered.push('\n');
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("capabilities-v2.json");
    if std::env::args().any(|argument| argument == "--check") {
        let committed = std::fs::read_to_string(&path)?;
        if committed != rendered {
            return Err(format!(
                "{} is stale; run `cargo run -p skin-schema --bin generate-capabilities-v2`",
                path.display()
            )
            .into());
        }
    } else {
        std::fs::write(&path, rendered)?;
        println!("wrote {}", path.display());
    }
    Ok(())
}
