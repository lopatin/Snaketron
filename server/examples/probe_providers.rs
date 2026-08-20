//! Call whichever image providers are configured, once each, and shape the
//! result through the real pixel pass.
//! `cargo run -p server --example probe_providers -- <out-dir> [reference.png]`
use server::generation::ProviderOutcome;
use server::texture::TextureKind;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let out = std::path::PathBuf::from(&args[1]);
    std::fs::create_dir_all(&out)?;
    let references: Vec<Vec<u8>> = args
        .get(2)
        .map(std::fs::read)
        .transpose()?
        .into_iter()
        .collect();

    let kind = TextureKind::Coat;
    let (w, h) = server::texture_pixels::canonical_size(kind, None);
    let prompt = server::texture::build_prompt(
        kind,
        "cracked volcanic basalt with glowing magma veins",
        w,
        h,
        1,
    );
    println!(
        "references: {}\nprompt: {}\n",
        references.len(),
        &prompt[..prompt.len().min(140)]
    );

    for provider in server::generation_providers::configured_providers() {
        let started = std::time::Instant::now();
        match provider.generate(&prompt, w, h, &references).await {
            ProviderOutcome::Image { png, usd_micros } => {
                let pixels =
                    server::texture_pixels::decode(&png).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let shaped = server::texture_pixels::shape(&pixels, kind, None, false)
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                let name = provider.name();
                std::fs::write(
                    out.join(format!("{name}-coat.png")),
                    &shaped.canonical.bytes,
                )?;
                println!(
                    "{name}: OK in {:?}, ${:.3}, {}x{}, seam h={:.4} (gate {}), rungs {}",
                    started.elapsed(),
                    usd_micros as f64 / 1e6,
                    shaped.canonical.width_px,
                    shaped.canonical.height_px,
                    shaped.seams.horizontal_ratio,
                    if shaped.seams.passes(kind) {
                        "pass"
                    } else {
                        "FAIL"
                    },
                    shaped.rungs.len(),
                );
            }
            ProviderOutcome::Refused { reason } => {
                println!("{}: refused — {reason}", provider.name())
            }
            ProviderOutcome::Unavailable { detail } => {
                println!("{}: unavailable — {detail}", provider.name())
            }
        }
    }
    Ok(())
}
