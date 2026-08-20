//! Run the texture pixel pass over a real file, for eyeballing the result.
//! `cargo run -p server --example shape_texture -- in.png out-dir coat`
fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let (src, out, kind) = (&args[1], std::path::PathBuf::from(&args[2]), &args[3]);
    let kind = server::texture::TextureKind::parse(kind).expect("a texture kind");
    let bytes = std::fs::read(src)?;
    let pixels = server::texture_pixels::decode(&bytes).map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let shaped = server::texture_pixels::shape(&pixels, kind, Some(20), false)
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    std::fs::create_dir_all(&out)?;
    let stem = std::path::Path::new(src)
        .file_stem()
        .unwrap()
        .to_string_lossy();
    std::fs::write(
        out.join(format!("{stem}-canonical.png")),
        &shaped.canonical.bytes,
    )?;
    println!(
        "{stem}: canonical {}x{}  seam h={:.4} v={:.4}  rungs {:?}",
        shaped.canonical.width_px,
        shaped.canonical.height_px,
        shaped.seams.horizontal_ratio,
        shaped.seams.vertical_ratio,
        shaped
            .rungs
            .iter()
            .map(|r| (r.texels_per_cell, r.width_px, r.height_px))
            .collect::<Vec<_>>()
    );
    Ok(())
}
