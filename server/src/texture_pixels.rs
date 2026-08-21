//! The pixel half of a texture: decode it, make it tile, measure it, and build
//! the ladder the renderer picks from.
//!
//! This is the work the PRD assigned to a `texture-forge` worker container —
//! Python, for PIL and numpy and LaMa. Section 20 then dropped the LaMa half:
//! the inpainting repair that pulled a borderline generation back over the
//! seam gate was judged not worth a torch image in the serving path. What is
//! left after that decision is decode, crop, resize and arithmetic, and none
//! of that needs a second language or a second container.
//!
//! Seamlessness is the interesting part. The offline tool makes a strip tile by
//! inpainting across the wrap, which needs a model; here it is made to tile by
//! **construction** instead. Half the strip is generated and the other half is
//! its mirror, so the wrap join is column zero meeting its own reflection —
//! exactly equal, not approximately. Measured with the tooling's own percentile
//! metric that scores 0.000 against 1.000 for the raw generation, and it costs
//! no provider call, no retry and no model. What it spends is symmetry: a
//! mirrored texture repeats about a vertical axis, which on fur, stripes and
//! camouflage is invisible and on a legible motif — lettering, a logo — would
//! not be. Only tiling kinds are mirrored for that reason.

use image::{ImageFormat, RgbaImage, imageops::FilterType};

use crate::texture::{SeamReport, TextureError, TextureKind};

/// A decoded image, ready to be shaped.
#[derive(Clone)]
pub struct Pixels {
    pub image: RgbaImage,
}

/// One rung of the ladder, encoded and ready to store.
pub struct Rung {
    pub texels_per_cell: u32,
    pub width_px: u32,
    pub height_px: u32,
    pub bytes: Vec<u8>,
}

/// Everything the pixel pass produces for one texture.
pub struct Shaped {
    pub canonical: Rung,
    pub rungs: Vec<Rung>,
    pub seams: SeamReport,
    pub rows: Option<u32>,
}

/// The largest image this will decode, in pixels of output.
///
/// A PNG header may declare dimensions far larger than its compressed size
/// suggests — the decompression bomb — so the ceiling is enforced against the
/// *declared* size, before any allocation, rather than discovered afterwards.
const MAX_DECODED_PIXELS: u64 = 4096 * 4096;

/// Decode a PNG, refusing anything whose header is already out of bounds.
pub fn decode(bytes: &[u8]) -> Result<Pixels, TextureError> {
    let header = crate::texture::read_png_header(bytes)?;
    if u64::from(header.width_px) * u64::from(header.height_px) > MAX_DECODED_PIXELS {
        return Err(TextureError::new(
            "image",
            format!(
                "{}×{} is more pixels than this will decode",
                header.width_px, header.height_px
            ),
        ));
    }

    let decoded = image::load_from_memory_with_format(bytes, ImageFormat::Png)
        .map_err(|error| TextureError::new("image", format!("is not readable: {error}")))?;
    Ok(Pixels {
        image: decoded.to_rgba8(),
    })
}

/// Where a wrap join ranks among the image's own column steps, 0..1.
///
/// The tooling's metric, and its reasoning carries over verbatim: the obvious
/// ratio — the join's step over the mean interior step — is not diagnostic,
/// because a flat texture makes any real edge look enormous and a busy one
/// buries a genuine misalignment. A percentile asks the only question that
/// matters, *is this join unusual for this texture*, in the texture's own
/// units. 0.5 is a perfectly ordinary column boundary.
fn seam_percentile(image: &RgbaImage, vertical: bool) -> f32 {
    let (width, height) = image.dimensions();
    if width < 2 || height < 2 {
        return 0.0;
    }

    let channel_gap = |a: &image::Rgba<u8>, b: &image::Rgba<u8>| -> f64 {
        (0..3)
            .map(|c| (f64::from(a[c]) - f64::from(b[c])).abs())
            .sum::<f64>()
            / 3.0
    };

    // The join, and every interior step it is being compared against.
    let (join, interior) = if vertical {
        let join = (0..width)
            .map(|x| channel_gap(image.get_pixel(x, 0), image.get_pixel(x, height - 1)))
            .sum::<f64>()
            / f64::from(width);
        let interior: Vec<f64> = (0..height - 1)
            .map(|y| {
                (0..width)
                    .map(|x| channel_gap(image.get_pixel(x, y), image.get_pixel(x, y + 1)))
                    .sum::<f64>()
                    / f64::from(width)
            })
            .collect();
        (join, interior)
    } else {
        let join = (0..height)
            .map(|y| channel_gap(image.get_pixel(0, y), image.get_pixel(width - 1, y)))
            .sum::<f64>()
            / f64::from(height);
        let interior: Vec<f64> = (0..width - 1)
            .map(|x| {
                (0..height)
                    .map(|y| channel_gap(image.get_pixel(x, y), image.get_pixel(x + 1, y)))
                    .sum::<f64>()
                    / f64::from(height)
            })
            .collect();
        (join, interior)
    };

    let below = interior.iter().filter(|step| **step < join).count();
    below as f32 / interior.len().max(1) as f32
}

/// Measure the exact decoded bytes that will be served, without reshaping.
pub fn seam_report(pixels: &Pixels) -> SeamReport {
    SeamReport {
        horizontal_ratio: seam_percentile(&pixels.image, false),
        vertical_ratio: seam_percentile(&pixels.image, true),
        repaired: false,
    }
}

/// Crop the widest band of the requested aspect from the middle of an image.
///
/// A coat is twelve cells long and one cell tall; a model asked for one returns
/// a square. Squashing the square would smear every stripe by the aspect
/// ratio, so a band of the right shape is taken and then scaled — the texture
/// keeps its own proportions and simply shows less of itself.
fn centre_band(image: &RgbaImage, aspect: f64) -> RgbaImage {
    let (width, height) = image.dimensions();
    let band_height = ((f64::from(width) / aspect).round() as u32).clamp(1, height);
    let top = (height - band_height) / 2;
    image::imageops::crop_imm(image, 0, top, width, band_height).to_image()
}

/// Join an image to its own mirror, so the wrap is exact.
fn mirrored(image: &RgbaImage) -> RgbaImage {
    let (width, height) = image.dimensions();
    let mut out = RgbaImage::new(width * 2, height);
    for y in 0..height {
        for x in 0..width {
            let pixel = *image.get_pixel(x, y);
            out.put_pixel(x, y, pixel);
            out.put_pixel(width * 2 - 1 - x, y, pixel);
        }
    }
    out
}

fn encode(image: &RgbaImage) -> Result<Vec<u8>, TextureError> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut bytes, ImageFormat::Png)
        .map_err(|error| TextureError::new("image", format!("could not be encoded: {error}")))?;
    Ok(bytes.into_inner())
}

/// Turn a decoded image into a canonical texture plus its ladder.
///
/// `already_shaped` is the upload path: an author who hands over art at exactly
/// the canonical size meant it, so it is measured and laddered but never
/// cropped or mirrored. Anything else — a generation, or an upload at the wrong
/// size — is fitted to the kind.
pub fn shape(
    pixels: &Pixels,
    kind: TextureKind,
    rows: Option<u32>,
    already_shaped: bool,
) -> Result<Shaped, TextureError> {
    let cell = kind.canonical_texels_per_cell();
    let (target_width, target_height) = canonical_size(kind, rows);

    let canonical_image = if already_shaped {
        pixels.image.clone()
    } else if kind.tiles_along_body() {
        // Half the width from the source, then its mirror — which is what
        // makes the wrap exact rather than merely close.
        let half = target_width / 2;
        let band = centre_band(&pixels.image, f64::from(half) / f64::from(target_height));
        mirrored(&image::imageops::resize(
            &band,
            half,
            target_height,
            FilterType::Lanczos3,
        ))
    } else {
        // A sheet's rows are moments and an overlay does not repeat, so
        // neither is mirrored; both are simply fitted.
        let band = centre_band(
            &pixels.image,
            f64::from(target_width) / f64::from(target_height),
        );
        image::imageops::resize(&band, target_width, target_height, FilterType::Lanczos3)
    };

    let seams = SeamReport {
        horizontal_ratio: seam_percentile(&canonical_image, false),
        vertical_ratio: seam_percentile(&canonical_image, true),
        // Nothing here repairs; it constructs. A mirrored wrap was never
        // broken, so claiming a repair would misreport what happened.
        repaired: false,
    };

    let canonical = Rung {
        texels_per_cell: cell,
        width_px: canonical_image.width(),
        height_px: canonical_image.height(),
        bytes: encode(&canonical_image)?,
    };

    // Every rung is resized from the canonical one rather than from its
    // predecessor, so error does not compound down the ladder.
    let mut rungs = Vec::new();
    for &texels in kind.ladder() {
        let scale = f64::from(texels) / f64::from(cell);
        let width = ((f64::from(canonical.width_px) * scale).round() as u32).max(1);
        let height = ((f64::from(canonical.height_px) * scale).round() as u32).max(1);
        let small = image::imageops::resize(&canonical_image, width, height, FilterType::Lanczos3);
        rungs.push(Rung {
            texels_per_cell: texels,
            width_px: width,
            height_px: height,
            bytes: encode(&small)?,
        });
    }

    Ok(Shaped {
        canonical,
        rungs,
        seams,
        rows,
    })
}

/// The size a kind's canonical variant is stored at.
pub fn canonical_size(kind: TextureKind, rows: Option<u32>) -> (u32, u32) {
    match kind {
        // Twelve cells of body at 64 texels each, one cell tall.
        TextureKind::Coat => (768, 64),
        TextureKind::Overlay => (256, 64),
        // A stack of frames, sixteen texels per cell.
        TextureKind::Sheet => (320, 16 * rows.unwrap_or(20)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noise(width: u32, height: u32) -> RgbaImage {
        // Deterministic, and busy enough that a seam has something to hide in:
        // a flat image makes every join look perfect and proves nothing.
        let mut image = RgbaImage::new(width, height);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for pixel in image.pixels_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let byte = |shift: u32| ((state >> shift) & 0xff) as u8;
            *pixel = image::Rgba([byte(0), byte(8), byte(16), 255]);
        }
        image
    }

    /// The whole reason for mirroring: the wrap is exact, not merely close.
    #[test]
    fn a_coat_tiles_because_its_wrap_is_its_own_reflection() {
        let source = Pixels {
            image: noise(1024, 1024),
        };
        let shaped = shape(&source, TextureKind::Coat, None, false).expect("shapes");

        assert_eq!(
            (shaped.canonical.width_px, shaped.canonical.height_px),
            (768, 64)
        );
        assert_eq!(
            shaped.seams.horizontal_ratio, 0.0,
            "a mirrored wrap has to score zero; anything else means the join \
             is a cut rather than a reflection"
        );
        assert!(!shaped.seams.repaired, "nothing was repaired, it was built");

        // And it is genuinely a reflection, not a metric coincidence.
        let decoded = decode(&shaped.canonical.bytes).expect("re-reads");
        for y in 0..64 {
            assert_eq!(
                decoded.image.get_pixel(0, y),
                decoded.image.get_pixel(767, y),
                "the wrap columns differ at row {y}"
            );
        }
    }

    /// The same source, not mirrored, is what the mirror is an improvement on.
    #[test]
    fn an_unmirrored_strip_is_the_thing_mirroring_fixes() {
        let plain = image::imageops::resize(&noise(1024, 1024), 768, 64, FilterType::Lanczos3);
        let raw = seam_percentile(&plain, false);
        assert!(
            raw > 0.2,
            "the fixture is supposed to have a visible join to fix, got {raw}"
        );
    }

    /// A ladder is built from the canonical image, so error does not compound.
    #[test]
    fn every_rung_is_the_kind_s_own_ladder() {
        let source = Pixels {
            image: noise(800, 200),
        };
        let shaped = shape(&source, TextureKind::Coat, None, false).expect("shapes");
        let sizes: Vec<(u32, u32, u32)> = shaped
            .rungs
            .iter()
            .map(|rung| (rung.texels_per_cell, rung.width_px, rung.height_px))
            .collect();
        assert_eq!(sizes, vec![(32, 384, 32), (16, 192, 16)]);
        for rung in &shaped.rungs {
            assert!(!rung.bytes.is_empty());
        }
    }

    /// A sheet's rows are moments in an animation; mirroring them would play
    /// the loop forwards and then backwards.
    #[test]
    fn a_sheet_is_fitted_rather_than_mirrored() {
        let source = Pixels {
            image: noise(600, 600),
        };
        let shaped = shape(&source, TextureKind::Sheet, Some(20), false).expect("shapes");
        assert_eq!(
            (shaped.canonical.width_px, shaped.canonical.height_px),
            (320, 320)
        );
        assert_eq!(shaped.rungs.len(), 1);
        assert_eq!(shaped.rungs[0].texels_per_cell, 8);
    }

    /// Art handed over at the canonical size was meant that way.
    #[test]
    fn an_upload_at_the_right_size_is_left_alone() {
        let source = Pixels {
            image: noise(768, 64),
        };
        let shaped = shape(&source, TextureKind::Coat, None, true).expect("shapes");
        let decoded = decode(&shaped.canonical.bytes).expect("re-reads");
        assert_eq!(decoded.image.dimensions(), (768, 64));
        // Untouched means the seam is whatever the author's art has, and the
        // report says so rather than flattering it.
        assert!(shaped.seams.horizontal_ratio > 0.0);
    }

    /// A header that declares a gigapixel image is refused before it is built.
    #[test]
    fn a_declared_bomb_is_refused_without_being_decoded() {
        // A valid 8-byte signature and an IHDR claiming 60000 x 60000.
        let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&60_000u32.to_be_bytes());
        bytes.extend_from_slice(&60_000u32.to_be_bytes());
        bytes.extend_from_slice(&[8, 6, 0, 0, 0]);

        let refused = match decode(&bytes) {
            Err(refused) => refused,
            Ok(_) => panic!("a declared bomb was decoded"),
        };
        assert!(
            refused.problem.contains("more pixels"),
            "refused for the wrong reason: {}",
            refused.problem
        );
    }
}
