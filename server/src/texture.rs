//! Textures: what one is, what shape it has to be, and how to ask a model for
//! one.
//!
//! A texture is one logical image stored as a small ladder of resolutions. The
//! conventions here are not invented — they are the ones
//! `client/design/tools/` already builds to and `client/src/skin/` already
//! reads, restated where a server can enforce them:
//!
//! - a **coat** is one repeat of a static pattern, `x` down the body from the
//!   head, `y` across it, one cell tall at 64 texels, and opaque;
//! - a **sheet** is animation: `y` is time, `x` is the body, one cell is
//!   16×16 texels and that is a ceiling rather than a preference, and the
//!   height must divide by the row count or every frame samples across a
//!   boundary;
//! - an **overlay** is a short run of cells on transparency, head-anchored,
//!   with no tiling obligation at all.
//!
//! Getting these wrong is not a rendering bug, it is a *silent* rendering bug:
//! a sheet whose height does not divide by its rows still draws, just with
//! every frame smeared into the next.

use serde::{Deserialize, Serialize};

/// A wrap join an exact PNG ladder has been measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum SeamAxis {
    X,
    Y,
}

impl SeamAxis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::X => "x",
            Self::Y => "y",
        }
    }
}

/// What a texture is for, which decides its shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum TextureKind {
    /// A static coat that tiles along the body.
    Coat,
    /// An animated sheet whose rows are frames.
    Sheet,
    /// A short head-anchored run on transparency.
    Overlay,
}

impl TextureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Coat => "coat",
            Self::Sheet => "sheet",
            Self::Overlay => "overlay",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "coat" => Some(Self::Coat),
            "sheet" => Some(Self::Sheet),
            "overlay" => Some(Self::Overlay),
            _ => None,
        }
    }

    /// Texels per cell in the canonical variant.
    ///
    /// Coats and overlays are authored at 64 so they stay supersampled at every
    /// arena cell size; sheets are pinned at 16, which is the pixel-art floor
    /// the sprite tooling treats as a hard ceiling rather than a default.
    pub fn canonical_texels_per_cell(self) -> u32 {
        match self {
            Self::Coat | Self::Overlay => 64,
            Self::Sheet => 16,
        }
    }

    /// The rungs below the canonical one, largest first.
    ///
    /// A sheet gets one rung because halving 16 is already below the pixel-art
    /// floor; going further would be inventing detail loss the artist never
    /// approved.
    pub fn ladder(self) -> &'static [u32] {
        match self {
            Self::Coat | Self::Overlay => &[32, 16],
            Self::Sheet => &[8],
        }
    }

    /// Whether the image must tile along the body axis.
    pub fn tiles_along_body(self) -> bool {
        matches!(self, Self::Coat)
    }

    /// Whether the image must tile in time (row `n-1` back to row `0`).
    pub fn tiles_in_time(self) -> bool {
        matches!(self, Self::Sheet)
    }

    /// Axes the legacy interactive worker guarantees without knowing the
    /// eventual document fit. Strict forge manifests carry the use-derived
    /// set explicitly instead.
    pub fn worker_seam_axes(self) -> &'static [SeamAxis] {
        match self {
            Self::Coat => &[SeamAxis::X],
            Self::Sheet => &[SeamAxis::Y],
            Self::Overlay => &[],
        }
    }
}

/// The largest image worth accepting, in either axis.
pub const MAX_CANONICAL_DIMENSION: u32 = 2048;

/// The largest single variant, in bytes.
pub const MAX_VARIANT_BYTES: usize = 2 * 1024 * 1024;

/// The measured renderer/decoded-memory ceiling for animation frames.
///
/// Twenty remains the interactive generator's economical default, not a
/// capability limit. The v2 capability manifest exposes 120 so deliberately
/// tall factory sheets can trade bytes for smoother slow-motion playback.
pub const MAX_SHEET_ROWS: u32 = skin_schema::v2::MAX_SPRITE_FRAME_ROWS;

/// One rung of the ladder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct TextureVariant {
    pub texels_per_cell: u32,
    pub width_px: u32,
    pub height_px: u32,
    pub bytes: u32,
    /// The hash of *these* bytes.
    ///
    /// Each rung is addressed by its own hash rather than by the texture's, so
    /// replacing one rung with hand-simplified art mints a new URL instead of
    /// changing what an immutable one serves.
    pub sha256: String,
}

/// What a seam check found, per axis.
///
/// The numbers are **percentiles**, not ratios, and the difference is not
/// cosmetic. A ratio — the join's step over the mean interior step — was tried
/// first and an adversarial review of the offline tooling established it is not
/// diagnostic in either direction: a posterised print is mostly flat, so its
/// mean interior step is tiny and one legitimate stripe edge landing on the
/// join reads as 1.95; a high-contrast hide has a mean large enough to bury a
/// real misalignment. Both were measured on shipped files
/// (`build_coat_textures.py::seam_ratio`).
///
/// A percentile asks the only question that matters — *is this join unusual for
/// this texture?* — and answers it in the texture's own units. 0.5 is a
/// perfectly ordinary column boundary.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SeamReport {
    /// Where the wrap join ranks among the image's own column steps, 0..1.
    pub horizontal_ratio: f32,
    /// The same for the join between the last row and the first.
    pub vertical_ratio: f32,
    /// Whether a repair was applied to get here.
    pub repaired: bool,
}

impl SeamReport {
    /// Where a join stops being an ordinary boundary and starts being a line.
    ///
    /// The offline tooling's own reading of the same metric: "anything above
    /// ~0.9 is a join that stands out from its own texture and will be visible
    /// when it repeats". A little slack under that, because a join is allowed
    /// to be *detectable* by a metric as long as it is not visible, and holding
    /// art to a stricter number rejects work that looks right.
    ///
    /// This was 1.5 while the doc above described a ratio — a threshold no
    /// percentile could ever reach, so the gate accepted every texture put to
    /// it. Nothing had been put to it yet, which is why it went unnoticed.
    pub const ACCEPTABLE_RATIO: f32 = 0.9;

    pub fn passes(&self, kind: TextureKind) -> bool {
        self.passes_axes(kind.worker_seam_axes())
    }

    pub fn passes_axes(&self, axes: &[SeamAxis]) -> bool {
        axes.iter().all(|axis| match axis {
            SeamAxis::X => self.horizontal_ratio <= Self::ACCEPTABLE_RATIO,
            SeamAxis::Y => self.vertical_ratio <= Self::ACCEPTABLE_RATIO,
        })
    }
}

/// One stored texture.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Texture {
    pub texture_id: i32,
    pub owner_user_id: i32,
    /// Whether another author may pin this immutable descriptor in a skin.
    /// Runtime bytes are public by hash either way; this flag controls reuse
    /// at save time, not pixel delivery.
    #[serde(default)]
    pub shareable: bool,
    /// The hash of the canonical variant, and the name the document uses.
    pub content_ref: String,
    pub kind: TextureKind,
    pub width_px: u32,
    pub height_px: u32,
    /// How many cells of body one repeat spans. Absent for an overlay, which
    /// does not repeat.
    pub repeat_cells: Option<f32>,
    /// Frame count, for a sheet.
    pub rows: Option<u32>,
    pub seams: SeamReport,
    /// Axes measured on every exact ladder rung before this metadata row was
    /// made reachable. Save-time document validation requires this set to be
    /// a superset of the axes implied by image fit and animation use.
    #[serde(default)]
    pub verified_seam_axes: Vec<SeamAxis>,
    /// Kept so regenerating is one edit rather than a retype.
    pub last_prompt: Option<String>,
    pub variants: Vec<TextureVariant>,
    pub created_at_ms: i64,
}

/// Public, immutable metadata needed to compile an image source.
///
/// This is intentionally not [`Texture`]: ownership and generation prompts
/// are author-library data, not inputs to rendering, and must never leak from
/// the anonymous manifest route.
impl Texture {
    pub fn descriptor(&self) -> skin_schema::v2::TextureDescriptorV2 {
        let body_columns = self.repeat_cells.and_then(|value| {
            let rounded = value.round();
            (value.is_finite() && value > 0.0 && (value - rounded).abs() < f32::EPSILON)
                .then_some(rounded as u32)
        });
        skin_schema::v2::TextureDescriptorV2 {
            kind: match self.kind {
                TextureKind::Coat => skin_schema::v2::TextureKindV2::Coat,
                TextureKind::Sheet => skin_schema::v2::TextureKindV2::Sheet,
                TextureKind::Overlay => skin_schema::v2::TextureKindV2::Overlay,
            },
            body_columns,
            frame_rows: self.rows,
            variants: self
                .variants
                .iter()
                .map(|variant| {
                    let content_ref = if variant.sha256.starts_with("sha256:") {
                        variant.sha256.clone()
                    } else {
                        format!("sha256:{}", variant.sha256)
                    };
                    skin_schema::v2::TextureVariantV2 {
                        url: format!("/api/textures/variants/{content_ref}.png"),
                        content_ref,
                        width_px: variant.width_px,
                        height_px: variant.height_px,
                        bytes: variant.bytes,
                        texels_per_cell: variant.texels_per_cell,
                    }
                })
                .collect(),
        }
    }
}

/// Why a proposed texture was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextureError {
    pub field: String,
    pub problem: String,
}

impl TextureError {
    pub fn new(field: impl Into<String>, problem: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            problem: problem.into(),
        }
    }
}

/// What an uploaded or generated image claims to be, before it is believed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProposedTexture {
    pub kind: TextureKind,
    pub width_px: u32,
    pub height_px: u32,
    pub rows: Option<u32>,
    pub byte_len: usize,
}

/// Check an image's shape against its kind's conventions.
///
/// Runs before any decode, from the PNG header alone, because the point is to
/// refuse a 20,000-pixel-square image *without* allocating it.
pub fn validate_shape(proposed: ProposedTexture) -> Result<(), Vec<TextureError>> {
    let mut errors = Vec::new();

    if proposed.width_px == 0 || proposed.height_px == 0 {
        errors.push(TextureError::new("image", "has no extent"));
        return Err(errors);
    }
    if proposed.width_px > MAX_CANONICAL_DIMENSION || proposed.height_px > MAX_CANONICAL_DIMENSION {
        errors.push(TextureError::new(
            "image",
            format!(
                "{}×{} exceeds the {MAX_CANONICAL_DIMENSION}px limit",
                proposed.width_px, proposed.height_px
            ),
        ));
    }
    if proposed.byte_len > MAX_VARIANT_BYTES {
        errors.push(TextureError::new(
            "image",
            format!(
                "{} bytes exceeds the {MAX_VARIANT_BYTES}-byte limit",
                proposed.byte_len
            ),
        ));
    }

    let cell = proposed.kind.canonical_texels_per_cell();

    match proposed.kind {
        TextureKind::Coat | TextureKind::Overlay => {
            // The body axis is measured in whole cells, so a width that is not
            // a multiple of the cell size means the last cell is a fraction of
            // a pattern — which reads as a stutter every repeat.
            if !proposed.width_px.is_multiple_of(cell) {
                errors.push(TextureError::new(
                    "width",
                    format!(
                        "{}px is not a whole number of {cell}px cells",
                        proposed.width_px
                    ),
                ));
            }
            if proposed.kind == TextureKind::Coat && proposed.height_px != cell {
                errors.push(TextureError::new(
                    "height",
                    format!(
                        "a coat is exactly one cell tall; {}px is not {cell}px",
                        proposed.height_px
                    ),
                ));
            }
            if proposed.rows.is_some() {
                errors.push(TextureError::new("rows", "only a sheet has frames"));
            }
        }
        TextureKind::Sheet => {
            let Some(rows) = proposed.rows else {
                errors.push(TextureError::new(
                    "rows",
                    "a sheet must say how many frames it has",
                ));
                return Err(errors);
            };
            if rows == 0 || rows > MAX_SHEET_ROWS {
                errors.push(TextureError::new(
                    "rows",
                    format!("must be between 1 and {MAX_SHEET_ROWS}"),
                ));
            } else if !proposed.height_px.is_multiple_of(rows) {
                // The failure this prevents is invisible rather than loud: the
                // sheet still draws, with every frame sampling across the
                // boundary into the next one.
                errors.push(TextureError::new(
                    "height",
                    format!(
                        "{}px does not divide by {rows} frames, so every frame would \
                         sample across a boundary",
                        proposed.height_px
                    ),
                ));
            }
            if !proposed.width_px.is_multiple_of(cell) {
                errors.push(TextureError::new(
                    "width",
                    format!(
                        "{}px is not a whole number of {cell}px cells",
                        proposed.width_px
                    ),
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// What a PNG header says the image is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PngHeader {
    pub width_px: u32,
    pub height_px: u32,
}

/// Read a PNG's dimensions from its header, without decoding it.
///
/// This is the gate that makes the size limits meaningful. A PNG's IHDR
/// declares its dimensions in the first 24 bytes, and a 4 MB upload can
/// declare 60,000 × 60,000 — which is fourteen gigabytes once a decoder
/// believes it. Checking the header first means the limit is enforced against
/// a number rather than against an allocation.
pub fn read_png_header(bytes: &[u8]) -> Result<PngHeader, TextureError> {
    const SIGNATURE: [u8; 8] = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];

    if bytes.len() < 24 {
        return Err(TextureError::new("image", "is too short to be a PNG"));
    }
    if bytes[..8] != SIGNATURE {
        return Err(TextureError::new(
            "image",
            "is not a PNG — only PNG is accepted, and the magic bytes decide, not the filename",
        ));
    }
    // Bytes 8..12 are the IHDR length, 12..16 the chunk type.
    if &bytes[12..16] != b"IHDR" {
        return Err(TextureError::new("image", "has no IHDR where one must be"));
    }

    let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
    if width == 0 || height == 0 {
        return Err(TextureError::new("image", "declares no extent"));
    }
    Ok(PngHeader {
        width_px: width,
        height_px: height,
    })
}

/// How many cells of body one repeat of this image spans.
pub fn repeat_cells(kind: TextureKind, width_px: u32) -> Option<f32> {
    if kind == TextureKind::Overlay {
        return None;
    }
    Some(width_px as f32 / kind.canonical_texels_per_cell() as f32)
}

/// Build the prompt for one texture.
///
/// The constraints are stated to the model rather than hoped for, because the
/// pipeline downstream *rejects* rather than repairs a structurally wrong
/// source: a prompt that omits "seamless vertically" produces work that is
/// thrown away, having cost a generation either way.
pub fn build_prompt(
    kind: TextureKind,
    subject: &str,
    width: u32,
    height: u32,
    rows: u32,
) -> String {
    let subject = subject.trim();
    match kind {
        TextureKind::Sheet => format!(
            "Create a {width}x{height} px seamless texture of: {subject}. This is an \
             animated sprite for a snake in a Snake game. The texture is applied to the \
             snake lengthwise, left to right, starting at the head; the y axis is time — \
             every tick advances one row, wrapping back to the top like a kernel. Every \
             cell of the snake is 16x16 px. We need {rows} rows of unique frames before \
             the animation repeats. The image must be seamless vertically, and seamless \
             horizontally so it can tile along the body. Flat, even lighting; no drop \
             shadows; no frame-to-frame drift — the pattern must animate in place rather \
             than sliding, or the snake will read as rotating."
        ),
        TextureKind::Coat => format!(
            "Create a {width}x{height} px seamless texture of: {subject}. This is the coat \
             of a snake in a Snake game, applied lengthwise from the head. It must be \
             seamless in both axes so it tiles along the body without a visible join. Flat, \
             even lighting with no directional shadow. Six to eight distinct marks across \
             the height, so the pattern reads as a coat rather than as a grid. Fill the \
             frame edge to edge; no border, no vignette, no background."
        ),
        TextureKind::Overlay => format!(
            "Create a {width}x{height} px image of: {subject}. This is a piece of equipment \
             worn by a snake in a Snake game, sitting over the head and the cells just \
             behind it. Every cell of the snake is 16x16 px, so this is {} cells long. \
             Transparent background — only the object itself is drawn. It does not tile and \
             must not be seamless; draw it once, centred, filling the height.",
            width / TextureKind::Overlay.canonical_texels_per_cell()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_descriptor_is_exact_content_addressed_and_sanitized() {
        let canonical = "a".repeat(64);
        let smaller = "b".repeat(64);
        let texture = Texture {
            texture_id: 7,
            owner_user_id: 42,
            shareable: false,
            content_ref: format!("sha256:{canonical}"),
            kind: TextureKind::Sheet,
            width_px: 320,
            height_px: 640,
            repeat_cells: Some(20.0),
            rows: Some(40),
            seams: SeamReport {
                horizontal_ratio: 0.2,
                vertical_ratio: 0.3,
                repaired: true,
            },
            verified_seam_axes: vec![SeamAxis::Y],
            last_prompt: Some("private generation prompt".to_string()),
            variants: vec![
                TextureVariant {
                    texels_per_cell: 16,
                    width_px: 320,
                    height_px: 640,
                    bytes: 100,
                    sha256: canonical.clone(),
                },
                TextureVariant {
                    texels_per_cell: 8,
                    width_px: 160,
                    height_px: 320,
                    bytes: 50,
                    sha256: smaller.clone(),
                },
            ],
            created_at_ms: 1,
        };

        let descriptor = texture.descriptor();
        assert_eq!(descriptor.kind, skin_schema::v2::TextureKindV2::Sheet);
        assert_eq!(descriptor.body_columns, Some(20));
        assert_eq!(descriptor.frame_rows, Some(40));
        assert_eq!(
            descriptor.variants[1].url,
            format!("/api/textures/variants/sha256:{smaller}.png")
        );
        assert_eq!(
            descriptor.variants[1].content_ref,
            format!("sha256:{smaller}")
        );

        let public = serde_json::to_string(&descriptor).expect("serializes");
        assert!(!public.contains("owner"));
        assert!(!public.contains("prompt"));
        assert!(!public.contains("42"));
    }

    fn coat(width: u32, height: u32) -> ProposedTexture {
        ProposedTexture {
            kind: TextureKind::Coat,
            width_px: width,
            height_px: height,
            rows: None,
            byte_len: 1024,
        }
    }

    fn sheet(width: u32, height: u32, rows: u32) -> ProposedTexture {
        ProposedTexture {
            kind: TextureKind::Sheet,
            width_px: width,
            height_px: height,
            rows: Some(rows),
            byte_len: 1024,
        }
    }

    /// The shipped coats, as the client declares them, must be accepted — the
    /// server's idea of a coat and the renderer's have to be the same idea.
    #[test]
    fn the_shipped_coat_dimensions_are_accepted() {
        // zebra 768x64 (12 cells), tiger/jaguar 832x64 (13 cells).
        assert!(validate_shape(coat(768, 64)).is_ok());
        assert!(validate_shape(coat(832, 64)).is_ok());
        assert_eq!(repeat_cells(TextureKind::Coat, 768), Some(12.0));
        assert_eq!(repeat_cells(TextureKind::Coat, 832), Some(13.0));
    }

    #[test]
    fn the_shipped_sheet_dimensions_are_accepted() {
        // zebra-live 576x320 (36 cells, 20 rows), tiger-live 896x320,
        // race-livery 1280x320, stars-and-stripes 320x308 (14 rows).
        assert!(validate_shape(sheet(576, 320, 20)).is_ok());
        assert!(validate_shape(sheet(896, 320, 20)).is_ok());
        assert!(validate_shape(sheet(1280, 320, 20)).is_ok());
        assert!(validate_shape(sheet(320, 308, 14)).is_ok());
    }

    /// The quiet one. A sheet whose height does not divide by its rows still
    /// draws — every frame just samples across into the next.
    #[test]
    fn a_sheet_height_that_does_not_divide_by_its_rows_is_refused() {
        let errors = validate_shape(sheet(320, 321, 20)).expect_err("321 does not divide by 20");
        assert!(errors.iter().any(|error| error.field == "height"));
        assert!(
            errors[0].problem.contains("sample across"),
            "the message should say what goes wrong: {errors:?}"
        );
    }

    #[test]
    fn a_coat_must_be_exactly_one_cell_tall_and_a_whole_number_of_cells_wide() {
        assert!(
            validate_shape(coat(768, 65)).is_err(),
            "65px is not one cell"
        );
        assert!(
            validate_shape(coat(770, 64)).is_err(),
            "770px is not whole cells"
        );
    }

    #[test]
    fn an_oversized_or_heavy_image_is_refused_from_its_header_alone() {
        let huge = ProposedTexture {
            kind: TextureKind::Coat,
            width_px: 20_000,
            height_px: 64,
            rows: None,
            byte_len: 1024,
        };
        assert!(validate_shape(huge).is_err());

        let heavy = ProposedTexture {
            byte_len: MAX_VARIANT_BYTES + 1,
            ..coat(768, 64)
        };
        assert!(validate_shape(heavy).is_err());
    }

    #[test]
    fn a_sheet_must_declare_its_frames_and_stay_within_the_ceiling() {
        let no_rows = ProposedTexture {
            rows: None,
            ..sheet(320, 320, 20)
        };
        assert!(validate_shape(no_rows).is_err());
        assert!(validate_shape(sheet(320, 320, 0)).is_err());
        assert!(validate_shape(sheet(320, 320, MAX_SHEET_ROWS + 1)).is_err());
    }

    #[test]
    fn only_a_sheet_carries_frames() {
        let coat_with_rows = ProposedTexture {
            rows: Some(4),
            ..coat(768, 64)
        };
        assert!(validate_shape(coat_with_rows).is_err());
    }

    /// An overlay is drawn once and never tiles, so it is held to neither seam.
    #[test]
    fn an_overlay_is_not_held_to_a_seam_it_does_not_have() {
        let seams = SeamReport {
            horizontal_ratio: 1.0,
            vertical_ratio: 1.0,
            repaired: false,
        };
        assert!(seams.passes(TextureKind::Overlay));
        assert!(
            !seams.passes(TextureKind::Coat),
            "a coat tiles along the body"
        );
        assert!(!seams.passes(TextureKind::Sheet), "a sheet tiles in time");
        assert_eq!(repeat_cells(TextureKind::Overlay, 64), None);
    }

    #[test]
    fn a_join_within_the_gate_passes_for_the_axis_that_needs_it() {
        let repaired = SeamReport {
            horizontal_ratio: 0.85,
            vertical_ratio: 0.85,
            repaired: true,
        };
        assert!(repaired.passes(TextureKind::Coat));
        assert!(repaired.passes(TextureKind::Sheet));
    }

    /// The ladders are per kind, and a sheet does not get upsampled rungs it
    /// has no detail for.
    #[test]
    fn ladders_match_the_conventions_the_renderer_reads() {
        assert_eq!(TextureKind::Coat.canonical_texels_per_cell(), 64);
        assert_eq!(TextureKind::Coat.ladder(), &[32, 16]);
        assert_eq!(TextureKind::Sheet.canonical_texels_per_cell(), 16);
        assert_eq!(TextureKind::Sheet.ladder(), &[8]);
        assert_eq!(TextureKind::Overlay.canonical_texels_per_cell(), 64);
    }

    /// The prompt has to carry the constraints the pipeline will enforce, or
    /// the generation is thrown away having cost the same as a good one.
    #[test]
    fn a_sheet_prompt_states_every_constraint_the_gates_will_check() {
        let prompt = build_prompt(TextureKind::Sheet, "a waving american flag", 320, 320, 20);
        assert!(prompt.contains("320x320"));
        assert!(prompt.contains("a waving american flag"));
        assert!(prompt.contains("y axis is time"));
        assert!(prompt.contains("16x16"));
        assert!(prompt.contains("20 rows"));
        assert!(prompt.contains("seamless vertically"));
        // Frame translation is a build error downstream, so it is a prompt
        // instruction here.
        assert!(prompt.contains("sliding") || prompt.contains("drift"));
    }

    #[test]
    fn a_coat_prompt_asks_for_both_axes_and_the_mark_density() {
        let prompt = build_prompt(TextureKind::Coat, "leopard print", 768, 64, 1);
        assert!(prompt.contains("seamless in both axes"));
        assert!(prompt.contains("Six to eight"));
        assert!(prompt.contains("even lighting"));
    }

    #[test]
    fn an_overlay_prompt_asks_for_transparency_and_says_how_long_it_is() {
        let prompt = build_prompt(TextureKind::Overlay, "a knight's helmet", 256, 64, 1);
        assert!(prompt.contains("Transparent background"));
        assert!(prompt.contains("4 cells long"));
        assert!(
            prompt.contains("does not tile"),
            "an overlay must not be asked for seamlessness it cannot have"
        );
    }

    fn png_bytes(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes
    }

    #[test]
    fn a_png_header_gives_up_its_dimensions_without_a_decode() {
        let header = read_png_header(&png_bytes(768, 64)).expect("a well-formed header");
        assert_eq!(header.width_px, 768);
        assert_eq!(header.height_px, 64);
    }

    /// The decompression-bomb gate: a small upload can declare an enormous
    /// image, and the size limit has to be applied to the declaration rather
    /// than to whatever a decoder allocates believing it.
    #[test]
    fn an_enormous_declaration_is_caught_from_a_tiny_upload() {
        let bytes = png_bytes(60_000, 60_000);
        assert!(bytes.len() < 32, "the upload itself is tiny");

        let header = read_png_header(&bytes).expect("the header parses");
        let refusal = validate_shape(ProposedTexture {
            kind: TextureKind::Coat,
            width_px: header.width_px,
            height_px: header.height_px,
            rows: None,
            byte_len: bytes.len(),
        })
        .expect_err("14 gigabytes decoded");
        assert!(refusal.iter().any(|error| error.problem.contains("limit")));
    }

    /// The magic bytes decide, not the content type or the filename.
    #[test]
    fn anything_that_is_not_a_png_is_refused_by_its_bytes() {
        assert!(read_png_header(b"GIF89a and then some padding bytes").is_err());
        assert!(read_png_header(b"too short").is_err());
        assert!(read_png_header(&png_bytes(0, 64)).is_err(), "no extent");

        let mut wrong_chunk = png_bytes(64, 64);
        wrong_chunk[12..16].copy_from_slice(b"IDAT");
        assert!(read_png_header(&wrong_chunk).is_err());
    }

    #[test]
    fn kinds_round_trip_through_their_stored_strings() {
        for kind in [TextureKind::Coat, TextureKind::Sheet, TextureKind::Overlay] {
            assert_eq!(TextureKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(TextureKind::parse("sprite"), None);
    }
}
