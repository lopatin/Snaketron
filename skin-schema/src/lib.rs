//! The skin document format, and the rules a document has to satisfy.
//!
//! A skin is data. That is what makes it safe to let someone other than a Rust
//! programmer write one, and it is why this crate has no rendering dependency:
//! the same validator runs in CI, in the authoring CLI, and (compiled to wasm)
//! in the browser, so a skin cannot pass one gate and fail another.
//!
//! The constraints below are not style advice. Team games communicate through
//! colour — blue is a friend, red is not, yellow means someone is boosting —
//! and a skin that breaks those readings is a competitive bug, not a bold
//! choice. Validation is where that line is enforced.

pub mod color;
pub mod expr;

use color::{ENEMY_HUES, FRIENDLY_HUES, HueWindow, NEUTRAL_CHROMA, Rgb, contrast_ratio};
use serde::{Deserialize, Serialize};

/// The only schema version this build understands.
pub const SCHEMA_VERSION: u32 = 1;

/// The Boost band is pinned for document skins: an opponent has to be able to
/// tell you are boosting, and a document author cannot be allowed to tune that
/// signal away. First-party Rust skins may restyle it, and are held to the same
/// visible-difference conformance check instead.
pub const REQUIRED_BOOST_COLOR: &str = "#fff200";
pub const REQUIRED_BOOST_EXTRA_PX: f64 = 6.0;

/// Bounds that keep a skin from distorting the layout around it.
pub const MAX_OUTLINE_EXTRA_PX: f64 = 4.0;
pub const MIN_OUTLINE_EXTRA_PX: f64 = 0.0;
/// The roster reserves row height for the overhang, so an unbounded outline
/// would squeeze the snake into nothing.
pub const MAX_OVERHANG_PX: f64 = 3.0;
/// Minimum contrast for the name and carried-food ink against the body.
pub const MIN_LABEL_CONTRAST: f64 = 4.5;
/// Minimum contrast between the head core and the body, so the head stays
/// findable, and between the ready-check and the core it sits on.
pub const MIN_HEAD_CORE_CONTRAST: f64 = 2.0;
pub const MIN_READY_CHECK_CONTRAST: f64 = 3.0;
/// The roster's ready-check is painted white.
pub const READY_CHECK_INK: &str = "#ffffff";

/// How many discrete frames an animated skin is compiled into.
///
/// Animation is a ring of precomputed palettes rather than live colour maths:
/// it makes the per-frame cost an index, keeps the op sequence identical at
/// every point in the cycle, and lets the validator check every frame a viewer
/// can possibly see instead of sampling and hoping.
pub const ANIMATION_STEPS: usize = 32;

/// One body colour and its contour.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColorPair {
    pub fill: String,
    pub outline: String,
}

/// Colours for every role the renderer can resolve.
///
/// Dimensioned to the real decision table rather than to something tidier:
/// two within-team shades per side (so a 2v2 roster can tell teammates apart)
/// and four free-for-all slots. Spectated teams reuse the team palettes, which
/// is what lets the classic look be expressed as a document at all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RolePalette {
    /// Indexed by within-team shade slot.
    pub friendly: [ColorPair; 2],
    pub enemy: [ColorPair; 2],
    /// Indexed by the renderer-resolved free-for-all paint slot.
    pub free_for_all: [ColorPair; 4],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeadGradient {
    /// How far the brightening reaches back from the head, in cells.
    pub length_cells: f64,
    /// Opacity at the head itself.
    pub max_opacity: f64,
    pub color: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeadStyle {
    /// Radius of the dark core, as a fraction of one cell.
    pub core_ratio: f64,
    pub core_color: String,
    pub gradient: HeadGradient,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoostBand {
    pub color: String,
    pub extra_px: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OutlineStyle {
    /// Contour width beyond the body, total across both sides.
    pub extra_px: f64,
    pub boost_band: BoostBand,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelStyle {
    /// Explicit label ink. Derived from the fill when absent.
    #[serde(default)]
    pub ink: Option<String>,
    /// Explicit DOM swatch. The fill when absent.
    #[serde(default)]
    pub swatch: Option<String>,
}

/// What an animation track modulates.
///
/// Deliberately a closed list of scalars. A track can make a skin shimmer or
/// pulse; it cannot add, remove, or reorder anything the skin paints, which is
/// what keeps animated skins verifiable and cheap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackTarget {
    /// Lightness of the body fill.
    BodyLightness,
    /// Lightness of the contour.
    OutlineLightness,
    /// Opacity of the head gradient.
    GradientOpacity,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnimationTrack {
    pub target: TrackTarget,
    /// Peak deviation. Lightness tracks are in 0..1 of the channel range;
    /// the opacity track is in absolute opacity.
    pub amplitude: f64,
    /// Where in the cycle this track starts, in turns (0..1).
    #[serde(default)]
    pub phase: f64,
}

/// Light travelling along the body, head to tail.
///
/// The head gradient already paints one rectangle per cell at a per-cell
/// opacity, so making that opacity a function of distance *and* time costs
/// nothing extra — same rectangles, different alpha. It is the one animation
/// that makes a snake look alive rather than merely lit, which is why it earns
/// a place in the schema instead of being every skin's reason to escalate to
/// Rust.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WaveSpec {
    /// Distance between crests, in cells. Shorter reads as ripples, longer as
    /// a single slow surge.
    pub cells_per_crest: f64,
    /// How far the wave pushes the gradient's opacity either side of its
    /// resting value.
    pub amplitude: f64,
    /// Negative runs the wave tail-to-head instead.
    #[serde(default = "default_wave_speed")]
    pub crests_per_cycle: f64,
}

fn default_wave_speed() -> f64 {
    1.0
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnimationSpec {
    /// One full cycle, in milliseconds.
    pub period_ms: f64,
    #[serde(default)]
    pub tracks: Vec<AnimationTrack>,
    /// Optional travelling wave along the body.
    #[serde(default)]
    pub wave: Option<WaveSpec>,
}

/// Viewer-attributed base dressing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseTheme {
    pub friendly_zone: String,
    pub enemy_zone: String,
    pub friendly_wall: String,
    pub enemy_wall: String,
    pub friendly_text: String,
    pub enemy_text: String,
}

/// Scorer-attributed celebration dressing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CelebrationTheme {
    /// Names a first-party effect renderer. A document chooses which effect
    /// plays; it never supplies the code that draws it.
    pub effect: String,
    pub friendly_accent: String,
    pub enemy_accent: String,
    pub readout_friendly: String,
    pub readout_enemy: String,
}

/// Documents exempt from the lit-label contrast rule.
///
/// `classic-doc@1` reproduces the look Snaketron already ships, and that look
/// has one known weak spot: on the steel free-for-all slot (`#556270`) the
/// derived white label reaches only 3.2:1 where the head gradient lightens the
/// body underneath it. That is a real readability issue and predates skins by
/// years — but fixing it means changing pixels players already see, which the
/// skins work deliberately does not do. It is recorded here rather than papered
/// over by loosening the rule for everyone.
///
/// New skins get the strict rule. This list should never grow.
const LIT_LABEL_EXEMPT: &[&str] = &["classic-doc@1"];

/// Effect ids the client knows how to draw.
pub const KNOWN_EFFECTS: &[&str] = &["goal-impact-wave"];

/// One authored skin.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SkinDoc {
    pub schema_version: u32,
    /// Catalogue id, e.g. `aurora@1`.
    pub id: String,
    pub name: String,
    pub palette: RolePalette,
    pub head: HeadStyle,
    pub outline: OutlineStyle,
    #[serde(default)]
    pub labels: LabelStyle,
    #[serde(default)]
    pub animation: Option<AnimationSpec>,
    #[serde(default)]
    pub base: Option<BaseTheme>,
    #[serde(default)]
    pub celebration: Option<CelebrationTheme>,
}

/// Why a document was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkinDocError {
    pub field: String,
    pub problem: String,
}

impl SkinDocError {
    fn new(field: impl Into<String>, problem: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            problem: problem.into(),
        }
    }
}

impl std::fmt::Display for SkinDocError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.field, self.problem)
    }
}

impl std::error::Error for SkinDocError {}

/// Shift a colour's lightness, staying in gamut.
///
/// Used both by the animation compiler and by the validator, so the colours
/// that get checked are exactly the colours that get painted.
pub fn shift_lightness(color: Rgb, amount: f64) -> Rgb {
    let apply = |channel: f64| {
        if amount >= 0.0 {
            channel + (1.0 - channel) * amount
        } else {
            channel * (1.0 + amount)
        }
    };
    Rgb {
        r: apply(color.r).clamp(0.0, 1.0),
        g: apply(color.g).clamp(0.0, 1.0),
        b: apply(color.b).clamp(0.0, 1.0),
    }
}

/// The lightness offset a track contributes at one step of the cycle.
pub fn track_offset(track: &AnimationTrack, step: usize) -> f64 {
    let turns = step as f64 / ANIMATION_STEPS as f64 + track.phase;
    track.amplitude * (turns * std::f64::consts::TAU).sin()
}

/// The total offset for one target at one step.
pub fn animation_offset(
    animation: Option<&AnimationSpec>,
    target: TrackTarget,
    step: usize,
) -> f64 {
    animation.map_or(0.0, |spec| {
        spec.tracks
            .iter()
            .filter(|track| track.target == target)
            .map(|track| track_offset(track, step))
            .sum()
    })
}

fn parse(field: &str, hex: &str) -> Result<Rgb, SkinDocError> {
    Rgb::parse(hex).ok_or_else(|| {
        SkinDocError::new(
            field,
            format!("`{hex}` is not a 6-digit hex colour like `#3c8dde`"),
        )
    })
}

fn require_hue(field: &str, hex: &str, window: HueWindow, side: &str) -> Result<(), SkinDocError> {
    let color = parse(field, hex)?;
    let (hue, chroma) = color.oklch_hue_chroma();
    if chroma <= NEUTRAL_CHROMA {
        // A near-gray carries no side information either way, so it is judged
        // on legibility elsewhere rather than on hue.
        return Ok(());
    }
    if !window.contains(hue) {
        return Err(SkinDocError::new(
            field,
            format!(
                "`{hex}` reads at {hue:.0}deg, outside the {side} range \
                 ({:.0}..{:.0}deg). Teams are told apart by colour, so a {side} \
                 snake has to look like one.",
                window.from, window.to
            ),
        ));
    }
    Ok(())
}

/// Check one document against every rule.
///
/// Every reachable animation frame is checked, not just the resting pose: a
/// skin that only breaks the rules mid-pulse still breaks them.
pub fn validate(doc: &SkinDoc) -> Result<(), Vec<SkinDocError>> {
    let mut errors = Vec::new();
    let mut check = |result: Result<(), SkinDocError>| {
        if let Err(error) = result {
            errors.push(error);
        }
    };

    if doc.schema_version != SCHEMA_VERSION {
        check(Err(SkinDocError::new(
            "schema_version",
            format!(
                "this build understands version {SCHEMA_VERSION}, not {}. \
                 Older clients fall back to the classic skin rather than \
                 guessing at a newer format.",
                doc.schema_version
            ),
        )));
    }

    if doc.id.trim().is_empty() {
        check(Err(SkinDocError::new("id", "a skin needs a catalogue id")));
    }
    if doc.name.trim().is_empty() {
        check(Err(SkinDocError::new(
            "name",
            "a skin needs a display name",
        )));
    }

    // Boost telegraphy is pinned for documents.
    if doc.outline.boost_band.color.to_ascii_lowercase() != REQUIRED_BOOST_COLOR {
        check(Err(SkinDocError::new(
            "outline.boost_band.color",
            format!(
                "must be `{REQUIRED_BOOST_COLOR}`. Opponents read the Boost \
                 band to know you are boosting, so a document skin cannot \
                 restyle it."
            ),
        )));
    }
    if (doc.outline.boost_band.extra_px - REQUIRED_BOOST_EXTRA_PX).abs() > f64::EPSILON {
        check(Err(SkinDocError::new(
            "outline.boost_band.extra_px",
            format!(
                "must be {REQUIRED_BOOST_EXTRA_PX} so the band stays as visible as it is today"
            ),
        )));
    }

    if !(MIN_OUTLINE_EXTRA_PX..=MAX_OUTLINE_EXTRA_PX).contains(&doc.outline.extra_px) {
        check(Err(SkinDocError::new(
            "outline.extra_px",
            format!("must be between {MIN_OUTLINE_EXTRA_PX} and {MAX_OUTLINE_EXTRA_PX}"),
        )));
    }
    let overhang = (doc.outline.extra_px / 2.0).max(doc.outline.boost_band.extra_px / 2.0);
    if overhang > MAX_OVERHANG_PX {
        check(Err(SkinDocError::new(
            "outline",
            format!(
                "paints {overhang}px beyond the body, over the {MAX_OVERHANG_PX}px \
                 cap the roster row is sized for"
            ),
        )));
    }

    if !(0.05..=0.5).contains(&doc.head.core_ratio) {
        check(Err(SkinDocError::new(
            "head.core_ratio",
            "must be between 0.05 and 0.5 of a cell",
        )));
    }
    if !(0.0..=1.0).contains(&doc.head.gradient.max_opacity) {
        check(Err(SkinDocError::new(
            "head.gradient.max_opacity",
            "must be between 0 and 1",
        )));
    }
    if !(0.0..=64.0).contains(&doc.head.gradient.length_cells) {
        check(Err(SkinDocError::new(
            "head.gradient.length_cells",
            "must be between 0 and 64 cells",
        )));
    }

    check(parse("head.core_color", &doc.head.core_color).map(|_| ()));
    check(parse("head.gradient.color", &doc.head.gradient.color).map(|_| ()));

    if let Some(animation) = &doc.animation {
        if !(120.0..=60_000.0).contains(&animation.period_ms) {
            check(Err(SkinDocError::new(
                "animation.period_ms",
                "must be between 120ms and 60000ms — faster reads as a flicker",
            )));
        }
        if animation.tracks.is_empty() && animation.wave.is_none() {
            check(Err(SkinDocError::new(
                "animation",
                "an animation with no tracks and no wave should just be omitted",
            )));
        }
        if let Some(wave) = &animation.wave {
            if !(1.0..=64.0).contains(&wave.cells_per_crest) {
                check(Err(SkinDocError::new(
                    "animation.wave.cells_per_crest",
                    "must be between 1 and 64 cells; below one cell the wave \
                     lands between pixels and reads as noise",
                )));
            }
            if !(0.0..=0.35).contains(&wave.amplitude) {
                check(Err(SkinDocError::new(
                    "animation.wave.amplitude",
                    "must be between 0 and 0.35; a bigger swing stops reading \
                     as light moving along the body and starts reading as the \
                     body changing colour",
                )));
            }
            if !(-8.0..=8.0).contains(&wave.crests_per_cycle) || wave.crests_per_cycle == 0.0 {
                check(Err(SkinDocError::new(
                    "animation.wave.crests_per_cycle",
                    "must be a non-zero value between -8 and 8; negative runs \
                     the wave tail-to-head",
                )));
            }
        }
        for (index, track) in animation.tracks.iter().enumerate() {
            if !(0.0..=0.35).contains(&track.amplitude) {
                check(Err(SkinDocError::new(
                    format!("animation.tracks[{index}].amplitude"),
                    "must be between 0 and 0.35; a bigger swing changes what \
                     colour the snake reads as, not just how it shimmers",
                )));
            }
            if !(0.0..1.0).contains(&track.phase) {
                check(Err(SkinDocError::new(
                    format!("animation.tracks[{index}].phase"),
                    "must be between 0 and 1 turns",
                )));
            }
        }
    }

    // Colours, at every animation step, for every role.
    let body_track =
        |step| animation_offset(doc.animation.as_ref(), TrackTarget::BodyLightness, step);
    let outline_track =
        |step| animation_offset(doc.animation.as_ref(), TrackTarget::OutlineLightness, step);
    let steps = if doc.animation.is_some() {
        ANIMATION_STEPS
    } else {
        1
    };

    // The carried-food number and the roster name sit on the body a couple of
    // cells behind the head — exactly where the head gradient lightens it most,
    // and where a wave pushes it further still. Checking ink against the raw
    // fill would clear a skin whose label is unreadable in the one place labels
    // actually appear.
    let gradient_color = Rgb::parse(&doc.head.gradient.color);
    let peak_gradient = doc.head.gradient.max_opacity
        + doc
            .animation
            .as_ref()
            .and_then(|animation| animation.wave.as_ref())
            .map_or(0.0, |wave| wave.amplitude)
        + doc.animation.as_ref().map_or(0.0, |animation| {
            animation
                .tracks
                .iter()
                .filter(|track| track.target == TrackTarget::GradientOpacity)
                .map(|track| track.amplitude)
                .sum()
        });
    let peak_gradient = peak_gradient.clamp(0.0, 1.0);

    let lit_label_checked = !LIT_LABEL_EXEMPT.contains(&doc.id.as_str());
    let lit = |fill: Rgb| match gradient_color {
        Some(gradient) => Rgb {
            r: fill.r + (gradient.r - fill.r) * peak_gradient,
            g: fill.g + (gradient.g - fill.g) * peak_gradient,
            b: fill.b + (gradient.b - fill.b) * peak_gradient,
        },
        None => fill,
    };

    let core = Rgb::parse(&doc.head.core_color);
    let mut check_pair = |field: &str, pair: &ColorPair, window: Option<(HueWindow, &str)>| {
        if let Some((window, side)) = window {
            check(require_hue(
                &format!("{field}.fill"),
                &pair.fill,
                window,
                side,
            ));
            check(require_hue(
                &format!("{field}.outline"),
                &pair.outline,
                window,
                side,
            ));
        }
        let Ok(fill) = parse(&format!("{field}.fill"), &pair.fill) else {
            check(parse(&format!("{field}.fill"), &pair.fill).map(|_| ()));
            return;
        };
        check(parse(&format!("{field}.outline"), &pair.outline).map(|_| ()));

        for step in 0..steps {
            let animated_fill = shift_lightness(fill, body_track(step));
            let _ = outline_track(step);

            // The label has to stay readable on every frame of the animation.
            let ink = match &doc.labels.ink {
                Some(ink) => Rgb::parse(ink),
                None => Some(derive_label_ink(animated_fill)),
            };
            if let Some(ink) = ink {
                // Worst case for the label is the brightest the body ever gets
                // under it, which is the resting fill plus the full gradient.
                let under_label = lit(animated_fill);
                let ratio = if lit_label_checked {
                    contrast_ratio(ink, animated_fill).min(contrast_ratio(ink, under_label))
                } else {
                    contrast_ratio(ink, animated_fill)
                };
                if ratio < MIN_LABEL_CONTRAST {
                    check(Err(SkinDocError::new(
                        format!("{field}.fill"),
                        format!(
                            "the name and carried-food number only reach \
                             {ratio:.1}:1 on this body (step {step} of the \
                             animation, where the head gradient lightens \
                             `{}` to `{}`); {MIN_LABEL_CONTRAST}:1 is the floor",
                            animated_fill.to_hex(),
                            under_label.to_hex()
                        ),
                    )));
                    break;
                }
            }

            if let Some(core) = core {
                let ratio = contrast_ratio(core, animated_fill);
                if ratio < MIN_HEAD_CORE_CONTRAST {
                    check(Err(SkinDocError::new(
                        "head.core_color",
                        format!(
                            "the head core only reaches {ratio:.1}:1 against \
                             `{}`; players find the head by its core, so \
                             {MIN_HEAD_CORE_CONTRAST}:1 is the floor",
                            animated_fill.to_hex()
                        ),
                    )));
                    break;
                }
            }
        }
    };

    check_pair(
        "palette.friendly[0]",
        &doc.palette.friendly[0],
        Some((FRIENDLY_HUES, "friendly")),
    );
    check_pair(
        "palette.friendly[1]",
        &doc.palette.friendly[1],
        Some((FRIENDLY_HUES, "friendly")),
    );
    check_pair(
        "palette.enemy[0]",
        &doc.palette.enemy[0],
        Some((ENEMY_HUES, "enemy")),
    );
    check_pair(
        "palette.enemy[1]",
        &doc.palette.enemy[1],
        Some((ENEMY_HUES, "enemy")),
    );
    // Free-for-all slots 0 and 1 double as the spectated blue and red sides, so
    // they carry the same obligation; slots 2 and 3 are free.
    check_pair(
        "palette.free_for_all[0]",
        &doc.palette.free_for_all[0],
        Some((FRIENDLY_HUES, "friendly")),
    );
    check_pair(
        "palette.free_for_all[1]",
        &doc.palette.free_for_all[1],
        Some((ENEMY_HUES, "enemy")),
    );
    check_pair(
        "palette.free_for_all[2]",
        &doc.palette.free_for_all[2],
        None,
    );
    check_pair(
        "palette.free_for_all[3]",
        &doc.palette.free_for_all[3],
        None,
    );

    // The roster paints a white check on the head core when a player is ready.
    if let Some(core) = core {
        let ready = Rgb::parse(READY_CHECK_INK).expect("the ready check ink is a literal");
        let ratio = contrast_ratio(core, ready);
        if ratio < MIN_READY_CHECK_CONTRAST {
            check(Err(SkinDocError::new(
                "head.core_color",
                format!(
                    "the roster's white ready-check only reaches {ratio:.1}:1 \
                     on this core; keep the core dark enough to read it \
                     ({MIN_READY_CHECK_CONTRAST}:1 minimum)"
                ),
            )));
        }
    }

    if let Some(ink) = &doc.labels.ink {
        check(parse("labels.ink", ink).map(|_| ()));
    }
    if let Some(swatch) = &doc.labels.swatch {
        check(parse("labels.swatch", swatch).map(|_| ()));
    }

    if let Some(base) = &doc.base {
        check(require_hue(
            "base.friendly_zone",
            &base.friendly_zone,
            FRIENDLY_HUES,
            "friendly",
        ));
        check(require_hue(
            "base.enemy_zone",
            &base.enemy_zone,
            ENEMY_HUES,
            "enemy",
        ));
        check(require_hue(
            "base.friendly_wall",
            &base.friendly_wall,
            FRIENDLY_HUES,
            "friendly",
        ));
        check(require_hue(
            "base.enemy_wall",
            &base.enemy_wall,
            ENEMY_HUES,
            "enemy",
        ));
        check(parse("base.friendly_text", &base.friendly_text).map(|_| ()));
        check(parse("base.enemy_text", &base.enemy_text).map(|_| ()));
    }

    if let Some(celebration) = &doc.celebration {
        if !KNOWN_EFFECTS.contains(&celebration.effect.as_str()) {
            check(Err(SkinDocError::new(
                "celebration.effect",
                format!(
                    "`{}` is not an effect this client can draw. Available: {}",
                    celebration.effect,
                    KNOWN_EFFECTS.join(", ")
                ),
            )));
        }
        check(require_hue(
            "celebration.friendly_accent",
            &celebration.friendly_accent,
            FRIENDLY_HUES,
            "friendly",
        ));
        check(require_hue(
            "celebration.enemy_accent",
            &celebration.enemy_accent,
            ENEMY_HUES,
            "enemy",
        ));
        check(
            parse(
                "celebration.readout_friendly",
                &celebration.readout_friendly,
            )
            .map(|_| ()),
        );
        check(parse("celebration.readout_enemy", &celebration.readout_enemy).map(|_| ()));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// The ink a label gets when a document does not name one.
///
/// Mirrors the renderer's long-standing rule: pick whichever of the dark slate
/// or white reads better on this body colour.
pub fn derive_label_ink(fill: Rgb) -> Rgb {
    let dark = Rgb::parse("#0f172a").expect("literal");
    let light = Rgb::parse("#ffffff").expect("literal");
    if contrast_ratio(dark, fill) >= contrast_ratio(light, fill) {
        dark
    } else {
        light
    }
}

/// Parse and validate in one step.
pub fn load(json: &str) -> Result<SkinDoc, Vec<SkinDocError>> {
    let doc: SkinDoc = serde_json::from_str(json)
        .map_err(|error| vec![SkinDocError::new("document", error.to_string())])?;
    validate(&doc)?;
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classic() -> SkinDoc {
        serde_json::from_str(include_str!("../skins/classic.skin.json"))
            .expect("the shipped classic document parses")
    }

    #[test]
    fn the_classic_document_is_valid() {
        if let Err(errors) = validate(&classic()) {
            panic!("classic.skin.json should validate, but: {errors:?}");
        }
    }

    #[test]
    fn a_teammate_may_not_wear_an_enemy_colour() {
        let mut doc = classic();
        doc.palette.friendly[0].fill = "#ff4444".to_string();
        let errors = validate(&doc).expect_err("a red teammate must be rejected");
        assert!(
            errors.iter().any(|e| e.field == "palette.friendly[0].fill"),
            "{errors:?}"
        );
    }

    #[test]
    fn the_boost_band_cannot_be_dimmed_or_recoloured() {
        let mut doc = classic();
        doc.outline.boost_band.color = "#111111".to_string();
        let errors = validate(&doc).expect_err("a hidden Boost band must be rejected");
        assert!(
            errors.iter().any(|e| e.field == "outline.boost_band.color"),
            "{errors:?}"
        );

        let mut doc = classic();
        doc.outline.boost_band.extra_px = 1.0;
        let errors = validate(&doc).expect_err("a shrunken Boost band must be rejected");
        assert!(
            errors
                .iter()
                .any(|e| e.field == "outline.boost_band.extra_px"),
            "{errors:?}"
        );
    }

    /// The point of checking every step: a skin can look fine at rest and go
    /// unreadable halfway through its cycle.
    #[test]
    fn animation_is_checked_at_every_step_not_just_at_rest() {
        let mut doc = classic();
        doc.animation = Some(AnimationSpec {
            period_ms: 2_000.0,
            tracks: vec![AnimationTrack {
                target: TrackTarget::BodyLightness,
                // Legal on its own, but it brightens the body far enough that
                // the dark ink stops clearing AA partway round the cycle.
                amplitude: 0.34,
                phase: 0.0,
            }],
            wave: None,
        });
        doc.palette.friendly[0].fill = "#5f9ea0".to_string();
        doc.labels.ink = Some("#8fa3b8".to_string());

        let errors = validate(&doc).expect_err("a mid-cycle contrast failure must be caught");
        assert!(
            errors.iter().any(|e| e.problem.contains("step")),
            "the error should name the offending step: {errors:?}"
        );
    }

    #[test]
    fn amplitudes_beyond_a_shimmer_are_rejected() {
        let mut doc = classic();
        doc.animation = Some(AnimationSpec {
            period_ms: 2_000.0,
            tracks: vec![AnimationTrack {
                target: TrackTarget::BodyLightness,
                amplitude: 0.9,
                phase: 0.0,
            }],
            wave: None,
        });
        let errors = validate(&doc).expect_err("a huge swing must be rejected");
        assert!(
            errors
                .iter()
                .any(|e| e.field == "animation.tracks[0].amplitude"),
            "{errors:?}"
        );
    }

    /// The wave rides on the head gradient, which is exactly where the
    /// carried-food number sits. A wave that washes the label out has to fail
    /// even though the resting body is perfectly readable.
    #[test]
    fn a_wave_that_washes_out_the_label_is_rejected() {
        let mut doc = classic();
        doc.palette.friendly[0].fill = "#3f6f8f".to_string();
        doc.palette.friendly[0].outline = "#26485e".to_string();
        doc.labels.ink = Some("#ffffff".to_string());
        doc.head.gradient.max_opacity = 0.3;
        doc.animation = Some(AnimationSpec {
            period_ms: 1_800.0,
            tracks: Vec::new(),
            wave: Some(WaveSpec {
                cells_per_crest: 6.0,
                amplitude: 0.34,
                crests_per_cycle: 1.0,
            }),
        });

        let errors = validate(&doc).expect_err("a label-washing wave must be rejected");
        assert!(
            errors
                .iter()
                .any(|e| e.problem.contains("head gradient lightens")),
            "the error should point at the gradient, not just the fill: {errors:?}"
        );
    }

    /// The exemption is for the shipped look and nothing else — a new skin
    /// with the same weakness must still be rejected.
    #[test]
    fn the_lit_label_exemption_covers_only_the_shipped_classic_document() {
        let mut doc = classic();
        assert!(validate(&doc).is_ok(), "the shipped document is exempt");

        doc.id = "borrowed-classic@1".to_string();
        let errors = validate(&doc)
            .expect_err("a new skin may not inherit the classic document's exemption");
        assert!(
            errors
                .iter()
                .any(|e| e.problem.contains("head gradient lightens")),
            "{errors:?}"
        );
    }

    #[test]
    fn wave_bounds_are_enforced() {
        for (wave, field) in [
            (
                WaveSpec {
                    cells_per_crest: 0.25,
                    amplitude: 0.1,
                    crests_per_cycle: 1.0,
                },
                "animation.wave.cells_per_crest",
            ),
            (
                WaveSpec {
                    cells_per_crest: 6.0,
                    amplitude: 0.9,
                    crests_per_cycle: 1.0,
                },
                "animation.wave.amplitude",
            ),
            (
                WaveSpec {
                    cells_per_crest: 6.0,
                    amplitude: 0.1,
                    crests_per_cycle: 0.0,
                },
                "animation.wave.crests_per_cycle",
            ),
        ] {
            let mut doc = classic();
            doc.animation = Some(AnimationSpec {
                period_ms: 1_800.0,
                tracks: Vec::new(),
                wave: Some(wave),
            });
            let errors = validate(&doc).expect_err("out-of-bounds wave must be rejected");
            assert!(errors.iter().any(|e| e.field == field), "{errors:?}");
        }
    }

    #[test]
    fn a_celebration_may_only_name_an_effect_the_client_can_draw() {
        let mut doc = classic();
        doc.celebration = Some(CelebrationTheme {
            effect: "supernova".to_string(),
            friendly_accent: "#5299bb".to_string(),
            enemy_accent: "#d45454".to_string(),
            readout_friendly: "#2b6f8c".to_string(),
            readout_enemy: "#a83232".to_string(),
        });
        let errors = validate(&doc).expect_err("unknown effects must be rejected");
        assert!(
            errors.iter().any(|e| e.field == "celebration.effect"),
            "{errors:?}"
        );
    }

    #[test]
    fn a_light_head_core_loses_the_ready_check() {
        let mut doc = classic();
        doc.head.core_color = "#f4f4f4".to_string();
        let errors = validate(&doc).expect_err("a washed-out core must be rejected");
        assert!(
            errors.iter().any(|e| e.field == "head.core_color"),
            "{errors:?}"
        );
    }

    #[test]
    fn a_future_schema_version_is_refused_rather_than_guessed_at() {
        let mut doc = classic();
        doc.schema_version = SCHEMA_VERSION + 1;
        let errors = validate(&doc).expect_err("a newer schema must be refused");
        assert!(errors.iter().any(|e| e.field == "schema_version"));
    }
}
