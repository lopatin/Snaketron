//! Composing a skin from a description.
//!
//! There is no image model behind this and that is deliberate rather than a
//! shortfall. The generation pipeline in `server::generation` records jobs,
//! meters spend and enforces ceilings, but nothing in the repository actually
//! calls a model — and the skins work concluded a model must not run inside
//! the game server at all. So the ideas here come from the schema's own
//! vocabulary instead: a prompt picks a mood, a mood picks colours and a
//! motif, and the motif assembles layers the renderer already knows how to
//! paint.
//!
//! What that buys is the property a generator actually needs: **every document
//! this returns has passed [`crate::v2::validate_v2`]**, the same function the
//! server runs before it will store one. Candidates that fail are repaired and
//! re-checked, and a candidate that cannot be repaired is dropped rather than
//! handed to an author who would meet the error later. Where the *ideas* come
//! from is a seam — swapping this for a model means replacing [`compose`] and
//! keeping the loop around it.
//!
//! Determinism is the other requirement. The same brief and seed produce the
//! same skins, which is what makes "regenerate" mean "show me different ones"
//! rather than "roll the dice again and hope".

use std::collections::BTreeMap;

use crate::color::{FRIENDLY_HUES, HueWindow, Rgb};
use crate::v2::{
    ClipV2, ColorRef, ColorTarget, CornerV2, GradientAxis, HeadCoreV2, LayerBodyV2, LayerV2,
    PropExpr, RegionV2, SCHEMA_VERSION_V2, SkinDocV2, SlotName, SourceV2, SpanV2, StopV2,
    TransformV2, validate_v2,
};
use crate::{ColorPair, LabelStyle, RolePalette, SkinDocError};

/// What the author asked for.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Brief {
    /// Free text. Read for mood words; anything unrecognised still steers the
    /// result, because the leftover text seeds the hue.
    pub prompt: String,
    /// Colours lifted from reference images the author supplied.
    ///
    /// A picture is a far better palette brief than a sentence, and it is one
    /// the client can honour without a model: sample the image, cluster the
    /// pixels, hand the results here.
    pub reference_colors: Vec<String>,
    /// Documents the author kept from an earlier round, as JSON.
    ///
    /// Keeping one is a stronger signal than any adjective, so the next round
    /// stays near their palettes and motifs instead of starting over.
    pub liked: Vec<SkinDocV2>,
    /// Extra text typed on a re-roll: "darker", "no stripes", "faster".
    pub guidance: String,
}

/// A mood, distilled from the words.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Mood {
    /// Where the theme sits on the wheel. Only the unwindowed slots can wear
    /// it literally; the rest get its character.
    hue: f64,
    /// 0 is ash, 1 is neon.
    chroma: f64,
    /// 0 is midnight, 1 is bleached.
    lightness: f64,
    /// How much the skin should move, 0..1.
    energy: f64,
}

impl Default for Mood {
    fn default() -> Self {
        Self {
            hue: 210.0,
            chroma: 0.62,
            lightness: 0.55,
            energy: 0.45,
        }
    }
}

/// One word and what it does to the mood. Additive, so "dark neon lava" reads
/// as all three rather than only the last one.
struct Word {
    stem: &'static str,
    hue: Option<f64>,
    chroma: Option<f64>,
    lightness: Option<f64>,
    energy: Option<f64>,
}

const fn word(
    stem: &'static str,
    hue: Option<f64>,
    chroma: Option<f64>,
    lightness: Option<f64>,
    energy: Option<f64>,
) -> Word {
    Word {
        stem,
        hue,
        chroma,
        lightness,
        energy,
    }
}

/// The vocabulary. Stems rather than whole words, so "icy", "ice" and "iced"
/// all land — matching is a prefix test on each token.
const WORDS: &[Word] = &[
    // Temperature and material.
    word("lava", Some(24.0), Some(0.95), Some(0.46), Some(0.8)),
    word("magma", Some(20.0), Some(0.95), Some(0.42), Some(0.8)),
    word("fire", Some(30.0), Some(0.9), Some(0.52), Some(0.85)),
    word("flame", Some(28.0), Some(0.9), Some(0.54), Some(0.85)),
    word("ember", Some(18.0), Some(0.8), Some(0.4), Some(0.5)),
    word("sun", Some(45.0), Some(0.85), Some(0.62), Some(0.6)),
    word("gold", Some(45.0), Some(0.8), Some(0.58), Some(0.4)),
    word("amber", Some(38.0), Some(0.82), Some(0.55), Some(0.4)),
    word("rust", Some(22.0), Some(0.6), Some(0.42), Some(0.2)),
    word("blood", Some(6.0), Some(0.8), Some(0.38), Some(0.5)),
    word("ice", Some(196.0), Some(0.45), Some(0.76), Some(0.3)),
    word("frost", Some(190.0), Some(0.4), Some(0.8), Some(0.25)),
    word("glacier", Some(200.0), Some(0.42), Some(0.72), Some(0.2)),
    word("arctic", Some(198.0), Some(0.38), Some(0.78), Some(0.2)),
    word("ocean", Some(215.0), Some(0.7), Some(0.45), Some(0.4)),
    word("sea", Some(200.0), Some(0.65), Some(0.5), Some(0.4)),
    word("water", Some(205.0), Some(0.6), Some(0.55), Some(0.4)),
    word("sky", Some(210.0), Some(0.6), Some(0.68), Some(0.3)),
    word("storm", Some(230.0), Some(0.4), Some(0.38), Some(0.7)),
    word("forest", Some(140.0), Some(0.55), Some(0.38), Some(0.25)),
    word("jungle", Some(130.0), Some(0.7), Some(0.4), Some(0.4)),
    word("moss", Some(110.0), Some(0.45), Some(0.42), Some(0.15)),
    word("toxic", Some(90.0), Some(0.95), Some(0.55), Some(0.75)),
    word("venom", Some(100.0), Some(0.9), Some(0.48), Some(0.7)),
    word("poison", Some(95.0), Some(0.88), Some(0.5), Some(0.65)),
    word("rose", Some(350.0), Some(0.7), Some(0.62), Some(0.35)),
    word("candy", Some(330.0), Some(0.85), Some(0.7), Some(0.6)),
    word("berry", Some(320.0), Some(0.75), Some(0.45), Some(0.4)),
    word("violet", Some(295.0), Some(0.7), Some(0.55), Some(0.35)),
    word("royal", Some(275.0), Some(0.7), Some(0.42), Some(0.3)),
    word("cosmic", Some(280.0), Some(0.75), Some(0.35), Some(0.6)),
    word("void", Some(270.0), Some(0.35), Some(0.22), Some(0.35)),
    word("night", Some(245.0), Some(0.4), Some(0.25), Some(0.3)),
    word("shadow", Some(260.0), Some(0.3), Some(0.26), Some(0.3)),
    word("steel", Some(215.0), Some(0.18), Some(0.5), Some(0.2)),
    word("iron", Some(220.0), Some(0.14), Some(0.42), Some(0.15)),
    word("chrome", Some(205.0), Some(0.16), Some(0.66), Some(0.5)),
    word("silver", Some(210.0), Some(0.12), Some(0.72), Some(0.4)),
    word("bone", Some(45.0), Some(0.2), Some(0.82), Some(0.15)),
    word("sand", Some(40.0), Some(0.4), Some(0.7), Some(0.15)),
    word("desert", Some(35.0), Some(0.5), Some(0.62), Some(0.2)),
    word("neon", None, Some(1.0), Some(0.6), Some(0.85)),
    word("cyber", Some(300.0), Some(0.95), Some(0.5), Some(0.85)),
    word("laser", Some(320.0), Some(1.0), Some(0.58), Some(0.9)),
    word("plasma", Some(290.0), Some(0.95), Some(0.55), Some(0.9)),
    word("electric", Some(200.0), Some(0.95), Some(0.6), Some(0.9)),
    word("neonatal", None, None, None, None),
    // Modifiers, which move a dial without claiming a hue.
    word("dark", None, None, Some(0.28), None),
    word("deep", None, None, Some(0.32), None),
    word("black", None, Some(0.12), Some(0.18), None),
    word("light", None, None, Some(0.78), None),
    word("pale", None, Some(0.3), Some(0.8), None),
    word("pastel", None, Some(0.35), Some(0.8), Some(0.25)),
    word("bright", None, Some(0.9), Some(0.62), None),
    word("vivid", None, Some(0.95), Some(0.58), None),
    word("muted", None, Some(0.25), None, None),
    word("faded", None, Some(0.22), Some(0.65), Some(0.15)),
    word("matte", None, Some(0.3), None, Some(0.1)),
    word("calm", None, None, None, Some(0.12)),
    word("quiet", None, None, None, Some(0.1)),
    word("slow", None, None, None, Some(0.15)),
    word("fast", None, None, None, Some(0.9)),
    word("wild", None, Some(0.9), None, Some(0.9)),
    word("angry", Some(8.0), Some(0.9), Some(0.42), Some(0.85)),
    word("hot", None, Some(0.9), None, Some(0.7)),
    word("cold", None, Some(0.45), Some(0.7), Some(0.25)),
];

/// The shapes a body can wear. Every one is assembled from layers the shipped
/// skins already use, which is what keeps the budget and the gates predictable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Motif {
    /// Nothing but body and outline. The quietest thing a skin can be, and the
    /// right answer for a prompt that asked for a colour and not a pattern.
    Plain,
    /// Two lanes of squares half a period apart.
    Board,
    /// One band across the body: a racing stripe.
    Stripe,
    /// Rings around the body at a regular interval.
    Bands,
}

impl Motif {
    const ALL: [Motif; 4] = [Motif::Plain, Motif::Board, Motif::Stripe, Motif::Bands];

    fn name(self) -> &'static str {
        match self {
            Motif::Plain => "plain",
            Motif::Board => "board",
            Motif::Stripe => "stripe",
            Motif::Bands => "bands",
        }
    }
}

/// A tiny deterministic generator. SplitMix64 — small, well distributed, and
/// reproducible across platforms, which a `HashMap`-derived hash is not.
struct Rolls(u64);

impl Rolls {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9e37_79b9_7f4a_7c15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A float in 0..1.
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A float in `low..high`.
    fn range(&mut self, low: f64, high: f64) -> f64 {
        low + self.unit() * (high - low)
    }

    fn pick<'a, T>(&mut self, from: &'a [T]) -> &'a T {
        &from[(self.next() % from.len() as u64) as usize]
    }

    fn chance(&mut self, probability: f64) -> bool {
        self.unit() < probability
    }
}

/// Hash a string to a stable number, so an unrecognised prompt still steers.
fn digest(text: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// Read the words for a mood.
fn read_mood(text: &str) -> Mood {
    let lower = text.to_lowercase();
    let mut mood = Mood::default();
    let mut matched = false;

    for token in lower.split(|c: char| !c.is_ascii_alphanumeric()) {
        if token.is_empty() {
            continue;
        }
        for entry in WORDS {
            // Prefix either way, so "icy" finds "ice" and "ice" finds "ice".
            if !token.starts_with(entry.stem) && !entry.stem.starts_with(token) {
                continue;
            }
            // A one- or two-letter token matching a stem's opening is a
            // coincidence, not a word.
            if token.len() < 3 {
                continue;
            }
            matched = true;
            if let Some(hue) = entry.hue {
                mood.hue = hue;
            }
            if let Some(chroma) = entry.chroma {
                mood.chroma = chroma;
            }
            if let Some(lightness) = entry.lightness {
                mood.lightness = lightness;
            }
            if let Some(energy) = entry.energy {
                mood.energy = energy;
            }
        }
    }

    // Nothing recognised: the text still decides, it just decides arbitrarily.
    // Better than handing every unknown prompt the same default blue.
    if !matched && !lower.trim().is_empty() {
        let seed = digest(&lower);
        mood.hue = (seed % 360) as f64;
        mood.chroma = 0.45 + ((seed >> 9) % 50) as f64 / 100.0;
        mood.lightness = 0.38 + ((seed >> 19) % 34) as f64 / 100.0;
        mood.energy = ((seed >> 29) % 80) as f64 / 100.0;
    }

    mood
}

/// Pull a mood out of colours sampled from a reference image.
fn mood_from_colors(colors: &[String]) -> Option<Mood> {
    let parsed: Vec<Rgb> = colors.iter().filter_map(|hex| Rgb::parse(hex)).collect();
    if parsed.is_empty() {
        return None;
    }

    // Average the hues as unit vectors; averaging degrees would put the mean of
    // 350 and 10 at 180, which is the opposite colour.
    let (mut x, mut y, mut chroma, mut lightness) = (0.0, 0.0, 0.0, 0.0);
    for color in &parsed {
        let (hue, c) = color.oklch_hue_chroma();
        let weight = c.max(0.02);
        x += hue.to_radians().cos() * weight;
        y += hue.to_radians().sin() * weight;
        chroma += c;
        lightness += (color.r + color.g + color.b) / 3.0;
    }
    let count = parsed.len() as f64;

    Some(Mood {
        hue: y.atan2(x).to_degrees().rem_euclid(360.0),
        // OKLCH chroma tops out around 0.37 for sRGB; scale it to the 0..1 dial.
        chroma: (chroma / count / 0.33).clamp(0.1, 1.0),
        lightness: (lightness / count).clamp(0.12, 0.9),
        energy: 0.45,
    })
}

/// Blend two moods, `weight` being how much of `other` to take.
fn blend(base: Mood, other: Mood, weight: f64) -> Mood {
    let angle = |a: f64, b: f64| {
        let (ax, ay) = (a.to_radians().cos(), a.to_radians().sin());
        let (bx, by) = (b.to_radians().cos(), b.to_radians().sin());
        let (x, y) = (
            ax * (1.0 - weight) + bx * weight,
            ay * (1.0 - weight) + by * weight,
        );
        y.atan2(x).to_degrees().rem_euclid(360.0)
    };
    let mix = |a: f64, b: f64| a * (1.0 - weight) + b * weight;
    Mood {
        hue: angle(base.hue, other.hue),
        chroma: mix(base.chroma, other.chroma),
        lightness: mix(base.lightness, other.lightness),
        energy: mix(base.energy, other.energy),
    }
}

/// The centre of a hue window, going the short way round the wheel.
fn window_center(window: HueWindow) -> f64 {
    if window.from <= window.to {
        (window.from + window.to) / 2.0
    } else {
        ((window.from + window.to + 360.0) / 2.0).rem_euclid(360.0)
    }
}

/// The mood's hue, pulled into a window it is allowed to occupy.
///
/// A role with a hue window cannot wear an arbitrary theme colour — a friendly
/// snake is cool or it is not friendly — so what carries across is the mood's
/// *character*: how saturated, how light, and where inside its own window it
/// leans. A warm theme takes the warm end of the cool band.
fn steer(mood_hue: f64, window: HueWindow) -> f64 {
    let center = window_center(window);
    let span = if window.from <= window.to {
        window.to - window.from
    } else {
        window.to + 360.0 - window.from
    };
    // Where the mood sits relative to the window's centre, as -1..1.
    let offset = (((mood_hue - center + 540.0).rem_euclid(360.0)) - 180.0) / 180.0;
    // Lean, never leave: two fifths of the half-span keeps a comfortable margin
    // inside the window even after OKLCH disagrees with HSL about where the
    // boundary is.
    (center + offset * span * 0.4).rem_euclid(360.0)
}

/// Make a colour that reads at `target_hue` *in OKLCH*, by nudging the HSL hue
/// until it does.
///
/// HSL and OKLCH do not agree about where a hue is — that disagreement is the
/// whole reason this crate judges hue in OKLCH — so a colour built at HSL 200
/// may measure at OKLCH 230. Rather than invert the transform, this walks: it
/// asks what came out and corrects, which converges in a handful of steps and
/// cannot produce a colour it has not measured.
fn colour_at(target_hue: f64, saturation: f64, lightness: f64) -> Rgb {
    let mut hsl_hue = target_hue;
    let mut best = Rgb::from_hsl(hsl_hue, saturation, lightness);
    let mut best_error = f64::MAX;

    for _ in 0..24 {
        let candidate = Rgb::from_hsl(hsl_hue, saturation, lightness);
        let (measured, _) = candidate.oklch_hue_chroma();
        let error = ((measured - target_hue + 540.0).rem_euclid(360.0)) - 180.0;
        if error.abs() < best_error {
            best_error = error.abs();
            best = candidate;
        }
        if error.abs() < 0.5 {
            break;
        }
        hsl_hue = (hsl_hue - error * 0.7).rem_euclid(360.0);
    }
    best
}

/// The lightness a body has to clear for its label to survive the head glow.
///
/// The label ink is *derived from the authored fill* and then read against
/// whatever the layers composite to. A dark fill therefore picks white ink —
/// and the head glow promptly lightens the body out from under it, which is
/// the exact weak spot the shipped classic skin carries an exemption for. The
/// way out is not to darken further but to stay above the crossover, where the
/// ink comes out dark and a lightening glow only ever helps. Every shipped
/// skin sits here too: classic measures 0.46, Harlequin 0.31, Pitlane 0.35.
const MIN_FILL_LUMINANCE: f64 = 0.3;

/// Build a fill at this hue that clears the readability floor.
///
/// Raising *lightness* rather than blending toward white, which is the
/// difference between molten lava and dusty pink: `shift_lightness` moves every
/// channel toward 1.0, so it washes the colour out on its way up, while asking
/// HSL for the same hue at a higher lightness keeps the chroma. A prompt that
/// asked for lava should get something that still looks hot after it has been
/// made readable.
fn readable_fill(hue: f64, saturation: f64, lightness: f64) -> Rgb {
    let mut level = lightness;
    let mut candidate = colour_at(hue, saturation, level);
    // Sixteen steps of 0.03 reaches 0.48 above the starting lightness, which is
    // past white for any hue; the loop ends on the floor long before that.
    for _ in 0..16 {
        if candidate.relative_luminance() >= MIN_FILL_LUMINANCE {
            break;
        }
        level = (level + 0.03).min(0.95);
        candidate = colour_at(hue, saturation, level);
    }
    // A hue whose full-chroma form simply cannot reach the floor — a deep blue
    // is the case — gets its saturation traded away only as a last resort, and
    // only as far as it takes.
    let mut washed = saturation;
    for _ in 0..12 {
        if candidate.relative_luminance() >= MIN_FILL_LUMINANCE {
            break;
        }
        washed *= 0.85;
        candidate = colour_at(hue, washed, level);
    }
    candidate
}

/// A fill/outline/accent triple for one role.
fn pair_for(hue: f64, mood: Mood, rolls: &mut Rolls, accent_shift: f64) -> ColorPair {
    let saturation = (mood.chroma * 0.78).clamp(0.12, 0.92);
    let lightness = mood.lightness.clamp(0.3, 0.74);
    let fill = readable_fill(hue, saturation, lightness);
    // The outline is the fill darkened, always: an outline lighter than its
    // body stops reading as an edge.
    let outline = colour_at(
        hue,
        (saturation * 1.05).clamp(0.1, 1.0),
        (lightness * 0.62).clamp(0.08, 0.6),
    );
    // The accent stays on the fill's side of the wheel — Harlequin's rule, and
    // the thing that keeps a checked skin from wearing two teams' colours.
    let accent = colour_at(
        (hue + accent_shift).rem_euclid(360.0),
        (saturation * rolls.range(0.7, 1.0)).clamp(0.1, 0.95),
        (lightness + rolls.range(0.16, 0.3)).clamp(0.35, 0.92),
    );

    ColorPair {
        fill: fill.to_hex(),
        outline: outline.to_hex(),
        accent: Some(accent.to_hex()),
    }
}

/// Build the eight-slot palette for a mood.
fn palette_for(mood: Mood, rolls: &mut Rolls) -> RolePalette {
    let enemy_window = crate::color::ENEMY_HUES;
    let friendly = steer(mood.hue, FRIENDLY_HUES);
    let enemy = steer(mood.hue, enemy_window);
    // Two shades per side: the second is the same colour, deeper, which is
    // what tells a 2v2 partner from you without inventing a third hue.
    let deep = Mood {
        lightness: (mood.lightness * 0.78).clamp(0.22, 0.66),
        ..mood
    };
    let shift = rolls.range(24.0, 52.0) * if rolls.chance(0.5) { 1.0 } else { -1.0 };

    RolePalette {
        friendly: [
            pair_for(friendly, mood, rolls, shift),
            pair_for(friendly, deep, rolls, shift),
        ],
        enemy: [
            pair_for(enemy, mood, rolls, shift),
            pair_for(enemy, deep, rolls, shift),
        ],
        free_for_all: [
            pair_for(friendly, mood, rolls, shift),
            pair_for(enemy, mood, rolls, shift),
            // The two unwindowed slots are the only place the theme's own hue
            // can appear literally, so it does: this is where "molten lava"
            // actually gets to be orange.
            pair_for(
                (mood.hue + 180.0).rem_euclid(360.0),
                Mood {
                    chroma: (mood.chroma * 0.5).clamp(0.08, 0.5),
                    lightness: (mood.lightness + 0.2).clamp(0.45, 0.8),
                    ..mood
                },
                rolls,
                shift,
            ),
            pair_for(mood.hue, mood, rolls, shift),
        ],
    }
}

/// A layer with the fields every layer carries, so callers only state what
/// makes theirs different.
fn layer(name: &str, opacity: &str, body: LayerBodyV2) -> LayerV2 {
    LayerV2 {
        name: name.to_string(),
        boost_only: false,
        omit_on_single_cell: false,
        opacity: PropExpr(opacity.to_string()),
        transform: TransformV2::default(),
        body,
    }
}

fn ribbon(name: &str, region: RegionV2, slot: SlotName, extra_px: f64, tail_cap: bool) -> LayerV2 {
    layer(
        name,
        "1",
        LayerBodyV2::Ribbon {
            region,
            color: ColorRef::slot(slot),
            extra_px,
            joints: true,
            tail_cap,
        },
    )
}

/// One lane of a board, or a single ring if `duty` is small.
fn band(
    name: &str,
    t_center: f64,
    half_width: f64,
    period: f64,
    duty: f64,
    phase: f64,
    lighten: Option<String>,
) -> LayerV2 {
    let mut band = layer(
        name,
        "1",
        LayerBodyV2::Span {
            region: RegionV2::Body,
            clip: ClipV2::Silhouette,
            span: SpanV2::whole(),
            corner: CornerV2::Fan,
            source: SourceV2::Band {
                color: ColorRef {
                    target: ColorTarget::Slot {
                        slot: SlotName::Accent,
                    },
                    lighten: lighten.map(PropExpr),
                },
                period_cells: period,
                duty,
                phase_cells: phase * period,
                half_width: PropExpr(format!("{half_width}")),
                t_center: PropExpr(format!("{t_center}")),
                alpha: None,
            },
        },
    );
    // Half a cell of pattern on a one-cell snake is a smudge the head cap
    // covers anyway.
    band.omit_on_single_cell = true;
    band
}

/// A travelling highlight, as three stops whose positions read the clock.
fn gleam(name: &str, width: f64, alpha: f64, easing: &str, literal: &str) -> LayerV2 {
    let stop = |offset: String, target: ColorTarget, alpha: &str| StopV2 {
        offset: PropExpr(offset),
        color: ColorRef {
            target,
            lighten: None,
        },
        alpha: PropExpr(alpha.to_string()),
    };
    layer(
        name,
        "1",
        LayerBodyV2::Span {
            region: RegionV2::Body,
            clip: ClipV2::Silhouette,
            span: SpanV2::whole(),
            corner: CornerV2::Fan,
            source: SourceV2::Gradient {
                axis: GradientAxis::AlongBody,
                stops: vec![
                    stop(
                        format!("{easing} - {width}"),
                        ColorTarget::Slot {
                            slot: SlotName::Fill,
                        },
                        "0",
                    ),
                    stop(
                        easing.to_string(),
                        ColorTarget::Literal {
                            literal: literal.to_string(),
                        },
                        &format!("{alpha}"),
                    ),
                    stop(
                        format!("{easing} + {width}"),
                        ColorTarget::Slot {
                            slot: SlotName::Fill,
                        },
                        "0",
                    ),
                ],
            },
        },
    )
}

/// A shadow and a highlight rolling along the body in the snake's own colour.
fn sheen(name: &str, easing: &str, dark: f64, light: f64) -> LayerV2 {
    let lit = |offset: String, lighten: f64, alpha: f64| StopV2 {
        offset: PropExpr(offset),
        color: ColorRef {
            target: ColorTarget::Slot {
                slot: SlotName::Fill,
            },
            lighten: Some(PropExpr(format!("{lighten}"))),
        },
        alpha: PropExpr(format!("{alpha}")),
    };
    layer(
        name,
        "1",
        LayerBodyV2::Span {
            region: RegionV2::Body,
            clip: ClipV2::Silhouette,
            span: SpanV2::whole(),
            corner: CornerV2::Fan,
            source: SourceV2::Gradient {
                axis: GradientAxis::AlongBody,
                stops: vec![
                    lit(format!("{easing} - 0.44"), -dark, 0.0),
                    // The shadow outweighs the highlight on purpose: the
                    // sampler judges a gradient by its strongest stop and keeps
                    // the last of equals, so a tie would quietly move it onto
                    // the direction that cannot fail a contrast floor.
                    lit(format!("{easing} - 0.22"), -dark, 0.42),
                    lit(easing.to_string(), 0.0, 0.0),
                    lit(format!("{easing} + 0.22"), light, 0.4),
                    lit(format!("{easing} + 0.44"), light, 0.0),
                ],
            },
        },
    )
}

/// The white fall-off toward the head that every shipped skin wears.
fn head_glow(length: f64, peak: f64) -> LayerV2 {
    let mut glow = layer(
        "Head glow",
        &format!("(1 - s / {length}) * {peak}"),
        LayerBodyV2::HeadRamp {
            color: "#ffffff".to_string(),
            length_cells: length,
        },
    );
    glow.omit_on_single_cell = true;
    glow
}

/// Assemble one candidate. Nothing here consults the validator — [`variations`]
/// does that, and repairs what it can.
fn compose(mood: Mood, motif: Motif, rolls: &mut Rolls) -> SkinDocV2 {
    let mut literals = BTreeMap::new();
    literals.insert("gleam".to_string(), "#ffffff".to_string());

    let mut layers = vec![
        ribbon("Outline", RegionV2::Contour, SlotName::Outline, 2.0, false),
        ribbon("Body", RegionV2::Body, SlotName::Fill, 0.0, true),
    ];

    // How fast anything moves. `sin(tau * time)` is one cycle a turn, which is
    // as fast as the 32-step ring can carry without aliasing, so tempo lives in
    // the period rather than in the expression.
    let period_ms = (3400.0 - mood.energy * 2000.0).clamp(900.0, 6000.0);

    match motif {
        Motif::Plain => {}
        Motif::Board => {
            let period = *rolls.pick(&[1.0, 2.0]);
            // Harlequin's see-saw: the two lanes trade lightness in antiphase,
            // so the board's colours keep swapping dominance.
            let swing = 0.1 + mood.energy * 0.12;
            layers.push(band(
                "Board, upper",
                -0.25,
                0.25,
                period,
                0.5,
                0.0,
                Some(format!("{swing} * sin(tau * time)")),
            ));
            layers.push(band(
                "Board, lower",
                0.25,
                0.25,
                period,
                0.5,
                0.5,
                Some(format!("{swing} * sin(tau * (time + 0.5))")),
            ));
        }
        Motif::Stripe => {
            // One lane down the middle, wide enough to read as a stripe rather
            // than a seam.
            layers.push(band(
                "Stripe",
                0.0,
                rolls.range(0.16, 0.3),
                1.0,
                1.0,
                0.0,
                None,
            ));
        }
        Motif::Bands => {
            let period = rolls.range(2.0, 4.0);
            layers.push(band(
                "Bands",
                0.0,
                0.5,
                period,
                rolls.range(0.28, 0.45),
                0.0,
                None,
            ));
        }
    }

    // Movement, in rough order of how loud it is.
    if mood.energy > 0.3 {
        let easing = if mood.energy > 0.62 {
            "tri(time) * tri(time)"
        } else {
            "saw(time)"
        };
        layers.push(sheen(
            "Sheen",
            "tri(time + 0.25)",
            rolls.range(0.2, 0.34),
            rolls.range(0.3, 0.45),
        ));
        layers.push(gleam(
            "Shine",
            rolls.range(0.1, 0.2),
            (0.08 + mood.energy * 0.3).clamp(0.08, 0.4),
            easing,
            "gleam",
        ));
    }

    layers.push(head_glow(10.0, 0.3));

    SkinDocV2 {
        schema_version: SCHEMA_VERSION_V2,
        id: "draft@1".to_string(),
        // Named by the caller; a generator that titles its own work tends to
        // title it badly.
        name: "Generated skin".to_string(),
        palette: palette_for(mood, rolls),
        labels: LabelStyle::default(),
        base: None,
        celebration: None,
        literals,
        textures: Vec::new(),
        period_ms,
        head_core: HeadCoreV2 {
            ratio: 0.38,
            color: "#333333".to_string(),
        },
        layers,
    }
}

/// Nudge a document toward passing, given what the validator objected to.
///
/// Only two failures are worth repairing, and they are the two a colour choice
/// can cause: a label that cannot be read against the body, and a role whose
/// composited colour has drifted out of its hue window. Everything else is a
/// structural mistake in [`compose`], and papering over one of those would hide
/// a bug rather than fix a skin.
fn repair(doc: &mut SkinDocV2, errors: &[SkinDocError]) -> bool {
    let mut changed = false;
    for error in errors {
        let Some(index) = error.field.strip_prefix("palette.") else {
            continue;
        };
        let Some((group, at)) = index.split_once('[') else {
            continue;
        };
        let Ok(at) = at.trim_end_matches(']').parse::<usize>() else {
            continue;
        };
        let pair = match group {
            "friendly" => doc.palette.friendly.get_mut(at),
            "enemy" => doc.palette.enemy.get_mut(at),
            "free_for_all" => doc.palette.free_for_all.get_mut(at),
            _ => None,
        };
        let Some(pair) = pair else { continue };

        // Always upward. Below the crossover the ink is white and every
        // lightening layer erodes it; above, the ink is dark and lightening
        // only ever helps. There is no direction to choose.
        let Some(fill) = Rgb::parse(&pair.fill) else {
            continue;
        };
        pair.fill = crate::shift_lightness(fill, 0.1).to_hex();
        changed = true;
    }
    changed
}

/// The result of one attempt, so a caller can show what it got.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Variation {
    /// A valid document, ready for the Builder or the renderer.
    pub document: SkinDocV2,
    /// A short name suggested from the prompt, for the Builder's name field.
    pub suggested_name: String,
    /// Which motif it wore, so a caller can say "more like this one".
    pub motif: &'static str,
    /// The seed that produced it, so it can be reproduced exactly.
    pub seed: u64,
}

/// Produce `count` distinct, valid skins for a brief.
///
/// Every document returned has passed [`validate_v2`]. Candidates that fail are
/// repaired once and re-checked; ones that still fail are dropped, so a caller
/// never has to handle an invalid document and an author never meets a gate
/// error on something they were shown as an option.
pub fn variations(brief: &Brief, count: usize, seed: u64) -> Vec<Variation> {
    let mut mood = read_mood(&format!("{} {}", brief.prompt, brief.guidance));

    // A reference image outvotes the adjectives on colour — it is a far more
    // specific brief — but says nothing about tempo, so energy stays with the
    // words.
    if let Some(sampled) = mood_from_colors(&brief.reference_colors) {
        mood = blend(
            mood,
            Mood {
                energy: mood.energy,
                ..sampled
            },
            0.72,
        );
    }

    // Keeping a skin is the strongest signal there is, so the next round leans
    // on what was kept rather than starting from the prompt again.
    if !brief.liked.is_empty() {
        let kept: Vec<String> = brief
            .liked
            .iter()
            .flat_map(|doc| {
                [
                    doc.palette.free_for_all[3].fill.clone(),
                    doc.palette.friendly[0].fill.clone(),
                ]
            })
            .collect();
        if let Some(from_kept) = mood_from_colors(&kept) {
            mood = blend(
                mood,
                Mood {
                    energy: mood.energy,
                    ..from_kept
                },
                0.6,
            );
        }
    }

    // Motifs the author kept, so a re-roll keeps producing boards if boards are
    // what they liked.
    let liked_motifs: Vec<Motif> = brief
        .liked
        .iter()
        .map(|doc| {
            if doc.layers.iter().any(|l| l.name.starts_with("Board")) {
                Motif::Board
            } else if doc.layers.iter().any(|l| l.name == "Stripe") {
                Motif::Stripe
            } else if doc.layers.iter().any(|l| l.name == "Bands") {
                Motif::Bands
            } else {
                Motif::Plain
            }
        })
        .collect();

    let mut rolls = Rolls::new(seed ^ digest(&brief.prompt) ^ digest(&brief.guidance));
    let mut out = Vec::with_capacity(count);

    // Generous headroom: a candidate can be refused, and a caller asking for
    // six should get six.
    for attempt in 0..count * 12 {
        if out.len() >= count {
            break;
        }
        let candidate_seed = rolls.next();
        let mut local = Rolls::new(candidate_seed);

        // Spread the mood a little per candidate, so a round is a set of
        // options rather than one skin printed six times.
        let spread = Mood {
            hue: (mood.hue + local.range(-22.0, 22.0)).rem_euclid(360.0),
            chroma: (mood.chroma + local.range(-0.12, 0.12)).clamp(0.08, 1.0),
            lightness: (mood.lightness + local.range(-0.1, 0.1)).clamp(0.2, 0.82),
            energy: (mood.energy + local.range(-0.12, 0.12)).clamp(0.0, 1.0),
        };
        let motif = if !liked_motifs.is_empty() && local.chance(0.7) {
            *local.pick(&liked_motifs)
        } else {
            *local.pick(&Motif::ALL)
        };

        let mut doc = compose(spread, motif, &mut local);
        // Propose, then walk toward valid against what the validator actually
        // said. Bounded, because an unbounded loop is a hang rather than a
        // skin, and a candidate that will not converge in six steps is one to
        // drop rather than to keep pushing.
        for _ in 0..6 {
            match validate_v2(&doc) {
                Ok(()) => break,
                Err(errors) => {
                    if !repair(&mut doc, &errors) {
                        break;
                    }
                }
            }
        }
        if validate_v2(&doc).is_err() {
            continue;
        }

        doc.name = suggest_name(&brief.prompt, motif, attempt);
        out.push(Variation {
            suggested_name: doc.name.clone(),
            document: doc,
            motif: motif.name(),
            seed: candidate_seed,
        });
    }

    out
}

/// A short, human name from the prompt.
///
/// The prompt's own words wherever there are any — an author who typed "molten
/// lava" would rather see "Molten Lava" than a generated compound noun — with
/// a stock pairing only when they typed nothing at all.
fn suggest_name(prompt: &str, motif: Motif, index: usize) -> String {
    const NOUNS: &[&str] = &[
        "Drift", "Signal", "Circuit", "Cinder", "Tide", "Vector", "Prism", "Relay",
    ];

    let words: Vec<String> = prompt
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| word.len() > 2)
        .take(2)
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect();

    if words.is_empty() {
        let noun = NOUNS[index % NOUNS.len()];
        return match motif {
            Motif::Plain => noun.to_string(),
            Motif::Board => format!("{noun} Board"),
            Motif::Stripe => format!("{noun} Stripe"),
            Motif::Bands => format!("{noun} Bands"),
        };
    }

    let base = words.join(" ");
    if index == 0 {
        base
    } else {
        // Later variations of the same prompt need distinct names, and a bare
        // number reads as a version rather than a variant.
        format!("{base} {}", NOUNS[index % NOUNS.len()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{MAX_OPS_PER_SNAKE, predict_ops};

    /// The whole contract in one line: a caller never has to check.
    ///
    /// A generator that can hand back an invalid document is worse than no
    /// generator, because the author meets the gate later, on something they
    /// were shown as a finished option. Everything else here is taste; this is
    /// the guarantee.
    #[test]
    fn every_variation_is_a_document_that_validates() {
        let prompts = [
            "molten lava",
            "arctic frost",
            "neon cyber city",
            "dark royal velvet",
            "toxic venom",
            "pale bone desert",
            "",
            "asdkjhqwe nonsense words",
            "🐍🔥 emoji only",
            "a very long prompt that goes on and on about nothing in particular \
             and contains no recognised mood words whatsoever, just filler",
        ];

        for prompt in prompts {
            let brief = Brief {
                prompt: prompt.to_string(),
                ..Brief::default()
            };
            let produced = variations(&brief, 6, 12345);
            assert_eq!(
                produced.len(),
                6,
                "prompt {prompt:?} produced {} variations, not six",
                produced.len()
            );
            for variation in produced {
                if let Err(errors) = validate_v2(&variation.document) {
                    panic!("prompt {prompt:?} produced an invalid document: {errors:?}");
                }
                assert!(
                    predict_ops(&variation.document) <= MAX_OPS_PER_SNAKE,
                    "prompt {prompt:?} produced a document over budget"
                );
                assert!(!variation.suggested_name.trim().is_empty());
            }
        }
    }

    /// Same brief, same seed, same skins — which is what makes "regenerate"
    /// mean "show me different ones" rather than "roll again and hope".
    #[test]
    fn generation_is_reproducible_from_its_seed() {
        let brief = Brief {
            prompt: "storm over steel".to_string(),
            ..Brief::default()
        };
        let first = variations(&brief, 4, 99);
        let again = variations(&brief, 4, 99);
        let different_seed = variations(&brief, 4, 100);

        for (a, b) in first.iter().zip(&again) {
            assert_eq!(a.document, b.document, "the same seed drifted");
        }
        assert!(
            first
                .iter()
                .zip(&different_seed)
                .any(|(a, b)| a.document != b.document),
            "a different seed produced the same set"
        );
    }

    /// A round is a set of options, not one skin printed six times.
    #[test]
    fn a_round_offers_genuinely_different_skins() {
        let brief = Brief {
            prompt: "ocean".to_string(),
            ..Brief::default()
        };
        let produced = variations(&brief, 6, 7);
        let fills: std::collections::BTreeSet<String> = produced
            .iter()
            .map(|v| v.document.palette.free_for_all[3].fill.clone())
            .collect();
        assert!(
            fills.len() >= 4,
            "six variations only offered {} distinct theme colours",
            fills.len()
        );
    }

    /// The words have to actually steer, or the prompt box is decoration.
    #[test]
    fn the_prompt_moves_the_colours_it_is_allowed_to_move() {
        let hue_of = |prompt: &str| {
            let brief = Brief {
                prompt: prompt.to_string(),
                ..Brief::default()
            };
            let produced = variations(&brief, 1, 4);
            let doc = &produced.first().expect("a variation").document;
            Rgb::parse(&doc.palette.free_for_all[3].fill)
                .expect("valid hex")
                .oklch_hue_chroma()
                .0
        };

        // The unwindowed slot is where a theme gets to be its own colour, so
        // that is where warm and cold have to actually differ.
        let warm = hue_of("molten lava fire");
        let cold = hue_of("arctic ice frost");
        let apart = ((warm - cold + 540.0).rem_euclid(360.0) - 180.0).abs();
        assert!(
            apart > 60.0,
            "lava at {warm:.0}deg and ice at {cold:.0}deg are only {apart:.0}deg apart"
        );
    }

    /// A reference image is a better palette brief than a sentence, and it has
    /// to beat one.
    #[test]
    fn reference_colours_outvote_the_adjectives() {
        let words_only = Brief {
            prompt: "arctic ice".to_string(),
            ..Brief::default()
        };
        let with_image = Brief {
            prompt: "arctic ice".to_string(),
            // A picture of something unmistakably orange.
            reference_colors: vec![
                "#ff7a18".to_string(),
                "#e2590b".to_string(),
                "#ffb066".to_string(),
            ],
            ..Brief::default()
        };

        let hue = |brief: &Brief| {
            let produced = variations(brief, 1, 21);
            let doc = &produced.first().expect("a variation").document;
            Rgb::parse(&doc.palette.free_for_all[3].fill)
                .expect("valid hex")
                .oklch_hue_chroma()
                .0
        };
        let cold = hue(&words_only);
        let warmed = hue(&with_image);
        let moved = ((cold - warmed + 540.0).rem_euclid(360.0) - 180.0).abs();
        assert!(
            moved > 45.0,
            "the reference image moved the hue only {moved:.0}deg, from {cold:.0} to {warmed:.0}"
        );
    }

    /// Keeping one is the strongest signal an author can give, so the next
    /// round has to stay near it.
    #[test]
    fn keeping_a_skin_pulls_the_next_round_toward_it() {
        let opening = Brief {
            prompt: "forest".to_string(),
            ..Brief::default()
        };
        let kept = variations(&opening, 4, 3)
            .into_iter()
            .max_by(|a, b| {
                let hue = |v: &Variation| {
                    Rgb::parse(&v.document.palette.free_for_all[3].fill)
                        .expect("valid hex")
                        .oklch_hue_chroma()
                        .0
                };
                hue(a).partial_cmp(&hue(b)).expect("comparable")
            })
            .expect("a variation");
        let target = Rgb::parse(&kept.document.palette.free_for_all[3].fill)
            .expect("valid hex")
            .oklch_hue_chroma()
            .0;

        let next = Brief {
            prompt: "forest".to_string(),
            liked: vec![kept.document.clone()],
            ..Brief::default()
        };
        for variation in variations(&next, 4, 88) {
            let hue = Rgb::parse(&variation.document.palette.free_for_all[3].fill)
                .expect("valid hex")
                .oklch_hue_chroma()
                .0;
            let apart = ((hue - target + 540.0).rem_euclid(360.0) - 180.0).abs();
            assert!(
                apart < 90.0,
                "a re-roll around a kept skin landed {apart:.0}deg away from it"
            );
        }
    }

    /// Names come from the author's own words where there are any.
    #[test]
    fn names_read_like_the_prompt_and_never_repeat_within_a_round() {
        let brief = Brief {
            prompt: "molten lava".to_string(),
            ..Brief::default()
        };
        let produced = variations(&brief, 5, 6);
        assert_eq!(produced[0].suggested_name, "Molten Lava");
        let names: std::collections::BTreeSet<&str> =
            produced.iter().map(|v| v.suggested_name.as_str()).collect();
        assert_eq!(names.len(), produced.len(), "two variations shared a name");

        let unprompted = variations(&Brief::default(), 3, 6);
        for variation in &unprompted {
            assert!(!variation.suggested_name.trim().is_empty());
        }
    }
}
