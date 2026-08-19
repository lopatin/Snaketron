//! Checking what a v2 skin *looks like*, not just what it says.
//!
//! v1 could check readability analytically because it knew the whole structure
//! in advance: there was one body colour, one glow over it, and the label sat
//! on the two of them. A v2 document can put anything anywhere, so the same
//! questions — can you read the carried-food number, can you find the head, does
//! a friendly snake read as friendly — can only be answered by working out what
//! is actually painted.
//!
//! So this composites. Not pixels: **points**, in body space, at the handful of
//! places the rules actually bind, for every animation step and every role. That
//! is enough because the rules are local (a label sits in one place) and the
//! clock is a ring of 32 steps rather than a continuum, so "every frame a viewer
//! could see" is a finite list.
//!
//! ## What this is and is not
//!
//! It is a **model** of the compositor, not a copy of it, and the difference
//! matters. It knows layer order, colour resolution, opacity and coverage; it
//! does not know capsule geometry, antialiasing, or clip shapes. Where it is
//! unsure whether a layer covers a point it assumes it **does**, which makes the
//! composite busier than reality and the verdict stricter — the safe direction,
//! because the failure that matters is passing a skin whose label cannot be
//! read.
//!
//! It is also not an art critic. It makes *unreadable* unpublishable; whether a
//! skin is ugly, or hostile in some way a number cannot see, stays with the
//! human review queue.

use crate::color::{ENEMY_HUES, FRIENDLY_HUES, HueWindow, NEUTRAL_CHROMA, Rgb, contrast_ratio};
use crate::expr::Env;
use crate::v2::{
    ColorRef, ColorTarget, LayerBodyV2, LayerV2, RegionV2, SkinDocV2, SlotName, SourceV2,
};
use crate::{
    ANIMATION_STEPS, ColorPair, MIN_HEAD_CORE_CONTRAST, MIN_LABEL_CONTRAST, SkinDocError,
    derive_label_ink, shift_lightness,
};

/// Where along the body the roster name and the carried-food digits sit.
///
/// Two cells behind the head, which is where the arena queues them today. The
/// span rather than the single point, because a digit is a glyph with width and
/// the body under it is not one colour.
const LABEL_SAMPLES_S: [f64; 4] = [1.5, 2.0, 2.5, 3.0];

/// How far along the body the "what side is this" reading is taken.
///
/// Spread down the body rather than concentrated near the head, because head
/// dressing is small and local while what a player reads at a glance is the
/// length of the snake.
const SIDE_SAMPLES_S: [f64; 6] = [1.0, 4.0, 8.0, 12.0, 16.0, 20.0];

/// How much of a cell one glyph's ink actually covers.
///
/// Measured from the strip `client/design/tools/build_glyph_atlas.py` writes:
/// 569 opaque pixels across 41 glyphs of 16x16 each. Kept as a number here
/// rather than read from the PNG, because this crate has no image decoder and
/// deliberately no dependency that would give it one.
const TEXT_INK_COVERAGE: f64 = 569.0 / (41.0 * 16.0 * 16.0);

/// One resolved colour with an opacity, ready to composite.
struct Paint {
    color: Rgb,
    alpha: f64,
}

/// Composite `over`, the only blend the per-frame path uses.
fn over(source: &Paint, destination: Rgb) -> Rgb {
    let a = source.alpha.clamp(0.0, 1.0);
    Rgb {
        r: source.color.r * a + destination.r * (1.0 - a),
        g: source.color.g * a + destination.g * (1.0 - a),
        b: source.color.b * a + destination.b * (1.0 - a),
    }
}

/// Resolve a document colour reference for one role at one step.
///
/// Mirrors the compiler's own resolution, deliberately: if these two ever
/// disagreed, the sampler would be judging a skin nobody is going to see.
fn resolve(reference: &ColorRef, pair: &ColorPair, doc: &SkinDocV2, time: f64) -> Option<Rgb> {
    let base = match &reference.target {
        ColorTarget::Slot { slot } => match slot {
            SlotName::Fill => pair.fill.clone(),
            SlotName::Outline => pair.outline.clone(),
            SlotName::Accent => pair.accent.clone().unwrap_or_else(|| pair.fill.clone()),
            SlotName::HeadCore => doc.head_core.color.clone(),
        },
        ColorTarget::Literal { literal } => doc.literals.get(literal)?.clone(),
    };
    let rgb = Rgb::parse(&base)?;
    Some(match &reference.lighten {
        None => rgb,
        Some(expr) => {
            let amount = expr
                .parse()
                .map(|parsed| parsed.eval(&env_at(time, 0.0)))
                .unwrap_or(0.0);
            shift_lightness(rgb, amount.clamp(-1.0, 1.0))
        }
    })
}

fn env_at(time: f64, s: f64) -> Env {
    Env {
        s,
        t: 0.0,
        // A reference body long enough that a `len`-reading expression is
        // evaluated somewhere realistic rather than at zero.
        len: 21.0,
        time,
        // Judged not boosting: the Boost band is checked structurally, and a
        // layer that only appears while boosting is extra paint on top of a
        // composite that has already had to be readable without it.
        boost: 0.0,
        seed: 0.0,
    }
}

fn scalar(expr: &crate::v2::PropExpr, time: f64, s: f64, fallback: f64) -> f64 {
    expr.parse()
        .map(|parsed| parsed.eval(&env_at(time, s)))
        .unwrap_or(fallback)
}

/// Whether a layer's paint reaches a point, erring toward "yes".
///
/// Coverage is where a model of a compositor is most likely to be wrong, so
/// every uncertain case resolves to covering. A busier composite is a stricter
/// verdict, and the expensive mistake here is clearing a skin nobody can read.
fn covers(body: &LayerBodyV2, s: f64) -> bool {
    match body {
        LayerBodyV2::Group { .. } => false,
        // A contour ribbon paints outside the silhouette; a body one fills it.
        LayerBodyV2::Ribbon { region, .. } => *region != RegionV2::Contour,
        LayerBodyV2::HeadRamp { length_cells, .. } => s <= *length_cells,
        // Discs are bounded to the head cell.
        LayerBodyV2::HeadDisc { .. } => s <= 0.5,
        LayerBodyV2::Span { span, source, .. } => {
            let reaches = match span.from {
                crate::v2::AnchorV2::Whole => true,
                crate::v2::AnchorV2::Head => span.natural.is_none_or(|natural| s <= natural),
                // A tail-anchored span is not near the head, where the label
                // and the core are, and the side reading samples the length.
                crate::v2::AnchorV2::Tail => s > 8.0,
                crate::v2::AnchorV2::At { at } => {
                    s >= at && span.natural.is_none_or(|natural| s <= at + natural)
                }
                crate::v2::AnchorV2::Fraction { .. } => true,
            };
            // A band paints part of each repeat, and which part depends on
            // where the repeats land — so it is treated as covering wherever
            // its span does. Text is likewise per cell.
            reaches && !matches!(source, SourceV2::Image { .. })
        }
    }
}

/// The colour a layer contributes at a point, if any.
fn paint_of(
    layer: &LayerV2,
    doc: &SkinDocV2,
    pair: &ColorPair,
    time: f64,
    s: f64,
) -> Option<Paint> {
    if !covers(&layer.body, s) {
        return None;
    }
    let layer_alpha = scalar(&layer.opacity, time, s, 1.0).clamp(0.0, 1.0);
    if layer_alpha <= 0.0 {
        return None;
    }

    let (color, alpha) = match &layer.body {
        LayerBodyV2::Group { .. } => return None,
        LayerBodyV2::Ribbon { color, .. } => (resolve(color, pair, doc, time)?, layer_alpha),
        LayerBodyV2::HeadRamp { color, .. } => {
            // The glow's own curve *is* its opacity, evaluated at this point.
            let curve = scalar(&layer.opacity, time, s, 0.0).clamp(0.0, 1.0);
            (Rgb::parse(color)?, curve)
        }
        LayerBodyV2::HeadDisc { paint, .. } => match paint {
            crate::v2::DiscPaintV2::Ref(reference) => {
                (resolve(reference, pair, doc, time)?, layer_alpha)
            }
            // The glow at its peak, which is its curve at the head.
            crate::v2::DiscPaintV2::Named(_) => return None,
        },
        LayerBodyV2::Span { source, .. } => match source {
            SourceV2::Solid { color } => (resolve(color, pair, doc, time)?, layer_alpha),
            SourceV2::Gradient { stops, .. } => {
                // Which stops can reach this point, and how strongly.
                //
                // A stop whose offset is a constant sits at one place on the
                // body, so only samples near it feel it. A stop whose offset
                // reads the clock **travels**, and over a cycle it visits
                // everywhere — so it is felt everywhere, which is exactly what
                // makes a travelling gleam a readability question rather than a
                // decoration. Modelling both as "everywhere" would reject every
                // ordinary head-to-tail fade; modelling both as "only at rest"
                // would clear a gleam that washes the label out twice a second.
                let reach = 1.0 / stops.len().max(2) as f64 + 0.15;
                let strongest = stops
                    .iter()
                    .filter_map(|stop| {
                        let offset_expr = stop.offset.parse().ok()?;
                        let travels = offset_expr.inputs().contains(crate::expr::Input::Time);
                        if !travels {
                            // Where this stop sits along the body, against the
                            // reference length the samples are taken on.
                            let at = offset_expr.eval(&env_at(time, s)).clamp(0.0, 1.0) * 21.0;
                            if (at - s).abs() > reach * 21.0 {
                                return None;
                            }
                        }
                        let alpha = scalar(&stop.alpha, time, s, 1.0).clamp(0.0, 1.0);
                        Some((stop, alpha))
                    })
                    .max_by(|left, right| {
                        left.1
                            .partial_cmp(&right.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })?;
                (
                    resolve(&strongest.0.color, pair, doc, time)?,
                    layer_alpha * strongest.1,
                )
            }
            SourceV2::Band { color, alpha, .. } => (
                resolve(color, pair, doc, time)?,
                layer_alpha
                    * alpha
                        .as_ref()
                        .map_or(1.0, |expr| scalar(expr, time, s, 1.0))
                        .clamp(0.0, 1.0),
            ),
            SourceV2::Text { color, scale, .. } => (
                resolve(color, pair, doc, time)?,
                // A letter is mostly holes. Every other source here covers the
                // stretch it claims, so treating text the same way would have
                // a word behind the head reading as a solid dark band — and
                // would refuse every text skin ever written.
                //
                // The fraction is measured from the strip the generator
                // writes: 569 opaque pixels across 41 glyphs of 16x16, so
                // about 5.4% of a cell is ink. Scale squares it, because
                // shrinking a glyph shrinks it in both axes.
                //
                // This is an *area* model, which is the right answer for what
                // a stretch of body reads as and an approximation for the
                // label sitting directly on a letter. Text under the label is
                // a judgement the review queue sees too.
                layer_alpha * TEXT_INK_COVERAGE * scale.clamp(0.0, 1.0).powi(2),
            ),
            // Pixels this crate does not hold. Deliberately not guessed at:
            // section 7.3 of the PRD makes texture *content* the forge's
            // question and the review queue's, not the sampler's.
            SourceV2::Image { .. } => return None,
        },
    };

    Some(Paint { color, alpha })
}

/// The composited colour at one point on the body.
pub fn sample(doc: &SkinDocV2, pair: &ColorPair, time: f64, s: f64) -> Rgb {
    // The arena's field is what shows through anything the skin leaves
    // unpainted, so it is the ground rather than black.
    let mut result = Rgb::parse("#ffffff").expect("literal");
    for layer in &doc.layers {
        match &layer.body {
            LayerBodyV2::Group { layers } => {
                let group_alpha = scalar(&layer.opacity, time, s, 1.0).clamp(0.0, 1.0);
                for child in layers {
                    if let Some(mut paint) = paint_of(child, doc, pair, time, s) {
                        paint.alpha *= group_alpha;
                        result = over(&paint, result);
                    }
                }
            }
            _ => {
                if let Some(paint) = paint_of(layer, doc, pair, time, s) {
                    result = over(&paint, result);
                }
            }
        }
    }
    result
}

/// One role: what to call it, its colours, and the hue window it owes.
type Role<'a> = (
    &'static str,
    &'a ColorPair,
    Option<(HueWindow, &'static str)>,
);

/// Every role a skin has to read correctly in.
fn roles(doc: &SkinDocV2) -> Vec<Role<'_>> {
    vec![
        (
            "friendly[0]",
            &doc.palette.friendly[0],
            Some((FRIENDLY_HUES, "friendly")),
        ),
        (
            "friendly[1]",
            &doc.palette.friendly[1],
            Some((FRIENDLY_HUES, "friendly")),
        ),
        (
            "enemy[0]",
            &doc.palette.enemy[0],
            Some((ENEMY_HUES, "enemy")),
        ),
        (
            "enemy[1]",
            &doc.palette.enemy[1],
            Some((ENEMY_HUES, "enemy")),
        ),
        (
            "free_for_all[0]",
            &doc.palette.free_for_all[0],
            Some((FRIENDLY_HUES, "friendly")),
        ),
        (
            "free_for_all[1]",
            &doc.palette.free_for_all[1],
            Some((ENEMY_HUES, "enemy")),
        ),
        ("free_for_all[2]", &doc.palette.free_for_all[2], None),
        ("free_for_all[3]", &doc.palette.free_for_all[3], None),
    ]
}

/// Check what a document actually paints, at every step and in every role.
///
/// Returns the failures, most specific first. An empty result means the skin
/// is *readable*, which is a narrower claim than "good" — see the module docs.
pub fn check(doc: &SkinDocV2) -> Vec<SkinDocError> {
    let mut errors = Vec::new();
    let steps = ANIMATION_STEPS;

    // The shipped look's one recorded weak spot, carried across from v1 rather
    // than rediscovered: on the steel free-for-all slot the derived white label
    // reaches only ~3.5:1 where the head glow lightens the body beneath it.
    //
    // Worth stating plainly, because it is the strongest evidence this module
    // works: v1 found that analytically, from a closed structure it knew in
    // advance. This found it by compositing, knowing nothing in advance. Two
    // independent methods landing on the same slot for the same reason is what
    // a model earning its keep looks like.
    let label_checked = !crate::LIT_LABEL_EXEMPT.contains(&doc.id.as_str());

    for (role, pair, window) in roles(doc) {
        let Some(fill) = Rgb::parse(&pair.fill) else {
            continue;
        };
        let ink = match &doc.labels.ink {
            Some(ink) => Rgb::parse(ink),
            None => Some(derive_label_ink(fill)),
        };

        let mut worst_label: Option<(f64, usize, Rgb)> = None;
        let mut worst_core: Option<(f64, usize, Rgb)> = None;
        // Area-weighted only in the crude sense that every sample counts once;
        // the samples are evenly spread, which is what makes that fair.
        let mut side_samples: Vec<Rgb> = Vec::new();

        for step in 0..steps {
            let time = step as f64 / steps as f64;

            if let Some(ink) = ink {
                for s in LABEL_SAMPLES_S {
                    let under = sample(doc, pair, time, s);
                    let ratio = contrast_ratio(ink, under);
                    if worst_label.is_none_or(|(worst, _, _)| ratio < worst) {
                        worst_label = Some((ratio, step, under));
                    }
                }
            }

            if let Some(core) = Rgb::parse(&doc.head_core.color) {
                // Just outside the core, which is what it has to stand out
                // from — inside it, the core is the only thing painted.
                let around = sample(doc, pair, time, 0.6);
                let ratio = contrast_ratio(core, around);
                if worst_core.is_none_or(|(worst, _, _)| ratio < worst) {
                    worst_core = Some((ratio, step, around));
                }
            }

            for s in SIDE_SAMPLES_S {
                side_samples.push(sample(doc, pair, time, s));
            }
        }

        if let Some((ratio, step, under)) = worst_label
            && label_checked
            && ratio < MIN_LABEL_CONTRAST
        {
            errors.push(SkinDocError::new(
                format!("palette.{role}"),
                format!(
                    "the name and carried-food number only reach {ratio:.1}:1 \
                     where they sit, two cells behind the head. At step {step} \
                     of the cycle your layers composite to `{}` there, and \
                     {MIN_LABEL_CONTRAST}:1 is the floor. A layer painting over \
                     that stretch is the usual cause.",
                    under.to_hex()
                ),
            ));
        }

        if let Some((ratio, step, around)) = worst_core
            && ratio < MIN_HEAD_CORE_CONTRAST
        {
            errors.push(SkinDocError::new(
                "head_core.color",
                format!(
                    "the head core only reaches {ratio:.1}:1 against what your \
                     layers paint around it (`{}` at step {step}, role {role}). \
                     Players find the head by its core, so {MIN_HEAD_CORE_CONTRAST}:1 \
                     is the floor.",
                    around.to_hex()
                ),
            ));
        }

        // What the snake *reads* as, rather than what its fill is set to. This
        // is the check that survives an author covering a blue body in gold.
        if let Some((window, side)) = window
            && let Some(problem) = side_reading(&side_samples, window, side)
        {
            errors.push(SkinDocError::new(format!("palette.{role}"), problem));
        }
    }

    errors
}

/// Whether a set of composited samples still reads as its side.
///
/// Weighted by chroma: a near-grey sample carries no side information, and
/// letting it drag the average toward neutral would be how a skin talks its way
/// past this. A body that is *mostly* neutral passes, because a grey snake
/// misleads nobody; a body that is chromatic and on the wrong side does not.
fn side_reading(samples: &[Rgb], window: HueWindow, side: &str) -> Option<String> {
    let mut weight = 0.0;
    let mut x = 0.0;
    let mut y = 0.0;
    for sample in samples {
        let (hue, chroma) = sample.oklch_hue_chroma();
        if chroma <= NEUTRAL_CHROMA {
            continue;
        }
        let radians = hue.to_radians();
        x += chroma * radians.cos();
        y += chroma * radians.sin();
        weight += chroma;
    }
    if weight <= 0.0 {
        // Nothing chromatic was painted; a grey snake carries no side claim.
        return None;
    }

    let mean = y.atan2(x).to_degrees().rem_euclid(360.0);
    if window.contains(mean) {
        return None;
    }
    Some(format!(
        "painted, this snake reads at {mean:.0}deg, outside the {side} range \
         ({:.0}..{:.0}deg). Its fill is inside the range, so the layers over it \
         are what changed the reading — teams are told apart by colour, and a \
         {side} snake has to look like one.",
        window.from, window.to
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{
        ClipV2, CornerV2, LayerV2, PropExpr, RegionV2, SpanV2, TransformV2, upgrade, validate_v2,
    };

    fn classic_v2() -> SkinDocV2 {
        let v1: crate::SkinDoc =
            serde_json::from_str(include_str!("../skins/classic.skin.json")).expect("parses");
        upgrade(&v1)
    }

    /// The classic stack under a *new* skin's id, which is what an author who
    /// started from the Classic template actually has. It gets the strict
    /// rule; only the shipped document carries the exemption.
    fn authored() -> SkinDocV2 {
        let mut doc = classic_v2();
        doc.id = "authored@1".to_string();
        // ...and its one inherited weak spot repaired, so a test that adds a
        // layer is measuring that layer rather than the steel slot.
        doc.palette.free_for_all[2].fill = "#3d4752".to_string();
        doc
    }

    fn cover(name: &str, color: ColorRef, opacity: &str) -> LayerV2 {
        LayerV2 {
            name: name.to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr(opacity.to_string()),
            transform: TransformV2::default(),
            body: LayerBodyV2::Span {
                region: RegionV2::Body,
                clip: ClipV2::Silhouette,
                span: SpanV2::whole(),
                corner: CornerV2::Fan,
                source: SourceV2::Solid { color },
            },
        }
    }

    /// The shipped look has to pass its own sampler, or the sampler is wrong.
    #[test]
    fn the_converted_classic_document_reads_correctly() {
        let problems = check(&classic_v2());
        assert!(problems.is_empty(), "{problems:?}");
    }

    /// The sampler's strongest evidence, kept as a test rather than a claim.
    ///
    /// v1 recorded one weak spot in the shipped look — the steel free-for-all
    /// slot, where the derived white label sits on a body the head glow has
    /// lightened — and found it analytically, from a structure it knew in
    /// advance. Strip the exemption and this module finds the same slot for
    /// the same reason, knowing nothing in advance and only compositing.
    ///
    /// It also pins the exemption's scope: it covers the shipped document and
    /// nothing else, exactly as v1's does.
    #[test]
    fn the_recorded_weak_spot_is_rediscovered_by_compositing() {
        let mut borrowed = classic_v2();
        borrowed.id = "borrowed-classic@1".to_string();

        let problems = check(&borrowed);
        let error = problems
            .iter()
            .find(|error| error.problem.contains("carried-food number"))
            .expect("a new skin may not inherit the classic document's exemption");
        assert_eq!(
            error.field, "palette.free_for_all[2]",
            "the steel slot is the one v1 recorded: {error}"
        );
    }

    /// The check v1 could not do: a layer that covers the body changes what
    /// the label sits on, and the document's *fill* still looks fine.
    #[test]
    fn a_layer_that_washes_out_the_label_is_caught() {
        let mut doc = authored();
        doc.literals
            .insert("pale".to_string(), "#f2f4f6".to_string());
        doc.labels.ink = Some("#ffffff".to_string());
        doc.layers.push(cover(
            "Wash",
            ColorRef {
                target: ColorTarget::Literal {
                    literal: "pale".to_string(),
                },
                lighten: None,
            },
            "0.95",
        ));

        let problems = check(&doc);
        assert!(
            problems
                .iter()
                .any(|error| error.problem.contains("carried-food number")),
            "{problems:?}"
        );
    }

    /// The reason the side reading exists: every colour in the palette is
    /// inside its window, and the snake still reads as the wrong side.
    #[test]
    fn a_friendly_snake_painted_red_is_caught_even_with_a_legal_palette() {
        let mut doc = classic_v2();
        doc.literals
            .insert("danger".to_string(), "#e33d3d".to_string());
        doc.layers.push(cover(
            "Red coat",
            ColorRef {
                target: ColorTarget::Literal {
                    literal: "danger".to_string(),
                },
                lighten: None,
            },
            "0.9",
        ));

        let problems = check(&doc);
        assert!(
            problems
                .iter()
                .any(|error| error.problem.contains("reads at")),
            "a friendly snake covered in red must be refused: {problems:?}"
        );

        // The palette itself is untouched and still passes v1's rule, which is
        // exactly why the structural check could never have caught this.
        let mut structural = Vec::new();
        crate::validate_palette_hues(&doc.palette, &mut structural);
        assert!(structural.is_empty(), "{structural:?}");
    }

    /// A grey snake claims no side, so it is not judged on hue. Without this
    /// the check would refuse every monochrome skin.
    #[test]
    fn a_neutral_coat_carries_no_side_claim() {
        let mut doc = classic_v2();
        doc.literals
            .insert("ash".to_string(), "#8a8f95".to_string());
        doc.layers.push(cover(
            "Ash coat",
            ColorRef {
                target: ColorTarget::Literal {
                    literal: "ash".to_string(),
                },
                lighten: None,
            },
            "0.95",
        ));
        assert!(
            !check(&doc)
                .iter()
                .any(|error| error.problem.contains("reads at")),
            "a grey snake misleads nobody"
        );
    }

    /// A layer can be legal at rest and illegal halfway round the cycle, which
    /// is the whole reason every step is checked rather than sampled.
    #[test]
    fn a_failure_that_only_happens_mid_cycle_is_still_a_failure() {
        let mut doc = authored();
        doc.literals
            .insert("pale".to_string(), "#fbfcfd".to_string());
        doc.labels.ink = Some("#ffffff".to_string());
        // Zero at rest, near-total at a quarter turn.
        doc.layers.push(cover(
            "Pulse",
            ColorRef {
                target: ColorTarget::Literal {
                    literal: "pale".to_string(),
                },
                lighten: None,
            },
            "0.5 + 0.5 * sin(tau * time)",
        ));

        let problems = check(&doc);
        let error = problems
            .iter()
            .find(|error| error.problem.contains("carried-food number"))
            .expect("a mid-cycle wash must be caught");
        assert!(error.problem.contains("step"), "{error}");
    }

    /// The sampler is part of validation, not a separate opt-in gate — a skin
    /// that cannot be read must not be savable.
    #[test]
    fn validation_runs_the_sampler() {
        let mut doc = authored();
        doc.literals
            .insert("pale".to_string(), "#f2f4f6".to_string());
        doc.labels.ink = Some("#ffffff".to_string());
        doc.layers.push(cover(
            "Wash",
            ColorRef {
                target: ColorTarget::Literal {
                    literal: "pale".to_string(),
                },
                lighten: None,
            },
            "0.95",
        ));

        let errors = validate_v2(&doc).expect_err("an unreadable skin must not validate");
        assert!(
            errors
                .iter()
                .any(|error| error.problem.contains("carried-food number")),
            "{errors:?}"
        );
    }
}
